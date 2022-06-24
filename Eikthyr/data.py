# Copyright 2021-2022, Hojin Koh
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import luigi as lg

from pathlib import Path
from contextlib import contextmanager
from hashlib import md5
import pickle
import json

from luigi.local_target import LocalFileSystem
from luigi.task import flatten

metadir = "meta"

class LocalOverwriteFileSystem(LocalFileSystem):
    def rename_dont_move(self, path, dest):
        self.move(path, dest, raise_if_exists=False)

class MetaTarget(lg.LocalTarget):
    fs = LocalOverwriteFileSystem()

    def __init__(self, task, path):
        super().__init__(path)
        self.task = task
        self.metapath = Path(metadir) / "{}.json".format(self.path)

        self.objMeta = None
        self.cacheOutdated = None

    @contextmanager
    def pathWrite(self):
        self.makedirs()
        with self.temporary_path() as f:
            yield f
        self.writeMeta()

    def writeMeta(self):
        self.metapath.parent.mkdir(parents=True, exist_ok=True)
        objMeta = {'data': {}, 'gen': {
            'task': repr(self.task),
            'code': self.task.getCodeHash(),
            'src': [],
            }}

        pathThis = Path(self.path)
        if pathThis.is_dir():
            h = md5()
            for f in sorted((p for p in pathThis.glob('**/*') if p.is_file())):
                h.update(f.read_bytes())
            objMeta['gen']['out'] = h.hexdigest()
        else:
            objMeta['gen']['out'] = md5(pathThis.read_bytes(), usedforsecurity=False).hexdigest()

        for tgt in flatten(self.task.input()):
            if not isinstance(tgt, MetaTarget): continue
            objMeta['gen']['src'].append(tgt.getMeta()['gen']['out'])
        objMeta['gen']['src'].sort()
        with self.metapath.open('w') as fpwMeta:
            json.dump(objMeta, fpwMeta, indent=2, sort_keys=True)
        self.objMeta = objMeta

    def getMeta(self):
        if self.objMeta == None:
            with self.metapath.open() as fpMeta:
                self.objMeta = json.load(fpMeta)
        return self.objMeta

    def isOutdated(self):
        if not Path(self.path).exists():
            return True
        if not self.metapath.exists():
            return True
        if 'gen' not in self.getMeta():
            return True
        objGen = self.getMeta()['gen']

        # Check hashes of this task
        if 'task' not in objGen or repr(self.task) != objGen['task']:
            return True
        if 'code' not in objGen or self.task.getCodeHash() != objGen['code']:
            return True

        # Check hashes of dependencies
        aHashSrcCalculated = []
        for tgt in flatten(self.task.input()):
            if not isinstance(tgt, MetaTarget): continue
            if not tgt.task.complete():
                return True
            aHashSrcCalculated.append(tgt.getMeta()['gen']['out'])
        if sorted(aHashSrcCalculated) != objGen['src']:
            return True
                
        return False
