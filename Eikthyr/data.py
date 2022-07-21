# -*- coding: utf-8 -*-
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


import json
from contextlib import contextmanager
from hashlib import md5
from pathlib import Path
from shutil import rmtree

import luigi as lg
from luigi.local_target import LocalFileSystem

from . import cache

class ConfigData(lg.Config):
    pathMeta = lg.Parameter('.meta')

class LocalOverwriteFileSystem(LocalFileSystem):
    def rename_dont_move(self, path, dest):
        pathDest = path(dest)
        if pathDest.is_dir():
            rmtree(dest)
            self.move(path, dest)
        else:
            pathDest.unlink(missing_ok=True)
            self.move(path, dest, raise_if_exists=False)

class Target(lg.LocalTarget):
    fs = LocalOverwriteFileSystem()

    def __init__(self, task, path):
        super().__init__(str(path))
        self.task = task
        pathForMeta = Path(self.path)
        if pathForMeta.is_absolute():
            if pathForMeta.is_relative_to(Path.cwd()):
                pathForMeta = pathForMeta.relative_to(Path.cwd())
            else:
                pathForMeta = pathForMeta.relative_to(pathForMeta.root)
        self.pathMeta = str(Path(ConfigData().pathMeta).resolve() / "{}.json".format(pathForMeta))
        self.pathRel = str(pathForMeta)

        self._objMeta = None

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self.path)

    @contextmanager
    def pathWrite(self):
        self.makedirs()
        with self.temporary_path() as f:
            yield f
        self.writeMeta()

    @contextmanager
    def fpWrite(self):
        with self.open('wb') as fpw:
            yield fpw
        self.writeMeta()

    def writeMeta(self):
        Path(self.pathMeta).parent.mkdir(parents=True, exist_ok=True)
        objMeta = {'data': {}, 'gen': {
            'task': self.task.getSignature(),
            'code': self.task.getCodeHash(),
            'src': [],
            }}
        objMeta['gen']['src'] = self.task.getSrcHash()

        if self.task.checkOutputHash:
            pathThis = Path(self.path)
            if pathThis.is_dir():
                h = md5(usedforsecurity=False)
                for f in sorted((p for p in pathThis.glob('**/*') if p.is_file())):
                    h.update(f.read_bytes())
                objMeta['gen']['out'] = h.hexdigest()
            else:
                objMeta['gen']['out'] = md5(pathThis.read_bytes(), usedforsecurity=False).hexdigest()
        else:
            objMeta['gen']['out'] = '0'

        with Path(self.pathMeta).open('w') as fpwMeta:
            json.dump(objMeta, fpwMeta, indent=2, sort_keys=True)
        self.writeCache(objMeta)

    def writeCache(self, objMeta=None):
        if objMeta == None:
            with Path(self.pathMeta).open() as fpMeta:
                objMeta = json.load(fpMeta)

        if cache.isAvailable():
            cache.putObj(self, objMeta)
        else:
            self._objMeta = objMeta
        return objMeta

    def getMeta(self):
        if cache.isAvailable():
            rslt = cache.getObj(self)
            if rslt != None:
                return rslt
        elif self._objMeta != None:
            return self._objMeta
        return self.writeCache()

    def isOutdated(self):
        if not Path(self.path).exists():
            return True
        if not Path(self.pathMeta).exists():
            return True
        if 'gen' not in self.getMeta():
            return True
        objGen = self.getMeta()['gen']

        # Check hashes of this task
        if 'task' not in objGen or self.task.getSignature() != objGen['task']:
            return True
        if 'code' not in objGen or self.task.getCodeHash() != objGen['code']:
            return True

        # Check hashes of dependencies
        if self.task.getSrcHash() != objGen['src']:
            return True

        return False
