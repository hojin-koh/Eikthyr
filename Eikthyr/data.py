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
from .config import metadir

import os
from http.client import HTTPConnection
from urllib.parse import quote_plus

class LocalOverwriteFileSystem(LocalFileSystem):
    def rename_dont_move(self, path, dest):
        self.move(path, dest, raise_if_exists=False)

class Target(lg.LocalTarget):
    fs = LocalOverwriteFileSystem()

    def __init__(self, task, path):
        super().__init__(path)
        self.task = task
        self.metapath = Path(metadir) / "{}.json".format(self.path)

        self.objMeta = None

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
        with self.open('w') as fpw:
            yield fpw
        self.writeMeta()

    def writeMeta(self):
        self.metapath.parent.mkdir(parents=True, exist_ok=True)
        objMeta = {'data': {}, 'gen': {
            'task': self.task.getSignature(),
            'code': self.task.getCodeHash(),
            'src': [],
            }}
        objMeta['gen']['src'] = self.task.getSrcHash()

        if self.task.checkOutputHash:
            pathThis = Path(self.path)
            if pathThis.is_dir():
                h = md5()
                for f in sorted((p for p in pathThis.glob('**/*') if p.is_file())):
                    h.update(f.read_bytes())
                objMeta['gen']['out'] = h.hexdigest()
            else:
                objMeta['gen']['out'] = md5(pathThis.read_bytes(), usedforsecurity=False).hexdigest()
        else:
            objMeta['gen']['out'] = '0'

        with self.metapath.open('w') as fpwMeta:
            json.dump(objMeta, fpwMeta, indent=2, sort_keys=True)
        self.writeCache(objMeta)

    def writeCache(self, objMeta=None):
        if objMeta == None:
            with self.metapath.open() as fpMeta:
                objMeta = json.load(fpMeta)

        if 'EIKTHYR_CACHE_IP' in os.environ:
            try:
                data = bytes(json.dumps(objMeta), 'utf-8')
                h = HTTPConnection(os.environ['EIKTHYR_CACHE_IP'], int(os.environ['EIKTHYR_CACHE_PORT']))
                h.request('PUT', '/{}'.format(quote_plus(repr(self))), body=data, headers={'Content-Length': len(data)})
                h.getresponse()
            finally:
                h.close()
        else:
            self.objMeta = objMeta
        return objMeta

    def getMeta(self):
        if 'EIKTHYR_CACHE_IP' in os.environ:
            try:
                h = HTTPConnection(os.environ['EIKTHYR_CACHE_IP'], int(os.environ['EIKTHYR_CACHE_PORT']))
                h.request('GET', '/{}'.format(quote_plus(repr(self))))
                r = h.getresponse()
                if r.status == 200:
                    return json.loads(r.read(int(r.headers['Content-Length'])))
                else:
                    return self.writeCache()
            finally:
                h.close()
        elif self.objMeta != None:
            return self.objMeta
        return self.writeCache()

    def isOutdated(self):
        if not Path(self.path).exists():
            return True
        if not self.metapath.exists():
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
