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


    def makedirs(self):
        super().makedirs()
        self.metapath.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def pathWrite(self):
        self.makedirs()
        with self.temporary_path() as f:
            yield f
            objMeta = {'data': {}, 'gen': {
                'task': repr(self.task),
                'code': self.task.getCodeHash(),
                'out': md5(Path(f).read_bytes(), usedforsecurity=False).hexdigest(),
                'src': [],
                }}
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
