import luigi as lg
from pathlib import Path
from contextlib import contextmanager
from hashlib import md5
import pickle
import json

metadir = "meta"

class MetaTarget(lg.LocalTarget):

    def __init__(self, task, path):
        super().__init__(path)
        self.task = task
        self.metapath = Path(metadir) / "{}.json".format(self.path)

    def makedirs(self):
        super().makedirs()
        self.metapath.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def pathWrite(self):
        self.makedirs()
        with self.temporary_path() as f:
            yield f
            objMeta = {"data": {}, "gen": {
                "task": repr(self.task),
                "code": md5(pickle.dumps(self.task.__class__.run, protocol=4), usedforsecurity=False).hexdigest(),
                "out": md5(Path(f).read_bytes(), usedforsecurity=False).hexdigest(),
                }}
            with self.metapath.open('w') as fpwMeta:
                json.dump(objMeta, fpwMeta, indent=2, sort_keys=True)
