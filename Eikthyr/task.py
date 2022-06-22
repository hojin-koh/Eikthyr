import luigi as lg
import pickle
from hashlib import md5
from inspect import getsource

import logging
from plumbum import FG

from luigi.task import flatten
from .data import MetaTarget

logger = logging.getLogger('luigi-interface')

class Task(lg.Task):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.objOutput = self.generates()

    def output(self):
        return self.objOutput;

    # Expected to get a plumbem object
    def ex(self, chain):
        logger.info("EX: {}".format(chain))
        chain & FG

    def getCodeHash(self):
        return md5(pickle.dumps(getsource(self.__class__.run), protocol=4), usedforsecurity=False).hexdigest()

    def complete(self):
        outputs = flatten(self.output())
        if len(outputs) == 0:
            return False
        for t in outputs:
            if isinstance(t, MetaTarget):
                if t.isOutdated():
                    return False
            else:
                if not t.exists():
                    return False
        return True

