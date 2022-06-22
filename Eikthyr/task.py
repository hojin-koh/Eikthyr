import luigi as lg
import pickle
from hashlib import md5
from inspect import getsource

import logging
from plumbum import FG

from luigi.task import flatten
from .data import MetaTarget

logger = logging.getLogger('Eikthyr')

class Task(lg.Task):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.objOutput = self.generates()
        self.cacheComplete = None

    def output(self):
        return self.objOutput;

    @lg.Task.event_handler(lg.Event.START)
    def hook_start(self):
        print("START! {}".format(self))

    @lg.Task.event_handler(lg.Event.PROCESSING_TIME)
    def hook_end(self, t):
        self.cacheComplete = None # invalidate the cache
        print("END! {}".format(t))

    # Expected to get a plumbum object
    def ex(self, chain):
        logger.info("EX: {}".format(chain))
        chain & FG

    def getCodeHash(self):
        return md5(pickle.dumps(getsource(self.__class__.run), protocol=4), usedforsecurity=False).hexdigest()

    def complete(self):
        if self.cacheComplete is not None:
            return self.cacheComplete
        outputs = flatten(self.output())
        if len(outputs) == 0:
            self.cacheComplete = False
            return False
        for t in outputs:
            if isinstance(t, MetaTarget):
                if t.isOutdated():
                    self.cacheComplete = False
                    return False
            else:
                if not t.exists():
                    self.cacheComplete = False
                    return False
        self.cacheComplete = True
        return True

