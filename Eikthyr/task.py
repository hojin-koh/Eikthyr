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
import pickle
from hashlib import md5
from inspect import getsource

import logging
from plumbum import FG

from luigi.task import flatten
from .data import MetaTarget
from logzero import setup_logger

logger = setup_logger('Eikthyr')

class Task(lg.Task):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.objOutput = self.generates()
        self.cacheComplete = None

    def output(self):
        return self.objOutput;

    @lg.Task.event_handler(lg.Event.START)
    def hook_start(self):
        logger.debug("Start {}".format(self))

    @lg.Task.event_handler(lg.Event.PROCESSING_TIME)
    def hook_end(self, t):
        self.cacheComplete = None # invalidate the cache
        logger.info("End {} in {:.3f}s".format(self, t))

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

