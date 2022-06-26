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
import time
from plumbum import FG

from luigi.task import flatten
from .data import Target

from logzero import setup_logger
logger = setup_logger('Eikthyr')

class Task(lg.Task):
    checkInputHash = True
    checkOutputHash = True
    checkCodeHash = True
    checkSignature = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if hasattr(self, 'generates'):
            self.objOutput = self.generates()
        else:
            self.objOutput = None
        self.cacheComplete = None
        self.hashSrc = None

    def output(self):
        return self.objOutput;

    # Expected to get a plumbum object
    def ex(self, chain):
        logger.info("EX: {}".format(chain))
        chain & FG

    def getCode(self):
        return self.__class__.run

    def getCodeHash(self):
        return md5(pickle.dumps(getsource(self.getCode()), protocol=4), usedforsecurity=False).hexdigest()

    def getSrcHash(self):
        return self.hashSrc

    def complete(self):
        if self.cacheComplete is not None:
            return self.cacheComplete

        # Check whether the dependencies are fine
        for tgt in flatten(self.input()):
            if not isinstance(tgt, Target): continue
            if not tgt.task.complete():
                return False

        # Reaching here, all inputs are completed
        if self.hashSrc == None:
            self.hashSrc = []
            for tgt in flatten(self.input()):
                if not isinstance(tgt, Target): continue
                self.hashSrc.append(tgt.getMeta()['gen']['out'])
            self.hashSrc.sort()

        outputs = flatten(self.output())
        if len(outputs) == 0:
            self.cacheComplete = False
            return False
        for t in outputs:
            if isinstance(t, Target):
                if t.isOutdated():
                    self.cacheComplete = False
                    return False
            else:
                if not t.exists():
                    self.cacheComplete = False
                    return False
        self.cacheComplete = True
        return True

@Task.event_handler(lg.Event.START)
def hook_start(self):
    if not hasattr(self, 'timeStart'):
        logger.debug("Start {}".format(self))
        self.timeStart = time.time()

@Task.event_handler(lg.Event.PROCESSING_TIME)
def hook_end(self, t):
    self.cacheComplete = None # invalidate the cache
    logger.info("End {} in {:.3f}s\n".format(self, time.time() - self.timeStart))
