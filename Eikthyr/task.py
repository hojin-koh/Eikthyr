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

import time
import pickle
from hashlib import md5
from inspect import getsource
from pathlib import Path

import luigi as lg
from luigi.task import flatten
from colorama import Fore, Style

from . import cache
from .cmd import MixinCmdUtilities
from .data import Target
from .logging import logger

class ConfigEnviron(lg.Config):
    environ = lg.DictParameter({}, significant=False, positional=False)

class Task(lg.Task, MixinCmdUtilities):
    checkInputHash = True
    checkOutputHash = True
    checkCodeHash = True
    checkSignature = True

    ReRunForMeta = False

    environ = lg.DictParameter(ConfigEnviron().environ, significant=False, positional=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if hasattr(self, 'generates'):
            self.objOutput = self.generates()
        else:
            self.objOutput = None
        self._cacheComplete = None
        self._hashSrc = None

    def output(self):
        return self.objOutput;

    def getSignature(self):
        if self.checkSignature:
            return repr(self)
        else:
            return "{}()".format(self.__class__.__name__)

    def getCode(self):
        return self.__class__.task

    def run(self):
        if not self.ReRunForMeta:
            outputToCheck = list(tgt for tgt in flatten(self.output()) if isinstance(tgt, Target))
            if all(Path(tgt.path).exists() for tgt in outputToCheck):
                for tgt in outputToCheck:
                    if not Path(tgt.pathMeta).exists():
                        logger.debug("Metadata for {} regenerated".format(tgt.path))
                        tgt.writeMeta()
                self.invalidateCache()
                if self.complete():
                    return True

        if not hasattr(self, 'timeStart'):
            logger.debug("{}{}Start {}{}".format(Fore.CYAN, Style.BRIGHT, self, Style.RESET_ALL))
            self.timeStart = time.time()
        with self.env(**self.environ):
            rtn = self.task()
        self.invalidateCache()
        logger.info("End {} in {:.3f}s\n".format(self, time.time() - self.timeStart))
        return rtn

    def task(self):
        pass

    def getCodeHash(self):
        if self.checkCodeHash:
            return md5(pickle.dumps(getsource(self.getCode()), protocol=4), usedforsecurity=False).hexdigest()
        else:
            return '0'

    def getSrcHash(self):
        if self._hashSrc == None:
            self._hashSrc = []
            if self.checkInputHash:
                for tgt in flatten(self.input()):
                    if not isinstance(tgt, Target): continue
                    self._hashSrc.append(tgt.getMeta()['gen']['out'])
                self._hashSrc.sort()
        return self._hashSrc

    def invalidateCache(self):
        if cache.isAvailable():
            rslt = cache.deleteObj(self)
        else:
            self._cacheComplete = None

    def writeCache(self, rslt):
        if cache.isAvailable():
            cache.putObj(self, rslt)
        else:
            self._cacheComplete = rslt
        return rslt

    def complete(self):
        if cache.isAvailable():
            rslt = cache.getObj(self)
            if rslt != None:
                return rslt
        elif self._cacheComplete is not None:
            return self._cacheComplete

        # Check whether the dependencies are fine
        for tgt in flatten(self.input()):
            if not isinstance(tgt, Target): continue
            if not tgt.task.complete():
                return self.writeCache(False)

        outputs = flatten(self.output())
        if len(outputs) == 0:
            return self.writeCache(False)
        for t in outputs:
            if isinstance(t, Target):
                if t.isOutdated():
                    return self.writeCache(False)
            else:
                if not t.exists():
                    return self.writeCache(False)
        return self.writeCache(True)

class STask(Task):
    ReRunForMeta = True
