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
from inspect import getsource, isgenerator
from pathlib import Path

import luigi as lg
from luigi.task import flatten
from colorama import Fore, Style
from plumbum import FG

from . import cache
from .cmd import withEnv
from .data import Target
from .logging import logger
from .param import TaskParameter, TaskListParameter

class ConfigEnviron(lg.Config):
    environ = lg.DictParameter({}, significant=False, positional=False)

class Task(lg.Task):
    checkInputHash = True
    checkOutputHash = True
    simplifiedOutputHash = False # Compute only filenames for a directory
    checkCodeHash = True
    checkSignature = True

    ReRunForMeta = False
    ReRunAfterDeps = False

    prev = TaskListParameter((), significant=False, positional=False)
    environ = lg.DictParameter(ConfigEnviron().environ, significant=False, positional=False)
    canChange = lg.BoolParameter(False, significant=False, positional=False)
    logger = logger

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.objOutput = None
        self._cacheComplete = None
        self._hashSrc = None

    # Mostly copied from the original luigi
    def __repr__(self):
        params = self.get_params()
        param_values = self.get_param_values(params, [], self.param_kwargs)

        # Build up task id
        repr_parts = []
        param_objs = dict(params)
        for param_name, param_value in param_values:
            if param_objs[param_name].significant:
                if hasattr(param_objs[param_name], 'serializeShort'):
                    thisRepr = param_objs[param_name].serializeShort(param_value)
                else:
                    thisRepr = param_objs[param_name].serialize(param_value)
                repr_parts.append('{}={}'.format(param_name, thisRepr))

        return '{}({})'.format(self.get_task_family(), ', '.join(repr_parts))

    def _requires(self):
        return flatten(self.requires()) + list(self.prev)

    def requires(self):
        if hasattr(self, 'src'):
            return self.src
        else:
            return []

    def generates(self):
        if hasattr(self, 'out'):
            return Target(self, self.out)
        elif hasattr(self, 'outPath'):
            return Target(self, self.outPath)
        else:
            return []

    def output(self):
        if self.objOutput == None:
            self.objOutput = self.generates()
        return self.objOutput;

    def run(self):
        if not self.ReRunForMeta:
            outputToCheck = list(tgt for tgt in flatten(self.output()) if isinstance(tgt, Target))
            if all(Path(tgt.path).exists() for tgt in outputToCheck):
                for tgt in outputToCheck:
                    if not Path(tgt.pathMeta).exists():
                        logger.debug("Metadata for {} regenerated".format(tgt.path))
                        tgt.writeMeta()
                self.invalidateCache()
                if not self.ReRunAfterDeps:
                    if self.complete():
                        return True

        if not hasattr(self, 'timeStart'):
            logger.debug("{}{}Start {}{}".format(Fore.CYAN, Style.BRIGHT, self, Style.RESET_ALL))
            self.timeStart = time.time()
        with withEnv(**self.environ):
            rtn = self.task()
            if isgenerator(rtn):
                yield from rtn

        # After running, generate the remaining missing metadata
        for tgt in outputToCheck:
            if Path(tgt.path).exists():
                if not Path(tgt.pathMeta).exists():
                    tgt.writeMeta()
                elif tgt.isOutdated():
                    tgt.writeMeta()

        self.invalidateCache()
        logger.info("End {} in {:.3f}s\n".format(self, time.time() - self.timeStart))
        return rtn

    def task(self):
        pass

    def getSignature(self):
        if self.checkSignature:
            return repr(self)
        else:
            return "{}()".format(self.__class__.__name__)

    def getShortStamp(self):
        return self.__class__.__name__

    def getStamp(self):
        return "".join(c for c in self.getStampFileName() if c.isalnum())

    def getCode(self):
        return self.__class__.task

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
                if self.canChange:
                    if t.isOutdated(reCompute=True):
                        return self.writeCache(False)
                else:
                    if t.isOutdated():
                        return self.writeCache(False)
            else:
                if not t.exists():
                    return self.writeCache(False)
        return self.writeCache(True)

    # Expected to get a plumbum object
    def ex(self, chain):
        self.logger.info("RUN: {}".format(chain))
        chain & FG

class STask(Task):
    ReRunForMeta = True

class NITask(Task):
    ReRunAfterDeps = True
