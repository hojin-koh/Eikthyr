# -*- coding: utf-8 -*-
# Copyright 2021-2023, Hojin Koh
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
from inspect import isgenerator
from pathlib import Path

import luigi as lg
from luigi.task import flatten
from colorama import Fore, Style
from plumbum import FG

from .cmd import withEnv
from .target import Target, BinaryTarget
from .logging import logger
from .param import TaskParameter, TaskListParameter

def getAllInputTargets(aTask):
    if len(aTask) == 0: return set()
    setRslt = set()
    for t in aTask:
        setRslt |= set(flatten(t.input()))
        setRslt |= getAllInputTargets(t._requires())
    return setRslt

class BaseTask(lg.Task):
    prev = TaskListParameter((), significant=False, positional=False)
    environ = lg.DictParameter({}, significant=False, positional=False)
    logger = logger

    #def __init__(self, *args, **kwargs):
    #    super().__init__(*args, **kwargs)
    #    self.objOutput = None

    # Mostly copied from the original luigi
    def __repr__(self):
        params = self.get_params()
        param_values = self.get_param_values(params, [], self.param_kwargs)

        # Build up task id
        repr_parts = []
        param_objs = dict(params)
        for param_name, param_value in param_values:
            if param_objs[param_name].significant:
                # If there is serializeShort we added, use it
                if hasattr(param_objs[param_name], 'serializeShort'):
                    thisRepr = param_objs[param_name].serializeShort(param_value)
                else:
                    thisRepr = param_objs[param_name].serialize(param_value)
                repr_parts.append('{}={}'.format(param_name, thisRepr))

        return '{}({})'.format(self.get_task_family(), ', '.join(repr_parts))

    def _requires(self):
        return flatten(self.requires()) + list(self.prev)

    # The default input if there's a parameter called "src"
    def requires(self):
        if hasattr(self, 'src'):
            return self.src
        else:
            return []

    # The default output if there's a parameter called "out" or "outbin"
    def output(self):
        if hasattr(self, 'out'):
            return Target(self.out)
        elif hasattr(self, 'outbin'):
            return BinaryTarget(self.outbin)
        else:
            return []

    #def run(self):
    #    if not hasattr(self, 'timeStart'):
    #        logger.debug("{}{}Start {}{}".format(Fore.CYAN, Style.BRIGHT, self, Style.RESET_ALL))
    #        self.timeStart = time.time()
    #    with withEnv(**self.environ):
    #        rtn = self.task()
    #        if isgenerator(rtn):
    #            yield from rtn

    #    # After running, generate the remaining missing metadata
    #    for tgt in outputToCheck:
    #        if Path(tgt.path).exists():
    #            if not Path(tgt.pathMeta).exists():
    #                tgt.writeMeta()
    #            elif tgt.isOutdated():
    #                tgt.writeMeta()

    #    self.invalidateCache()
    #    logger.info("End {} in {:.3f}s\n".format(self, time.time() - self.timeStart))
    #    return rtn

    def complete(self):
        """
        Return `True` if all output files exist and their modification time is newer than
        the modification time of any input file. Otherwise, return `False`.
        """
        aInputs = list(getAllInputTargets([self]))
        aOutputs = flatten(self.output())

        # First, still check the output exists, as in luigi
        if not all((output.exists() for output in aOutputs)):
            return False

        # Collect the list of mtime from outputs. If none can be checked, then just assume they're okay
        aMtimesOutput = [output.mtime() for output in aOutputs if hasattr(output, 'mtime')]
        if len(aMtimesOutput) == 0:
            return True

        # Collect the list of mtime from inputs. If none can be checked, then just assume they're okay
        aMtimesInput = [obj.mtime() for obj in aInputs if hasattr(obj, 'mtime')]
        if len(aMtimesInput) == 0:
            return True

        # Reaching here, all outputs exist, and both input side and output side has some mtime for comparison
        if max(aMtimesInput) > min(aMtimesOutput):
            return False

        return True

    # Expected to get a plumbum object
    def ex(self, chain):
        self.logger.info("RUN: {}".format(chain))
        chain & FG

class Task(BaseTask):
    pass

@Task.event_handler(lg.Event.START)
def logTaskStart(task):
    logger.debug("{}{}Start {}{}".format(Fore.CYAN, Style.BRIGHT, task, Style.RESET_ALL))

@Task.event_handler(lg.Event.PROCESSING_TIME)
def logTaskStart(task, t):
    logger.info("{}{}Done {} in {:.1f}s{}\n".format(Fore.GREEN, Style.BRIGHT, task, t, Style.RESET_ALL))
