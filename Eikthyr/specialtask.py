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
from copy import copy

from .task import Task
from .data import Target
from .param import TargetParameter
from .config import stampdir

from logzero import setup_logger
logger = setup_logger('Eikthyr')

# Wrapper for an input file
class InputTask(Task):
    src = lg.Parameter()

    def generates(self):
        return Target(self, self.src)

    def run(self):
        if not Path(self.src).exists():
            raise OSError(1, "Input file not found", self.src)
        self.output().writeMeta()

# Wrapper for a single target
class TargetTask(lg.Task):
    src = TargetParameter()

    def requires(self):
        return self.src.task

    def output(self):
        return self.src

    def run(self):
        pass

    def complete(self):
        return self.src.task.complete()

# Stamp target: if code don't change, no need to re-run
class StampTask(Task):

    def generates(self):
        # Let's turn outself into a filename
        return Target(self, Path(stampdir) / "".join(c for c in repr(self) if c.isalnum()))

    def getCode(self):
        return self.__class__.task

    # Outputs in this task don't care about the upstream sources
    def getSrcHash(self):
        return copy([])

    # Actual thing defined here
    def task(self):
        pass

    def run(self):
        self.cacheComplete = None # invalidate the cache
        if self.complete():
            logger.debug("<NOP> Stamp unchanged.")
            return
        self.task()
        with self.output().fpWrite() as fpw:
            pass
