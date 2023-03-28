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


import os
import re
from pathlib import Path

import luigi as lg
from luigi.task import flatten

from .target import Target
from .logging import logger
from .param import PathParameter, TargetParameter
from .task import Task

# Wrapper for an input file
class InputTask(Task):
    src = PathParameter()

    def requires(self):
        return []

    def generates(self):
        return Target(self, self.src)

    def task(self):
        if not Path(self.src).exists():
            raise OSError(1, "Input file not found", self.src)
        self.output().writeMeta()

# Wrapper for a single target
class TargetWrapperTask(Task):
    src = TargetParameter()

    def requires(self):
        return self.src.task

    def output(self):
        return self.src

    def run(self):
        pass

    def complete(self):
        return self.src.task.complete()

# Stamp: if code don't change, no need to re-run
class StampTask(Task):
    pathStamp = PathParameter(os.getenv('EIKTHYR_DIR_STAMP', '.stamp'), positional=False)

    # This task doesn't care about the whether the upstream sources changed
    checkInputHash = False

    def getStampFileName(self):
        return re.sub('pathStamp=[^ ]+, ', ' ', repr(self))

    def generates(self):
        # Let's turn ourself into a filename
        return Target(self, Path(self.pathStamp).resolve() / self.getStamp())

    def run(self):
        rtn = yield from super().run()
        with self.output().fpWrite() as fpw:
            if self.checkInputHash:
                fpw.write(''.join(self.getSrcHash()))
            fpw.write(self.getCodeHash())
            fpw.write(self.getSignature())
        return rtn
