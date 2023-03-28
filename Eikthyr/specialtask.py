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

from .target import Target
from .param import PathParameter
from .task import BaseTask, Task

# Wrapper for an input file
class InputTask(BaseTask):
    src = PathParameter()

    def requires(self):
        return []

    def output(self):
        return Target(self, self.src)

    def run(self):
        if not Path(self.src).exists():
            raise OSError(1, "Input file not found", self.src)
