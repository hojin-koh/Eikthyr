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

import sys

import luigi as lg
from plumbum import local

from .logging import logger

class EnvCheck(object):
    _instance = None
    cmd = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
        return cls._instance

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.cmd != None:
            if isinstance(self.cmd, tuple) or isinstance(self.cmd, list) or isinstance(self.cmd, lg.ListParameter):
                if len(self.cmd)>0 and not self.cmd[0] in local:
                    self.failCmd(self.cmd[0])
            else:
                if not self.cmd in local:
                    self.failCmd(self.cmd)
            self.cmd = None # So that the check only runs once

    def failCmd(self, cmd):
        self.fail("Command '{}' not found".format(cmd))

    def fail(self, msg):
        logger.error("Environment check {} failed: {}".format(self.__class__.__name__, msg))
        sys.exit(1)
