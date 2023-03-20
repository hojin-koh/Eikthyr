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
import json
from contextlib import contextmanager
from hashlib import md5
from pathlib import Path
from shutil import rmtree

import luigi as lg
from luigi.local_target import LocalFileSystem

class LocalOverwriteFileSystem(LocalFileSystem):
    def rename_dont_move(self, path, dest):
        pathDest = Path(dest)
        if pathDest.is_dir():
            rmtree(dest)
            self.move(path, dest)
        else:
            pathDest.unlink(missing_ok=True)
            self.move(path, dest, raise_if_exists=False)

class Target(lg.LocalTarget):
    fs = LocalOverwriteFileSystem()

    def __init__(self, path, **kwargs):
        super().__init__(str(path), **kwargs)

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self.path)

    @contextmanager
    def pathWrite(self):
        self.makedirs()
        with self.temporary_path() as f:
            yield f

    @contextmanager
    def fpWrite(self):
        with self.open('w') as fpw:
            yield fpw

class BinaryTarget(Target):
    def __init__(self, path, **kwargs):
        super().__init__(str(path), format=lg.format.Nop, **kwargs)
