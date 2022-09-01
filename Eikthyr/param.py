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

import pickle
from base64 import b85encode, b85decode
from pathlib import Path

import luigi as lg
from luigi.task import flatten

from .data import Target

class PathParameter(lg.Parameter):
    def _warn_on_wrong_param_type(self, param_name, param_value):
        Path(param_value)

    def serializeShort(self, x):
        pathForShow = Path(x)
        if pathForShow.is_absolute():
            if pathForShow.is_relative_to(Path.cwd()):
                pathForShow = pathForShow.relative_to(Path.cwd())
        return str(pathForShow)

class WhateverParameter(lg.Parameter):
    def _warn_on_wrong_param_type(self, param_name, param_value):
        return

    def serialize(self, x):
        return str(b85encode(pickle.dumps(x)), encoding='ASCII')

    def parse(self, x):
        return pickle.loads(b85decode(bytes(x, encoding='ASCII')))

class TaskParameter(WhateverParameter):
    def serializeShort(self, x):
        try:
            return x.output().pathRel
        except:
            try:
                return ';'.join(sorted([y.pathRel for y in flatten(x.output())]))
            except:
                return super().serialize(x)

class TaskListParameter(WhateverParameter):
    def serializeShort(self, xs):
        try:
            return ';'.join(sorted([x.output().pathRel for x in xs]))
        except:
            try:
                return ';'.join(sorted([y.pathRel for x in xs for y in flatten(x.output())]))
            except:
                return super().serialize(xs)

    def serialize(self, x):
        return str(b85encode(pickle.dumps(x)), encoding='ASCII')

    def parse(self, x):
        return pickle.loads(b85decode(bytes(x, encoding='ASCII')))

class TargetParameter(WhateverParameter):
    def _warn_on_wrong_param_type(self, param_name, param_value):
        if self.__class__ != TargetParameter:
            return
        if not isinstance(param_value, Target):
            raise ValueError("parameter {} must be a Eikthyr target, got {} instead".format(param_name, param_value))

    def serializeShort(self, x):
        try:
            return x.pathRel
        except:
            return super().serialize(x)

    def serialize(self, x):
        return str(b85encode(pickle.dumps(x)), encoding='ASCII')

    def parse(self, x):
        return pickle.loads(b85decode(bytes(x, encoding='ASCII')))
