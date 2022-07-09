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

from pathlib import Path

import luigi as lg

from .data import Target
from .task import Task

class WhateverParameter(lg.Parameter):
    def _warn_on_wrong_param_type(self, param_name, param_value):
        return

class PathParameter(lg.PathParameter):
    def _warn_on_wrong_param_type(self, param_name, param_value):
        Path(param_value)

    def serialize(self, x):
        pathForShow = Path(x)
        if pathForShow.is_absolute():
            if pathForShow.is_relative_to(Path.cwd()):
                pathForShow = pathForShow.relative_to(Path.cwd())
        return str(pathForShow)

class TaskParameter(lg.Parameter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _warn_on_wrong_param_type(self, param_name, param_value):
        if self.__class__ != TaskParameter:
            return
        if not isinstance(param_value, Task):
            raise ValueError("parameter {} must be a Eikthyr task, got {} instead".format(param_name, param_value))

    def serialize(self, x):
        try:
            return x.output().pathRel
        except:
            return super().serialize(x)

class TaskListParameter(lg.Parameter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _warn_on_wrong_param_type(self, param_name, param_value):
        if self.__class__ != TaskListParameter:
            return
        for t in param_value:
            if not isinstance(t, lg.Task):
                raise ValueError("parameter {} must be a list of Eikthyr task, got {} instead".format(param_name, param_value))

    def serialize(self, xs):
        try:
            return ' '.join(sorted([x.output().pathRel for x in xs]))
        except:
            return super().serialize(xs)

class TargetParameter(lg.Parameter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _warn_on_wrong_param_type(self, param_name, param_value):
        if self.__class__ != TargetParameter:
            return
        if not isinstance(param_value, Target):
            raise ValueError("parameter {} must be a Eikthyr target, got {} instead".format(param_name, param_value))

    def serialize(self, x):
        try:
            return x.pathRel
        except:
            return super().serialize(x)
