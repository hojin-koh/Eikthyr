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

from .data import MetaTarget

class TaskParameter(lg.Parameter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.significant = False

    def _warn_on_wrong_param_type(self, param_name, param_value):
        if self.__class__ != TaskParameter:
            return
        if not isinstance(param_value, lg.Task):
            raise ValueError("parameter {} must be a Eikthyr task, got {} instead".format(param_name, param_value))

class TargetParameter(lg.Parameter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _warn_on_wrong_param_type(self, param_name, param_value):
        if self.__class__ != TargetParameter:
            return
        if not isinstance(param_value, MetaTarget):
            raise ValueError("parameter {} must be a Eikthyr target, got {} instead".format(param_name, param_value))
