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

import pickle
from base64 import b85encode, b85decode
from pathlib import Path

import luigi as lg
from luigi.task import flatten

class PathParameter(lg.PathParameter):
    """An extended type of PathParameter from the luigi one.

    This class has an additional serializeShort for displaying the task.
    If the specified path is inside the current directory, this class displays it as a relative path.
    """

    def serializeShort(self, x):
        pathForShow = Path(x)
        if pathForShow.is_absolute():
            if pathForShow.is_relative_to(Path.cwd()):
                pathForShow = pathForShow.relative_to(Path.cwd())
        return str(pathForShow)

# TODO: Test
class WhateverParameter(lg.Parameter):
    """A special type of parameter to contain anything.

    When serialize/deserialize, this class pickle all its contents.
    """

    def _warn_on_wrong_param_type(self, param_name, param_value):
        return

    def serialize(self, x):
        return str(b85encode(pickle.dumps(x)), encoding='ASCII')

    def parse(self, x):
        return pickle.loads(b85decode(bytes(x, encoding='ASCII')))

    def serializeShort(self, x):
        return self.serialize(x)

# TODO: Test
class TaskParameter(WhateverParameter):
    """A task, presumed to be pulled in as a dependency."""

    def _warn_on_wrong_param_type(self, param_name, param_value):
        pass # TODO: lg.Task?

    def serializeShort(self, x):
        aRepr = []
        for out in flatten(x.output()):
            if hasattr(out, 'pathRel'):
                aRepr.append(out.pathRel)
            elif hasattr(out, 'path'):
                aRepr.append(out.path)
            else:
                aRepr.append(repr(out))
        return ';'.join(sorted(aRepr))

# TODO: Test
class TaskListParameter(WhateverParameter):
    """A list of tasks, presumed to be pulled in as dependencies."""

    def _warn_on_wrong_param_type(self, param_name, param_value):
        pass # TODO: lg.Task?

    def serializeShort(self, xs):
        aRepr = []
        for out in flatten((x.output() for x in xs)):
            if hasattr(out, 'pathRel'):
                aRepr.append(out.pathRel)
            elif hasattr(out, 'path'):
                aRepr.append(out.path)
            else:
                aRepr.append(repr(out))
        return ';'.join(sorted(aRepr))
