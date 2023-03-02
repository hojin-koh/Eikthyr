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

import luigi as lg
from pathlib import Path

import hypothesis.strategies as st
from hypothesis import given, example
from hypothesis_fspaths import fspaths
from pytest import fixture

import Eikthyr.param

class TaskForPath(lg.Task):
    p = Eikthyr.param.PathParameter()

# Utility function to get the actual Parameter object
def getParameterObjAndVal(task, name):
    pReal = dict(task.get_params())[name]
    val = dict(task.get_param_values(task.get_params(), [], task.param_kwargs))[name]
    return pReal, val

@given(p=st.text())
def test_pathparameterShort(p):
    if len(p) <= 0: return
    t1 = TaskForPath(p)
    pReal, val = getParameterObjAndVal(t1, 'p')
    assert pReal.serialize(val) == p

    t2 = TaskForPath(Path.cwd() / p)
    pReal, val = getParameterObjAndVal(t2, 'p')
    assert pReal.serializeShort(val) == p

@given(p=st.text())
def test_pathparameterRestore(p):
    if len(p) <= 0: return
    t1 = TaskForPath(p)
    pReal, val = getParameterObjAndVal(t1, 'p')
    assert pReal.serialize(pReal.parse(pReal.serialize(val))) == p
