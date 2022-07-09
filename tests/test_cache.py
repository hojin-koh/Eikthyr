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

from pytest import fixture
from hypothesis import given, example
import hypothesis.strategies as st

import Eikthyr.cache as cache

def test_nothing():
    assert not cache.isAvailable()

@fixture(scope="module")
def fixtureCache():
    cache.startCache()
    yield
    cache.stopCache()

@given(key=st.text(), val=st.booleans())
def test_bool(fixtureCache, key, val):
    cache.putObj(key, val)
    assert cache.getObj(key) == val
    cache.deleteObj(key)
    assert cache.getObj(key) == None

@given(key=st.text(), val=st.dictionaries(st.text(), st.text()))
def test_dict(fixtureCache, key, val):
    cache.putObj(key, val)
    assert cache.getObj(key) == val
    cache.deleteObj(key)
    assert cache.getObj(key) == None
