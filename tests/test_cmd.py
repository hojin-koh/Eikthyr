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

import os
from pathlib import Path

import hypothesis.strategies as st
from hypothesis import given, example

import Eikthyr as eik
import plumbum
from plumbum import local

def test_chdir(fs):
    os.mkdir('123456')
    dirCurrent = Path.cwd()
    with eik.chdir('123456'):
        assert (dirCurrent / '123456') == Path.cwd()
    assert dirCurrent == Path.cwd()

def test_mkcd(fs):
    dirCurrent = Path.cwd()
    with eik.mkcd('123456'):
        assert (dirCurrent / '123456') == Path.cwd()
    assert dirCurrent == Path.cwd()

@given(val=st.text())
def test_withenv(val):
    if val.find('\0') != -1:
        return
    with eik.withEnv(EIKTEST_TEST00=val):
        assert eik.getenv('EIKTEST_TEST00') == val
        assert local.env['EIKTEST_TEST00'] == val
        with eik.withEnv(EIKTEST_TEST00="ABC"+val):
            assert eik.getenv('EIKTEST_TEST00') == "ABC"+val
            assert local.env['EIKTEST_TEST00'] == "ABC"+val

def test_cmdfmt():
    assert eik.cmdfmt(['ls', '{}/'], 'tests')
