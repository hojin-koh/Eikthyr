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
from datetime import timedelta
from pathlib import Path

from pytest import fixture
from .common import TestFieldForFile

from Eikthyr.task import Task
from Eikthyr.param import PathParameter, TaskParameter

# Put all luigi imports after Eikthyr to suppress annoying warnings
import luigi as lg

aSideEffects = []

class TaskA(Task):
    out = PathParameter()

    def run(self):
        aSideEffects.append('TaskA.run')
        with self.output().fpWrite() as fpw:
            fpw.write("Hello")

class TaskB(Task):
    src = TaskParameter()
    out = PathParameter()

    def run(self):
        aSideEffects.append('TaskB.run')
        with self.input().open('r') as fp:
            buf = fp.read()
        with self.output().fpWrite() as fpw:
            fpw.write(buf + ", ")

class TaskC(Task):
    src = TaskParameter()
    out = PathParameter()

    def run(self):
        aSideEffects.append('TaskC.run')
        with self.input().open('r') as fp:
            buf = fp.read()
        with self.output().fpWrite() as fpw:
            fpw.write(buf + "World!")

def getStdTaskChain():
    tA = TaskA('a.txt')
    tB = TaskB(tA, 'b.txt')
    tC = TaskC(tB, 'c.txt')
    return tA, tB, tC

def test_canTaskRun():
    aSideEffects.clear()
    with TestFieldForFile() as _:
        tA, tB, tC = getStdTaskChain()
        lg.build([tA, tB, tC,], local_scheduler=True, log_level='WARNING', workers=1)

        with open('a.txt', 'r') as fp:
            assert fp.read() == "Hello"

        with open('b.txt', 'r') as fp:
            assert fp.read() == "Hello, "

        with open('c.txt', 'r') as fp:
            assert fp.read() == "Hello, World!"

        assert aSideEffects == ['TaskA.run', 'TaskB.run', 'TaskC.run']

def test_canTaskReRunMtime1():
    aSideEffects.clear()
    with TestFieldForFile() as _:
        tA, tB, tC = getStdTaskChain()
        lg.build([tA, tB, tC,], local_scheduler=True, log_level='WARNING', workers=1)
        Path('b.txt').touch()
        lg.build([tA, tB, tC,], local_scheduler=True, log_level='WARNING', workers=1)

        assert aSideEffects == ['TaskA.run', 'TaskB.run', 'TaskC.run', 'TaskC.run']

def test_canTaskReRunMtime2():
    aSideEffects.clear()
    with TestFieldForFile() as _:
        tA, tB, tC = getStdTaskChain()
        lg.build([tA, tB, tC,], local_scheduler=True, log_level='WARNING', workers=1)
        Path('a.txt').touch()
        lg.build([tA, tB, tC,], local_scheduler=True, log_level='WARNING', workers=1)

        assert aSideEffects == ['TaskA.run', 'TaskB.run', 'TaskC.run', 'TaskB.run', 'TaskC.run']
