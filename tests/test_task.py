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
import os
from datetime import timedelta
from luigi.interface import _WorkerSchedulerFactory
from luigi import worker
from pathlib import Path

from pytest import fixture
from .common import TestFieldForFile

from Eikthyr.task import Task
from Eikthyr.param import PathParameter, TaskParameter

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

class TestFactory(_WorkerSchedulerFactory):
    def create_worker(self, scheduler, worker_processes, assistant=False):
        # Based on the suggestions in https://github.com/spotify/luigi/issues/2992
        return worker.Worker(scheduler=scheduler, worker_processes=worker_processes, assistant=assistant,
                cache_task_completion=True,
                check_complete_on_run=True,
                check_unfulfilled_deps=False,
                keep_alive=True,
                max_keep_alive_idle_duration=timedelta(seconds=1)
                )

def test_canTaskRun():
    aSideEffects.clear()
    with TestFieldForFile() as _:
        tA, tB, tC = getStdTaskChain()
        lg.build([tA, tB, tC,], local_scheduler=True, log_level='WARNING', workers=1, worker_scheduler_factory=TestFactory())

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
        lg.build([tA, tB, tC,], local_scheduler=True, log_level='WARNING', workers=1, worker_scheduler_factory=TestFactory())
        Path('b.txt').touch()
        lg.build([tA, tB, tC,], local_scheduler=True, log_level='WARNING', workers=1, worker_scheduler_factory=TestFactory())

        assert aSideEffects == ['TaskA.run', 'TaskB.run', 'TaskC.run', 'TaskC.run']

def test_canTaskReRunMtime2():
    aSideEffects.clear()
    with TestFieldForFile() as _:
        tA, tB, tC = getStdTaskChain()
        lg.build([tA, tB, tC,], local_scheduler=True, log_level='WARNING', workers=1, worker_scheduler_factory=TestFactory())
        Path('a.txt').touch()
        lg.build([tA, tB, tC,], local_scheduler=True, log_level='WARNING', workers=1, worker_scheduler_factory=TestFactory())

        assert aSideEffects == ['TaskA.run', 'TaskB.run', 'TaskC.run', 'TaskB.run', 'TaskC.run']
