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

import time
from datetime import timedelta

import luigi as lg
from luigi.interface import _WorkerSchedulerFactory
from luigi import worker

from .cache import startCache
from .logging import logger

class _EikthyrFactory(_WorkerSchedulerFactory):
    def create_worker(self, scheduler, worker_processes, assistant=False):
        # Based on the suggestions in https://github.com/spotify/luigi/issues/2992
        return worker.Worker(scheduler=scheduler, worker_processes=worker_processes, assistant=assistant,
                check_complete_on_run=True,
                check_unfulfilled_deps=False,
                keep_alive=True,
                max_keep_alive_idle_duration=timedelta(seconds=1)
                )

def run(tasks, print_summary=True, workers=1):
    if workers > 1:
        startCache()
    if isinstance(tasks, lg.Task):
        tasks = (tasks,)
    t0 = time.time()
    rtn = lg.build(tasks, local_scheduler=True, log_level='WARNING', detailed_summary=True,
            workers=workers, worker_scheduler_factory=_EikthyrFactory())
    if print_summary:
        logger.info("Total Time Spent: {:.3f}s".format(time.time() - t0))
        logger.debug(rtn.summary_text)
    return rtn

