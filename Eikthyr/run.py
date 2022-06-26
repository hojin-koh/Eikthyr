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
import time

from logzero import setup_logger
logger = setup_logger('Eikthyr')

def run(tasks, print_summary=True):
    t0 = time.time()
    rtn = lg.build(tasks, local_scheduler=True, log_level='WARNING', detailed_summary=True, workers=1)
    if print_summary:
        logger.info("Total Time Spent: {:.3f}s".format(time.time() - t0))
        logger.debug(rtn.summary_text)
    return rtn
