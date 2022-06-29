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
import pickle
from hashlib import md5
from inspect import getsource

import logging
import time
from plumbum import FG

from luigi.task import flatten
from .data import Target

from logzero import setup_logger
logger = setup_logger('Eikthyr')

import os
from http.client import HTTPConnection
from urllib.parse import quote_plus

class Task(lg.Task):
    checkInputHash = True
    checkOutputHash = True
    checkCodeHash = True
    checkSignature = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if hasattr(self, 'generates'):
            self.objOutput = self.generates()
        else:
            self.objOutput = None
        self.cacheComplete = None
        self.hashSrc = None

    def output(self):
        return self.objOutput;

    # Expected to get a plumbum object
    def ex(self, chain):
        logger.info("EX: {}".format(chain))
        chain & FG

    def getSignature(self):
        if self.checkSignature:
            return repr(self)
        else:
            return "{}()".format(self.__class__.__name__)

    def getCode(self):
        return self.__class__.task

    def run(self):
        if not hasattr(self, 'timeStart'):
            logger.debug("Start {}".format(self))
            self.timeStart = time.time()
        rtn = self.task()
        self.invalidateCache()
        logger.info("End {} in {:.3f}s\n".format(self, time.time() - self.timeStart))
        return rtn

    def task(self):
        pass

    def getCodeHash(self):
        if self.checkCodeHash:
            return md5(pickle.dumps(getsource(self.getCode()), protocol=4), usedforsecurity=False).hexdigest()
        else:
            return '0'

    def getSrcHash(self):
        if self.hashSrc == None:
            self.hashSrc = []
            if self.checkInputHash:
                for tgt in flatten(self.input()):
                    if not isinstance(tgt, Target): continue
                    self.hashSrc.append(tgt.getMeta()['gen']['out'])
                self.hashSrc.sort()
        return self.hashSrc

    def invalidateCache(self):
        if 'EIKTHYR_CACHE_IP' in os.environ:
            try:
                h = HTTPConnection(os.environ['EIKTHYR_CACHE_IP'], int(os.environ['EIKTHYR_CACHE_PORT']))
                h.request('DELETE', '/{}'.format(quote_plus(repr(self))))
                h.getresponse()
            finally:
                h.close()
        else:
            self.cacheComplete = None

    def writeCache(self, rslt):
        if 'EIKTHYR_CACHE_IP' in os.environ:
            try:
                data = bytes(rslt)
                h = HTTPConnection(os.environ['EIKTHYR_CACHE_IP'], int(os.environ['EIKTHYR_CACHE_PORT']))
                h.request('PUT', '/{}'.format(quote_plus(repr(self))), body=data, headers={'Content-Length': len(data)})
                h.getresponse()
            finally:
                h.close()
        else:
            self.cacheComplete = rslt
        return rslt

    def complete(self):
        if 'EIKTHYR_CACHE_IP' in os.environ:
            try:
                h = HTTPConnection(os.environ['EIKTHYR_CACHE_IP'], int(os.environ['EIKTHYR_CACHE_PORT']))
                h.request('GET', '/{}'.format(quote_plus(repr(self))))
                r = h.getresponse()
                if r.status == 200:
                    return bool(r.read(int(r.headers['Content-Length'])))
            finally:
                h.close()
        elif self.cacheComplete is not None:
            return self.cacheComplete

        # Check whether the dependencies are fine
        for tgt in flatten(self.input()):
            if not isinstance(tgt, Target): continue
            if not tgt.task.complete():
                return self.writeCache(False)

        outputs = flatten(self.output())
        if len(outputs) == 0:
            return self.writeCache(False)
        for t in outputs:
            if isinstance(t, Target):
                if t.isOutdated():
                    return self.writeCache(False)
            else:
                if not t.exists():
                    return self.writeCache(False)
        return self.writeCache(True)
