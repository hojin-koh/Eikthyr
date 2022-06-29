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

from luigi.interface import _WorkerSchedulerFactory
from luigi import worker

from logzero import setup_logger
logger = setup_logger('Eikthyr')

import os
import atexit
import threading
from random import randint
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
mCache = {}
sock = None # The IPC server for exchanging cached task information

class _EikthyrFactory(_WorkerSchedulerFactory):
    def create_worker(self, scheduler, worker_processes, assistant=False):
        # Based on the suggestions in https://github.com/spotify/luigi/issues/2992
        return worker.Worker(
            scheduler=scheduler, worker_processes=worker_processes, assistant=assistant, check_complete_on_run=True, check_unfulfilled_deps=False)

class RequestHandler(BaseHTTPRequestHandler):
    # Suppress the log message from HTTP request
    def log_message(self, format, *args):
        return

    def do_GET(self):
        global mCache
        if self.path in mCache:
            #print('GET: {} 200 {}'.format(self.path, mCache[self.path]))
            self.send_response(200)
            self.send_header('Content-Type', "text/json")
            self.send_header('Content-Length', len(mCache[self.path]))
            self.end_headers()
            self.wfile.write(mCache[self.path])
        else:
            #print('GET: {} 404'.format(self.path))
            self.send_response(404)
            self.end_headers()

    def do_PUT(self):
        global mCache
        mCache[self.path] = self.rfile.read(int(self.headers['Content-Length']))
        #print('PUT: {} {}'.format(self.path, mCache[self.path]))
        self.send_response(201)
        self.end_headers()

    def do_DELETE(self):
        global mCache
        #print('DELETE: {}'.format(self.path))
        if self.path in mCache:
            del mCache[self.path]
        self.send_response(201)
        self.end_headers()

def startCache():
    # Start the cacche server if not already
    global sock
    if sock == None:
        sock = ThreadingHTTPServer(('127.{:d}.{:d}.{:d}'.format(randint(1,251), randint(1,251), randint(1,251)), 0), RequestHandler)
        thr = threading.Thread(target=sock.serve_forever, daemon=True)
        thr.start()
        ip, port = sock.server_address
        logger.debug("Task cache server started at {}:{}".format(ip, port))
        os.environ['EIKTHYR_CACHE_IP'] = ip
        os.environ['EIKTHYR_CACHE_PORT'] = str(port)

def run(tasks, print_summary=True, workers=1):
    if workers > 1:
        startCache()
    t0 = time.time()
    rtn = lg.build(tasks, local_scheduler=True, log_level='WARNING', detailed_summary=True,
            workers=workers, worker_scheduler_factory=_EikthyrFactory())
    if print_summary:
        logger.info("Total Time Spent: {:.3f}s".format(time.time() - t0))
        logger.debug(rtn.summary_text)
    return rtn

@atexit.register
def closeSocket():
    global sock
    if sock != None:
        sock.shutdown()
        sock.server_close()
        logger.debug('Task cache server stopped')
