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

import atexit
import json
import os
import threading
from contextlib import contextmanager
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from random import randint
from urllib.parse import quote_plus

from .logging import logger

mCache = {}
sock = None # The IPC server for exchanging cached task information


def isAvailable():
    return 'EIKTHYR_CACHE_IP' in os.environ and 'EIKTHYR_CACHE_PORT' in os.environ

@contextmanager
def openConnection():
    try:
        h = HTTPConnection(os.environ['EIKTHYR_CACHE_IP'], int(os.environ['EIKTHYR_CACHE_PORT']))
        yield h
    finally:
        h.close()


def getObj(obj):
    with openConnection() as h:
        h.request('GET', '/{}'.format(quote_plus(repr(obj))))
        r = h.getresponse()
        if r.status == 200:
            if 'Content-Length' in r.headers:
                return json.loads(r.read(int(r.headers['Content-Length'])))
            else:
                return json.loads(r.read())
        else:
            return None

def putObj(obj, content):
    with openConnection() as h:
        data = bytes(json.dumps(content), 'ascii')
        h.request('PUT', '/{}'.format(quote_plus(repr(obj))), body=data, headers={'Content-Length': len(data)})
        h.getresponse()

def deleteObj(obj):
    with openConnection() as h:
        h.request('DELETE', '/{}'.format(quote_plus(repr(obj))))
        h.getresponse()


## === Server Part Below ===

class RequestHandler(BaseHTTPRequestHandler):
    # Suppress the log message from HTTP request
    def log_message(self, format, *args):
        return

    def do_GET(self):
        if self.path in mCache:
            self.send_response(200)
            self.send_header('Content-Type', "text/json")
            self.send_header('Content-Length', len(mCache[self.path]))
            self.end_headers()
            self.wfile.write(mCache[self.path])
        else:
            self.send_response(404)
            self.end_headers()

    def do_PUT(self):
        mCache[self.path] = self.rfile.read(int(self.headers['Content-Length']))
        self.send_response(201)
        self.end_headers()

    def do_DELETE(self):
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

@atexit.register
def stopCache():
    global sock
    if sock != None:
        sock.shutdown()
        sock.server_close()
        sock = None
        logger.debug('Task cache server stopped')
