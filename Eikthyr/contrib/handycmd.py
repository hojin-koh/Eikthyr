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

from pathlib import Path

from plumbum import FG, local
import plumbum.cmd as cmd

from ..logging import logger

def getGitDesc(path='.'):
    return cmd.git('-C', path, 'describe', '--long', '--always', '--dirty', '--tags', '--abbrev=16').strip()

def packZst(output, path='.', files=('.',), excludes=(), strip=0, zstd=22, key='', extras=()):
    if 'bsdtar' in local:
        tar = cmd.bsdtar
    else:
        tar = cmd.tar
    if (isinstance(files, str)):
        files = (files,)
    if (isinstance(excludes, str)):
        excludes = (excludes,)
    if (isinstance(extras, str)):
        extras = (extras,)
    if (not isinstance(key, str)):
        key = str(key)
    args = ['-cf', '-', '-C', path, '--strip-components', strip, *extras]
    for f in excludes:
        args.append('--exclude')
        args.append(f)
    args += files
    chain = tar[args]
    chain = chain | cmd.zstd['-T3', '-c', '--ultra', '-{}'.format(zstd)]
    if len(key) > 0:
        chain = chain | cmd.openssl['enc', '-aria-256-ecb', '-pbkdf2', '-k', key]
    logger.debug("Pack {} < {}".format(output, ' '.join(files)))
    (chain > output) & FG

def unpackZst(src, path='.', key='', extras=()):
    if 'bsdtar' in local:
        tar = cmd.bsdtar
    else:
        tar = cmd.tar
    if (isinstance(extras, str)):
        extras = (extras,)
    if (not isinstance(key, str)):
        key = str(key)
    args = ['-xf', '-', '-C', path, *extras]
    if len(key) > 0:
        chain = cmd.openssl['enc', '-aria-256-ecb', '-d', '-pbkdf2', '-in', src, '-k', key] | tar[args]
    else:
        chain = tar[args] < src
    Path(path).mkdir(parents=True, exist_ok=True)
    logger.debug("Unpack {} > {}".format(src, path))
    chain & FG

def downloadFile(url, out):
    chain = cmd.curl['-qfLC', '-', '--ftp-pasv', '--retry', '5', '--retry-delay', '5', '-o', out, url]
    logger.debug(chain)
    chain & FG

