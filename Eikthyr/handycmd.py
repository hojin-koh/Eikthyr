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

def getGitDesc(path='.'):
    return cmd.git('-C', path, 'describe', '--long', '--always', '--dirty', '--tags', '--abbrev=16').strip()

def packZst(output, path='.', files=('.',), excludes=(), strip=0, zstd=22, key='', extras=()):
    if 'bsdtar' in local:
        tar = cmd.bsdtar
    else:
        tar = cmd.tar
    args = ['-cf', '-', '-C', path, '--strip-components', strip, *extras]
    for f in excludes:
        args.append('--exclude')
        args.append(f)
    args += files
    chain = tar[args]
    chain = chain | cmd.zstd['-T3', '-c', '--ultra', '-{}'.format(zstd)]
    # TODO: encryption
    (chain > output) & FG
