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
import time
from pathlib import Path

import hypothesis.strategies as st
from hypothesis import given, example
from .common import TestFieldForFile

from Eikthyr.target import Target, BinaryTarget

@given(content=st.text())
def test_writeTextFile(content):
    with TestFieldForFile() as _:
        tgt = Target("000")
        with tgt.fpWrite() as fpw:
            fpw.write("")
        # Overwrite with proper content
        with tgt.fpWrite() as fpw:
            fpw.write(content)

        assert Path("000").read_text().splitlines() == content.splitlines()

@given(content=st.binary())
def test_writeBinaryFile(content):
    with TestFieldForFile() as _:
        tgt = BinaryTarget("000")
        with tgt.fpWrite() as fpw:
            fpw.write(b"")
        # Overwrite with proper content
        with tgt.fpWrite() as fpw:
            fpw.write(content)

        assert Path("000").read_bytes() == content

@given(content=st.binary())
def test_writeDir(content):
    with TestFieldForFile() as _:
        tgt = Target("000")
        p = "999"
        with tgt.pathWrite() as fw:
            Path(fw).mkdir(parents=True)
            # Write proper content
            (Path(fw) / p).write_bytes(content)

        assert Path("000/999").read_bytes() == content

def test_mtime():
    with TestFieldForFile() as _:
        tgt = Target("000")
        with tgt.fpWrite() as fpw:
            fpw.write("123")
        time.sleep(0.01)  # Ensure there's a measurable time difference between writes
        mtimeInitial = os.path.getmtime(tgt.path)

        with tgt.fpWrite() as fpw:
            fpw.write("456")
        time.sleep(0.01)  # Ensure there's a measurable time difference between writes
        mtimeUpdated = os.path.getmtime(tgt.path)

        # Check mtime method matches file modification time
        assert tgt.mtime() == mtimeUpdated
        assert tgt.mtime() != mtimeInitial
