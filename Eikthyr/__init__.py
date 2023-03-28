# -*- coding: utf-8 -*-

from dotenv import load_dotenv as _load_dotenv, find_dotenv as _find_dotenv
_load_dotenv(_find_dotenv(usecwd=True), override=True)

# Deal with luigi's annoying deprecation warning
import warnings as _warnings
with _warnings.catch_warnings():
    _warnings.simplefilter("ignore")
    import luigi as lg

from . import param
from .param import PathParameter, WhateverParameter, TaskParameter, TaskListParameter
from luigi import Parameter, BoolParameter, IntParameter, FloatParameter, ListParameter, DictParameter

from . import target
from .target import Target, BinaryTarget

from . import cmd
from .cmd import chdir, mkcd, withEnv, cmdfmt, getenv

from . import task
from .task import BaseTask, Task
from .specialtask import InputTask

from . import envcheck
from .envcheck import EnvCheck

from . import logging
from .logging import logger

from . import run
from .run import run

from luigi import Config

# Convenient shortcuts from plumbum
from plumbum import local
cmd = local.cmd

# Convenient shortcuts from core python
from pathlib import Path
import shutil as sh
