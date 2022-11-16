# -*- coding: utf-8 -*-

from dotenv import load_dotenv as _load_dotenv, find_dotenv as _find_dotenv
_load_dotenv(_find_dotenv(usecwd=True), override=True)

# Deal with luigi's annoying deprecation warning
import warnings as _warnings
with _warnings.catch_warnings():
    _warnings.simplefilter("ignore")
    import luigi as lg

from . import data
from .data import Target, WrapperTarget
from .data import ConfigData

from . import cmd
from .cmd import chdir, mkcd, withEnv, cmdfmt, getenv

from . import task
from .task import Task, STask, NITask

from . import envcheck
from .envcheck import EnvCheck

from . import param
from .param import WhateverParameter, TaskParameter, TaskListParameter, TargetParameter, PathParameter
from luigi import Parameter, BoolParameter, IntParameter, FloatParameter, ListParameter, DictParameter

from . import specialtask
from .specialtask import InputTask, TargetWrapperTask, StampTask

from . import logging
from .logging import logger

from . import run
from .run import run

from luigi import Config

from plumbum import local
cmd = local.cmd
