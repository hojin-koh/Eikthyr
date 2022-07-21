# -*- coding: utf-8 -*-

# Deal with luigi's annoying deprecation warning
import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import luigi as lg

from . import data
from .data import Target
from .data import ConfigData

from . import cmd
from .cmd import MixinCmdUtilities

from . import task
from .task import Task as Task
from .task import STask

from . import envcheck
from .envcheck import EnvCheck

from . import param
from .param import WhateverParameter
from .param import TaskParameter
from .param import TaskListParameter
from .param import TargetParameter
from .param import PathParameter
from luigi import Parameter
from luigi import BoolParameter
from luigi import IntParameter
from luigi import FloatParameter
from luigi import ListParameter
from luigi import DictParameter

from . import specialtask
from .specialtask import InputTask
from .specialtask import TargetWrapperTask
from .specialtask import StampTask

from . import logging
from .logging import logger

from . import run
from .run import run

from luigi import Config

