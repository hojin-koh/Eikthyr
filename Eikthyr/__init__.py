# -*- coding: utf-8 -*-

# Deal with luigi's annoying deprecation warning
import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import luigi as lg

from Eikthyr import data
from Eikthyr.data import Target

from Eikthyr import cmd
from Eikthyr.cmd import MixinCmdUtilities

from Eikthyr import task
from Eikthyr.task import Task
from Eikthyr.task import STask

from Eikthyr import envcheck
from Eikthyr.envcheck import EnvCheck

from Eikthyr import param
from Eikthyr.param import WhateverParameter
from Eikthyr.param import TaskParameter
from Eikthyr.param import TaskListParameter
from Eikthyr.param import TargetParameter
from Eikthyr.param import PathParameter
from luigi import Parameter
from luigi import ListParameter
from luigi import BoolParameter
from luigi import IntParameter
from luigi import FloatParameter

from Eikthyr import specialtask
from Eikthyr.specialtask import InputTask
from Eikthyr.specialtask import TargetWrapperTask
from Eikthyr.specialtask import StampTask

from Eikthyr import logging
from Eikthyr.logging import logger

from Eikthyr import run
from Eikthyr.run import run

from luigi import Config

