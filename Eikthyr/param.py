import luigi as lg

from .task import Task

class TaskParameter(lg.Parameter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.significant = False

    def _warn_on_wrong_param_type(self, param_name, param_value):
        if self.__class__ != TaskParameter:
            return
        if not isinstance(param_value, Task):
            raise ValueError("parameter {} must be a Eikthyr task, got {} instead".format(param_name, param_value))
