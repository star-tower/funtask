from abc import abstractmethod, ABC
from typing import NewType

from funtask.core.funtask_types.task_worker_manager import FunTaskManager

TaskGroupUUID = NewType("TaskGroupUUID", str)
CronTaskUUID = NewType("CronTaskUUID", str)


class Scheduler(FunTaskManager, ABC):
    ...
