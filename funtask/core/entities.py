from enum import unique, auto
from typing import NewType, TypeVar

from pydantic.dataclasses import dataclass

from funtask.utils.enum_utils import AutoName

TaskUUID = NewType("TaskUUID", str)

WorkerUUID = NewType("WorkerUUID", str)

FuncGroupUUID = NewType("FuncGroupUUID", str)
TaskGroupUUID = NewType("TaskGroupUUID", str)
CronTaskUUID = NewType("CronTaskUUID", str)
FuncUUID = NewType("FuncUUID", str)
FuncSchemaUUID = NewType("FuncSchema", str)
TaskQueryCursor = NewType("TaskQueryCursor", str)
SchedulerNodeUUID = NewType("SchedulerNodeUUID", str)
ClusterUUID = NewType("ClusterUUID", str)


@dataclass
class Task:
    ...


@dataclass
class Func:
    ...


@dataclass
class FuncArgument:
    ...


@dataclass
class FuncArgumentGroup:
    ...


@dataclass
class FuncGroup:
    ...


@dataclass
class FuncParameterSchema:
    ...


@dataclass
class TimePoints:
    ...


_T = TypeVar('_T')


@unique
class TaskStatus(AutoName):
    QUEUED = auto()
    RUNNING = auto()
    SUCCESS = auto()
    ERROR = auto()
    DIED = auto()


@unique
class WorkerStatus(AutoName):
    HEARTBEAT = auto()
