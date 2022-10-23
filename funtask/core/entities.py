import math
from enum import unique, auto
from typing import NewType, TypeVar, List, Any, Dict

from pydantic.dataclasses import dataclass

from funtask.utils.enum_utils import AutoName

TaskUUID = NewType("TaskUUID", str)

WorkerUUID = NewType("WorkerUUID", str)

FuncGroupUUID = NewType("FuncGroupUUID", str)
TaskGroupUUID = NewType("TaskGroupUUID", str)
CronTaskUUID = NewType("CronTaskUUID", str)
FuncUUID = NewType("FuncUUID", str)
FuncParameterSchemaUUID = NewType("FuncParameterSchemaUUID", str)
TaskQueryCursor = NewType("TaskQueryCursor", str)
SchedulerNodeUUID = NewType("SchedulerNodeUUID", str)
ClusterUUID = NewType("ClusterUUID", str)
FuncArgumentUUID = NewType("FuncArgumentUUID", str)
FuncArgumentGroupUUID = NewType("FuncArgumentGroupUUID", str)


@dataclass
class Task:
    uuid: TaskUUID
    uuid_in_manager: TaskUUID
    status: 'TaskStatus'
    worker_uuid: WorkerUUID
    func: 'Func'
    argument: 'FuncArgument'
    result_as_state: bool
    timeout: float


@dataclass
class CronTask:
    uuid: CronTaskUUID


@dataclass
class Func:
    uuid: FuncUUID
    func: bytes
    dependencies: List[str]
    parameter_schema: 'FuncParameterSchema'


@dataclass
class FuncArgument:
    uuid: FuncArgumentUUID
    args: List[Any]
    kwargs: Dict[str, Any]


@dataclass
class FuncArgumentGroup:
    uuid: FuncArgumentGroupUUID


@dataclass
class FuncGroup:
    uuid: FuncGroupUUID


@dataclass
class FuncParameterSchema:
    uuid: FuncParameterSchemaUUID


@dataclass
class TimePoints:
    ...


@unique
class QueueFullBehavior(AutoName):
    DROP = auto()
    SEIZE = auto()


@dataclass
class QueueBehavior:
    max_size = math.inf
    full_behavior: QueueFullBehavior


_T = TypeVar('_T')


@unique
class TaskStatus(AutoName):
    UNSCHEDULED = auto()
    QUEUED = auto()
    RUNNING = auto()
    SUCCESS = auto()
    ERROR = auto()
    DIED = auto()


@unique
class WorkerStatus(AutoName):
    HEARTBEAT = auto()
