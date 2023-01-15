import math
from dataclasses import field
from enum import unique, auto
from typing import NewType, TypeVar, List, Any, Dict, Optional, Union

from pydantic.dataclasses import dataclass

from funtask.utils.enum_utils import AutoName

TaskUUID = NewType("TaskUUID", str)

WorkerUUID = NewType("WorkerUUID", str)

FuncGroupUUID = NewType("FuncGroupUUID", str)
TaskGroupUUID = NewType("TaskGroupUUID", str)
CronTaskUUID = NewType("CronTaskUUID", str)
FuncUUID = NewType("FuncUUID", str)
ParameterSchemaUUID = NewType("ParameterSchemaUUID", str)
TaskQueryCursor = NewType("TaskQueryCursor", str)
SchedulerNodeUUID = NewType("SchedulerNodeUUID", str)
TaskWorkerManagerNodeUUID = NewType("TaskWorkerManagerNodeUUID", str)
ClusterUUID = NewType("ClusterUUID", str)
FuncArgumentUUID = NewType("FuncArgumentUUID", str)
FuncArgumentGroupUUID = NewType("FuncArgumentGroupUUID", str)


@dataclass
class TaskWorkerManagerNode:
    uuid: TaskWorkerManagerNodeUUID
    host: str
    port: int


@dataclass
class SchedulerNode:
    uuid: SchedulerNodeUUID
    host: str
    port: int


@dataclass
class Task:
    uuid: TaskUUID
    parent_task_uuid: TaskUUID | CronTaskUUID | None
    uuid_in_manager: TaskUUID | None
    status: 'TaskStatus'
    worker_uuid: WorkerUUID | None
    func: 'Union[Func, FuncUUID]'
    argument: 'Optional[FuncArgument]'
    result_as_state: bool
    timeout: float
    description: str
    result: Any = None
    name: str | None = None


@dataclass
class ArgumentQueue:
    uuid: str
    name: str
    parameter_schema: 'ParameterSchema'


@unique
class ArgumentGenerateStrategy(AutoName):
    STATIC = auto()
    DROP = auto()
    SKIP = auto()
    FROM_QUEUE_END_REPEAT_LATEST = auto()
    FROM_QUEUE_END_SKIP = auto()
    FROM_QUEUE_END_DROP = auto()
    UDF = auto()


@dataclass
class ArgumentStrategy:
    strategy: ArgumentGenerateStrategy
    static_value: 'Optional[FuncArgument]'
    argument_queue: ArgumentQueue | None
    udf: 'Func'
    # part of udf arguments
    udf_extra: Dict[str, Any] | None


@unique
class WorkerChooseStrategy(AutoName):
    STATIC = auto()
    RANDOM_FROM_LIST = auto()
    RANDOM_FROM_WORKER_TAGS = auto()
    UDF = auto()


@dataclass
class WorkerStrategy:
    strategy: WorkerChooseStrategy
    static_worker: Optional[WorkerUUID]
    workers: Optional[List[WorkerUUID]]
    worker_tags: Optional[List[str]]
    udf: 'Func'
    udf_extra: Dict[str, Any] | None


@dataclass
class CronTask:
    uuid: CronTaskUUID
    timepoints: List['TimePoint']
    # use func uuid on insert exist
    func: 'Union[Func, FuncUUID]'
    argument_generate_strategy: ArgumentStrategy
    worker_choose_strategy: WorkerStrategy
    task_queue_strategy: 'QueueStrategy'
    result_as_state: bool
    timeout: float
    description: str
    disabled: bool
    name: str | None = None
    tags: List[str] = field(default_factory=list)


@dataclass
class Func:
    uuid: FuncUUID
    func: bytes
    dependencies: List[str]
    parameter_schema: 'Optional[Union[ParameterSchema, ParameterSchemaUUID]]'
    description: str
    tags: List[str] = field(default_factory=list)
    name: str | None = None


@dataclass
class FuncArgument:
    uuid: FuncArgumentUUID
    args: List[bytes]
    kwargs: Dict[str, bytes]


@dataclass
class FuncArgumentGroup:
    uuid: FuncArgumentGroupUUID


@dataclass
class FuncGroup:
    uuid: FuncGroupUUID


@dataclass
class ParameterSchema:
    uuid: ParameterSchemaUUID


@unique
class TimeUnit(AutoName):
    SECOND = auto()
    MINUTE = auto()
    HOUR = auto()
    DAY = auto()
    WEEK = auto()
    MILLISECOND = auto()


@dataclass
class TimePoint:
    unit: TimeUnit
    n: int
    at: str | None

    def __str__(self):
        return f"{self.n}/{self.unit}" + (f"/{self.at}" if self.at is not None else "")


@unique
class QueueFullStrategy(AutoName):
    SKIP = auto()
    DROP = auto()
    SEIZE = auto()
    UDF = auto()


@dataclass
class QueueStrategy:
    max_size = math.inf
    full_strategy: QueueFullStrategy
    udf: Func
    udf_extra: Dict[str, Any] | None


_T = TypeVar('_T')


@unique
class TaskStatus(AutoName):
    UNSCHEDULED = auto()
    SCHEDULED = auto()
    SKIP = auto()
    QUEUED = auto()
    RUNNING = auto()
    SUCCESS = auto()
    ERROR = auto()
    DIED = auto()


@unique
class WorkerStatus(AutoName):
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()
    DIED = auto()


@dataclass
class Tag:
    key: str
    value: str | None
    namespace: str


@dataclass
class Worker:
    uuid: WorkerUUID
    status: WorkerStatus
    name: str | None = None
    tags: List[Tag] = field(default_factory=list)
