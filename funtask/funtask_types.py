from abc import abstractmethod
from dataclasses import dataclass
from enum import unique, auto

from mypy_extensions import VarArg
from typing import Callable, List, TypeVar, Generic, Any, Tuple, Union, Dict, Awaitable

from funtask.utils import AutoName

_T = TypeVar('_T')


@unique
class TaskStatus(AutoName):
    QUEUED = auto()
    RUNNING = auto()
    SUCCESS = auto()
    ERROR = auto()


@unique
class WorkerStatus(AutoName):
    HEARTBEAT = auto()


@unique
class TaskControl(AutoName):
    KILL = auto()


@dataclass
class TaskMeta:
    uuid: str
    arguments: Tuple[Any]
    timeout: float | None = None
    is_state_generator = False


@unique
class LogLevel(AutoName):
    INFO = auto()
    DEBUG = auto()
    WARNING = auto()
    ERROR = auto()


class Logger:
    @abstractmethod
    async def log(self, msg: str, level: LogLevel, tags: List[str]):
        ...


class WorkerManager:
    @abstractmethod
    def increase_worker(
            self,
            worker_uuid: str,
            task_queue: 'Queue[Tuple[bytes, TaskMeta]]',
            task_status_queue: 'Queue[Tuple[str, str, TaskStatus | WorkerStatus, Any]]',
            control_queue: 'Queue[Tuple[str, TaskControl]]',
            *args,
            **kwargs
    ):
        ...

    @abstractmethod
    def kill_worker(self, worker_uuid: str):
        ...

    @abstractmethod
    def stop_worker(self, worker_uuid: str):
        ...

    @abstractmethod
    def get_task_queue(self, worker_uuid: str) -> 'Queue[Tuple[bytes, TaskMeta]]':
        ...

    @abstractmethod
    def get_control_queue(self, worker_uuid: str) -> 'Queue[Tuple[str, TaskControl]]':
        ...


StateGenerator = Callable[[Any], Any]
StateGeneratorWithDependencies = Tuple[StateGenerator, str | List[str]] | StateGenerator | None
FuncTask = Callable[[Any, Logger, VarArg(Any)], _T] | Callable[[Any, Logger, VarArg(Any)], Awaitable[_T]]


class _TransStateGenerator:
    dependencies: List[str]


class WithGlobals:
    __globals__: Dict[str, Any]


TransStateGenerator = Union[FuncTask, _TransStateGenerator, WithGlobals]


class Queue(Generic[_T]):
    @abstractmethod
    async def put(self, obj: _T):
        ...

    @abstractmethod
    async def get(self) -> _T:
        ...

    @abstractmethod
    async def qsize(self) -> int:
        ...

    @abstractmethod
    async def empty(self) -> bool:
        ...


QueueFactory = Callable[[str], Queue]
