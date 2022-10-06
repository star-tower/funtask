from abc import abstractmethod
from dataclasses import dataclass
from enum import unique, auto

from mypy_extensions import VarArg
from typing import Callable, List, TypeVar, Generic, Any, Tuple, Dict, Awaitable

from funtask.core.utils.enum_utils import AutoName

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
    async def increase_worker(
            self,
            task_queue_factory: 'Callable[[str], Queue[Tuple[bytes, TransTaskMeta]]]',
            task_status_queue: 'Queue[Tuple[str, str, TaskStatus | WorkerStatus, Any]]',
            control_queue_factory: 'Callable[[str], Queue[Tuple[str, TaskControl]]]',
            *args,
            **kwargs
    ) -> str:
        ...

    @abstractmethod
    async def kill_worker(self, worker_uuid: str):
        ...

    @abstractmethod
    async def stop_worker(self, worker_uuid: str):
        ...

    @abstractmethod
    async def get_task_queue(self, worker_uuid: str) -> 'Queue[Tuple[bytes, TransTaskMeta]]':
        ...

    @abstractmethod
    async def get_control_queue(self, worker_uuid: str) -> 'Queue[Tuple[str, TaskControl]]':
        ...


FuncTask = Callable[[Any, Logger, VarArg(Any)], _T] | Callable[[Any, Logger, VarArg(Any)], Awaitable[_T]]


@dataclass
class TransTask:
    uuid: str
    task: FuncTask
    dependencies: List[str]
    result_as_state: bool


@dataclass
class TransTaskMeta:
    arguments: Tuple[Any]
    kw_arguments: Dict[str, Any]
    timeout: float | None = None


class BreakRef:
    @abstractmethod
    def if_break_now(self) -> bool:
        ...


class Queue(Generic[_T]):
    type: str
    config: Dict

    @abstractmethod
    async def put(self, obj: _T):
        ...

    @abstractmethod
    async def get(self) -> _T:
        ...

    @abstractmethod
    async def watch_and_get(self, break_ref: BreakRef) -> _T:
        ...

    @abstractmethod
    async def qsize(self) -> int:
        ...

    @abstractmethod
    async def empty(self) -> bool:
        ...


QueueFactory = Callable[[str], Queue]
