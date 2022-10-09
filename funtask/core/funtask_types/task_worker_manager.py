import time
from abc import abstractmethod
from dataclasses import dataclass, field
from enum import unique, auto

from mypy_extensions import VarArg
from typing import Callable, List, TypeVar, Generic, Any, Tuple, Dict, Awaitable, AsyncIterator

from funtask.utils.enum_utils import AutoName

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
    async def get_task_queue(self, worker_uuid: str) -> 'Queue[TaskQueueMessage]':
        ...

    @abstractmethod
    async def get_control_queue(self, worker_uuid: str) -> 'Queue[ControlQueueMessage]':
        ...


FuncTask = Callable[[Any, Logger, VarArg(Any)], _T] | Callable[[Any, Logger, VarArg(Any)], Awaitable[_T]]


@dataclass
class Task:
    uuid: str
    task: FuncTask
    dependencies: List[str]
    result_as_state: bool


@dataclass
class TaskMeta:
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
    async def get(self, timeout: None | float = None) -> _T:
        """
        timeout will return None
        """
        ...

    @abstractmethod
    async def watch_and_get(self, break_ref: BreakRef, timeout: None | float = None) -> _T:
        """
        timeout or break flag in break_ref is true will return None
        """
        ...

    @abstractmethod
    async def qsize(self) -> int:
        ...

    @abstractmethod
    async def empty(self) -> bool:
        ...


QueueFactory = Callable[[str], Queue]


class KVDB:
    @abstractmethod
    async def set(self, key: str, value: bytes):
        ...

    @abstractmethod
    async def get(self, key: str) -> bytes:
        ...

    @abstractmethod
    async def delete(self, key: str):
        ...

    @abstractmethod
    async def foreach(self, key: str) -> AsyncIterator[bytes]:
        ...

    @abstractmethod
    async def push(self, key: str, *value: bytes):
        ...

    @abstractmethod
    async def random_pop(self, key: str) -> bytes:
        ...

    @abstractmethod
    async def remove(self, key: str, *items: bytes):
        ...


@dataclass
class TaskQueueMessage:
    task: 'Task'
    task_meta: 'TaskMeta'
    create_timestamp: float = field(default_factory=time.time)


@dataclass
class StatusQueueMessage:
    worker_uuid: str
    task_uuid: None | str
    status: TaskStatus | WorkerStatus
    content: Any
    create_timestamp: float = field(default_factory=time.time)


@dataclass
class ControlQueueMessage:
    worker_uuid: str
    control_sig: TaskControl
    create_timestamp: float = field(default_factory=time.time)


@dataclass
class WorkerQueue:
    task_queue: Queue[TaskQueueMessage]
    status_queue: Queue[StatusQueueMessage]
    control_queue: Queue[ControlQueueMessage]
    create_timestamp: float = field(default_factory=time.time)


@dataclass
class TaskInstance(Generic[_T]):
    uuid: str
    worker_uuid: str
    _task_manager: 'FunTaskManager'

    async def stop(
            self
    ):
        await self._task_manager.stop_task(self.worker_uuid, self.uuid)


@dataclass
class StatusReport:
    worker_uuid: str
    task_uuid: str | None
    status: TaskStatus | WorkerStatus
    content: Any
    create_timestamp: float


TaskInput = Tuple[FuncTask, List[str]] | None | FuncTask


@dataclass
class WorkerInstance:
    uuid: str
    _task_manager: 'FunTaskManager'

    async def dispatch_fun_task(
            self,
            func_task: 'FuncTask',
            *arguments,
            **kwargs
    ) -> 'TaskInstance[_T]':
        return await self._task_manager.dispatch_fun_task(self.uuid, func_task, *arguments, **kwargs)

    async def regenerate_state(
            self,
            state_generator: 'FuncTask'
    ):
        return await self._task_manager.generate_worker_state(self.uuid, state_generator)

    async def stop(
            self
    ):
        await self._task_manager.stop_worker(self.uuid)

    async def kill(self):
        await self._task_manager.kill_worker(self.uuid)


class FunTaskManager:
    @abstractmethod
    async def increase_workers(
            self,
            number: int = None,
            *args,
            **kwargs
    ) -> List[WorkerInstance]:
        ...

    @abstractmethod
    async def increase_worker(
            self,
            *args,
            **kwargs
    ) -> WorkerInstance:
        ...

    @abstractmethod
    async def dispatch_fun_task(
            self,
            worker_uuid: str,
            func_task: TaskInput,
            change_status=False,
            timeout=None,
            *arguments,
            **kwargs
    ) -> TaskInstance[_T]:
        ...

    @abstractmethod
    async def generate_worker_state(
            self,
            worker_uuid: str,
            state_generator: TaskInput,
            timeout=None,
            *arguments
    ) -> TaskInstance[_T]:
        ...

    @abstractmethod
    async def stop_task(
            self,
            worker_uuid: str,
            task_uuid: str
    ):
        ...

    @abstractmethod
    async def stop_worker(
            self,
            worker_uuid: str
    ):
        ...

    @abstractmethod
    async def kill_worker(
            self,
            worker_uuid: str
    ):
        ...

    @abstractmethod
    async def get_queued_status(
            self,
            timeout: None | float = None
    ) -> StatusReport | None:
        ...
