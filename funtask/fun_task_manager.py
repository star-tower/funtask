from abc import abstractmethod
from dataclasses import dataclass
from enum import unique, auto
from typing import Callable, List, TypeVar, Generic, Any, Optional

from funtask.utils import AutoName

_T = TypeVar('_T')


@unique
class WorkerStatus(AutoName):
    RUNNING = auto()  # type: ignore
    ERROR = auto()  # type: ignore


@dataclass
class Worker:
    uuid: str
    status: WorkerStatus
    _task_manager: 'FunTaskManager'

    def dispatch_fun_task(
            self,
            func_task: Callable[[Any, 'Logger'], _T]
    ) -> 'Task[_T]':
        return self._task_manager.dispatch_fun_task(self.uuid, func_task)

    def regenerate_scope(
            self,
            scope_generator: Optional['ScopeGenerator']
    ):
        return self._task_manager.regenerate_worker_scope(self.uuid, scope_generator)

    def stop(
            self
    ):
        self._task_manager.stop_worker(self.uuid)


@unique
class TaskStatus(AutoName):
    RUNNING = auto()  # type: ignore
    DONE = auto()  # type: ignore
    ERROR = auto()  # type: ignore


@dataclass
class Task(Generic[_T]):
    uuid: str
    status: TaskStatus
    _task_manager: 'FunTaskManager'

    @abstractmethod
    async def get_result(self) -> _T:
        ...

    @abstractmethod
    async def get_log(self) -> str | List[str]:
        ...

    def stop(
            self
    ):
        self._task_manager.stop_task(self.uuid)


@unique
class LogLevel(AutoName):
    INFO = auto()  # type: ignore
    DEBUG = auto()  # type: ignore
    WARNING = auto()  # type: ignore
    ERROR = auto()  # type: ignore


@dataclass
class Logger:
    def log(self, level: LogLevel, tags: List[str], msg: str):
        ...


ScopeGenerator = Callable[[Any], Any]
FuncTask = Callable[[Any, Logger], _T]


class FunTaskManager:
    @abstractmethod
    def increase_workers(
            self,
            scope_generator: ScopeGenerator | List[ScopeGenerator] | None,
            number: int = None,
            *args,
            **kwargs
    ) -> List[Worker]:
        ...

    @abstractmethod
    def increase_worker(
            self,
            scope_generator: ScopeGenerator | List[ScopeGenerator] | None,
            *args,
            **kwargs
    ) -> Worker:
        ...

    @abstractmethod
    def get_worker_from_uuid(
            self,
            uuid: str
    ) -> Worker:
        ...

    @abstractmethod
    def dispatch_fun_task(
            self,
            worker_uuid: str,
            func_task: FuncTask
    ) -> Task[_T]:
        ...

    @abstractmethod
    def get_task_from_uuid(
            self,
            uuid: str
    ) -> Task:
        ...

    @abstractmethod
    def regenerate_worker_scope(
            self,
            worker_uuid: str,
            scope_generator: ScopeGenerator | None
    ):
        ...

    @abstractmethod
    def stop_task(
            self,
            task_uuid: str
    ):
        ...

    @abstractmethod
    def stop_worker(
            self,
            worker_uuid: str
    ):
        ...
