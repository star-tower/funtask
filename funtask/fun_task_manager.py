import functools
from abc import abstractmethod
from dataclasses import dataclass
from enum import unique, auto
from typing import Callable, List, TypeVar, Generic, Any, Tuple, Union, Dict

from mypy_extensions import VarArg

from funtask.utils import AutoName

_T = TypeVar('_T')


@unique
class WorkerStatus(AutoName):
    RUNNING = auto()  # type: ignore
    ERROR = auto()  # type: ignore


def _split_generator_and_dependencies(
        scope_generator: 'ScopeGeneratorWithDependencies'
) -> Tuple['ScopeGenerator', List[str]]:
    """
    change ScopeGeneratorWithDependencies to (ScopeGenerator, [dependencies_str...])
    :param scope_generator: ScopeGeneratorWithDependencies
    :return: (ScopeGenerator, [dependencies_str...])
    """
    if isinstance(scope_generator, Tuple):
        generator, dependencies = scope_generator
        if isinstance(dependencies, str):
            dependencies = [dependencies]
    elif scope_generator is None:
        generator, dependencies = lambda _: None, []
    else:
        generator, dependencies = scope_generator, []
    return generator, dependencies


def _warp_scope_generator(scope_generator: 'ScopeGeneratorWithDependencies') -> 'TransScopeGenerator':
    """
    warp scope_generator to callable with is_scope_regenerator and dependencies props
    """
    scope_generator, dependencies = _split_generator_and_dependencies(scope_generator)

    @functools.wraps(scope_generator)
    def generator_wrapper(scope):
        return scope_generator and scope_generator(scope)

    generator_wrapper.is_scope_regenerator = True
    generator_wrapper.dependencies = dependencies
    return generator_wrapper


@dataclass
class Worker:
    uuid: str
    status: WorkerStatus
    _task_manager: 'FunTaskManager'

    def dispatch_fun_task(
            self,
            func_task: 'FuncTask',
            *arguments
    ) -> 'Task[_T]':
        return self._task_manager.dispatch_fun_task(self.uuid, func_task, *arguments)

    def regenerate_scope(
            self,
            scope_generator: 'ScopeGeneratorWithDependencies'
    ):
        return self._task_manager.regenerate_worker_scope(self.uuid, scope_generator)

    def stop(
            self
    ):
        self._task_manager.stop_worker(self.uuid)


@unique
class TaskStatus(AutoName):
    QUEUED = auto()  # type: ignore
    RUNNING = auto()  # type: ignore
    SUCCESS = auto()  # type: ignore
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


class Logger:
    @abstractmethod
    def log(self, msg: str, level: LogLevel, tags: List[str]):
        ...


class StdLogger(Logger):

    def log(self, msg: str, level: LogLevel = LogLevel.INFO, tags: List[str] = None):
        tags = tags or ["default"]
        print(f"{level.value}-{tags}: {msg}")


ScopeGenerator = Callable[[Any], Any]
ScopeGeneratorWithDependencies = Tuple[ScopeGenerator, str | List[str]] | ScopeGenerator | None
FuncTask = Callable[[Any, Logger, VarArg(Any)], _T]


class _TransScopeGenerator:
    is_scope_regenerator: bool
    dependencies: List[str]


class WithGlobals:
    __globals__: Dict[str, Any]


TransScopeGenerator = Union[Callable[[Any], Any], _TransScopeGenerator, WithGlobals]


class FunTaskManager:
    @abstractmethod
    def increase_workers(
            self,
            scope_generator: ScopeGeneratorWithDependencies | List[
                ScopeGeneratorWithDependencies
            ] | None,
            number: int = None,
            *args,
            **kwargs
    ) -> List[Worker]:
        ...

    @abstractmethod
    def increase_worker(
            self,
            scope_generator: ScopeGeneratorWithDependencies,
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
            func_task: FuncTask,
            *arguments
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
            scope_generator: ScopeGeneratorWithDependencies
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
