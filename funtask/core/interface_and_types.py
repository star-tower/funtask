from abc import abstractmethod
from contextlib import contextmanager
from dataclasses import field
import time
from datetime import datetime
from enum import unique, auto
from typing import Callable, List, Generic, TypeVar, Dict, AsyncIterator, Tuple, Any, Awaitable, Generator

from mypy_extensions import VarArg
from dataclasses import dataclass
from funtask.core import entities
from funtask.utils.enum_utils import AutoName


class RecordNotFoundException(Exception):
    ...


class EmptyQueueException(Exception):
    ...


class StatusChangeException(Exception):
    ...


class BreakRef:
    @abstractmethod
    def if_break_now(self) -> bool:
        ...


_T = TypeVar('_T')


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
    async def get_front(self) -> _T | None:
        """
        get first element of queue, if queue is empty, raise an EmptyQueueException
        :return:
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
    ) -> 'entities.WorkerUUID':
        ...

    @abstractmethod
    async def kill_worker(self, worker_uuid: 'entities.WorkerUUID'):
        ...

    @abstractmethod
    async def stop_worker(self, worker_uuid: 'entities.WorkerUUID'):
        ...

    @abstractmethod
    async def get_task_queue(self, worker_uuid: 'entities.WorkerUUID') -> 'Queue[TaskQueueMessage]':
        ...

    @abstractmethod
    async def get_control_queue(self, worker_uuid: 'entities.WorkerUUID') -> 'Queue[ControlQueueMessage]':
        ...


QueueFactory = Callable[[str], Queue]

FuncTask = Callable[[Any, Logger, VarArg(Any)], _T] | Callable[[Any, Logger, VarArg(Any)], Awaitable[_T]]


@dataclass
class InnerTask:
    uuid: 'entities.TaskUUID'
    task: FuncTask
    dependencies: List[str]
    result_as_state: bool


@dataclass
class InnerTaskMeta:
    arguments: Tuple[Any]
    kw_arguments: Dict[str, Any]
    timeout: float | None = None


@dataclass
class TaskQueueMessage:
    task: 'InnerTask'
    task_meta: 'InnerTaskMeta'
    create_timestamp: float = field(default_factory=time.time)


@dataclass
class StatusQueueMessage:
    worker_uuid: entities.WorkerUUID
    task_uuid: None | entities.TaskUUID
    # status is None means this is a worker heart beat
    status: entities.TaskStatus | entities.WorkerStatus | None
    content: Any
    create_timestamp: float = field(default_factory=time.time)


@unique
class TaskControl(AutoName):
    KILL = auto()


@dataclass
class ControlQueueMessage:
    worker_uuid: entities.WorkerUUID
    control_sig: TaskControl
    create_timestamp: float = field(default_factory=time.time)


@dataclass
class WorkerQueue:
    task_queue: Queue[TaskQueueMessage]
    status_queue: Queue[StatusQueueMessage]
    control_queue: Queue[ControlQueueMessage]
    create_timestamp: float = field(default_factory=time.time)


@dataclass
class StatusReport:
    worker_uuid: entities.WorkerUUID
    task_uuid: entities.TaskUUID | None
    status: entities.TaskStatus | entities.WorkerStatus
    content: Any
    create_timestamp: float


TaskInput = Tuple[FuncTask, List[str]] | None | FuncTask


class FunTaskManager:
    @abstractmethod
    async def increase_workers(
            self,
            number: int = None,
            *args,
            **kwargs
    ) -> List[entities.WorkerUUID]:
        ...

    @abstractmethod
    async def increase_worker(
            self,
            *args,
            **kwargs
    ) -> entities.WorkerUUID:
        ...

    @abstractmethod
    async def dispatch_fun_task(
            self,
            worker_uuid: entities.WorkerUUID,
            func_task: TaskInput,
            change_status=False,
            timeout=None,
            *arguments,
            **kwargs
    ) -> entities.TaskUUID:
        ...

    @abstractmethod
    async def generate_worker_state(
            self,
            worker_uuid: entities.WorkerUUID,
            state_generator: TaskInput,
            timeout=None,
            *arguments
    ) -> entities.TaskUUID:
        ...

    @abstractmethod
    async def stop_task(
            self,
            worker_uuid: entities.WorkerUUID,
            task_uuid: entities.TaskUUID
    ):
        ...

    @abstractmethod
    async def stop_worker(
            self,
            worker_uuid: entities.WorkerUUID
    ):
        ...

    @abstractmethod
    async def kill_worker(
            self,
            worker_uuid: entities.WorkerUUID
    ):
        ...

    @abstractmethod
    async def get_queued_status(
            self,
            timeout: None | float = None
    ) -> StatusReport | None:
        ...


class RPCFunTaskManager:
    @abstractmethod
    async def increase_workers(
            self,
            number: int = None
    ) -> List[entities.WorkerUUID]:
        ...

    @abstractmethod
    async def increase_worker(
            self
    ) -> entities.WorkerUUID:
        ...

    @abstractmethod
    async def dispatch_fun_task(
            self,
            worker_uuid: entities.WorkerUUID,
            func_task: bytes,
            dependencies: List[str],
            change_status: bool,
            timeout: float,
            argument: entities.FuncArgument
    ) -> entities.TaskUUID:
        ...

    @abstractmethod
    async def stop_task(
            self,
            worker_uuid: entities.WorkerUUID,
            task_uuid: entities.TaskUUID
    ):
        ...

    @abstractmethod
    async def stop_worker(
            self,
            worker_uuid: entities.WorkerUUID
    ):
        ...

    @abstractmethod
    async def kill_worker(
            self,
            worker_uuid: entities.WorkerUUID
    ):
        ...

    @abstractmethod
    async def get_queued_status(
            self,
            timeout: None | float = None
    ) -> StatusReport | None:
        ...

    @abstractmethod
    async def get_task_queue_size(self, worker: entities.WorkerUUID) -> int:
        ...


class SchedulerNode:
    uuid: entities.SchedulerNodeUUID
    ip: str
    port: int


class LeaderControl:
    @abstractmethod
    async def get_leader(self) -> SchedulerNode | None:
        ...

    @abstractmethod
    async def elect_leader(self, uuid: entities.SchedulerNodeUUID) -> bool:
        ...

    @abstractmethod
    async def is_he_leader(self, uuid: entities.SchedulerNodeUUID) -> bool:
        ...

    @abstractmethod
    async def get_all_nodes(self) -> List[SchedulerNode]:
        ...

    @abstractmethod
    async def get_cluster_id(self) -> entities.ClusterUUID:
        ...


class LeaderSchedulerRPC:
    @abstractmethod
    async def assign_task_to_node(
            self,
            node: SchedulerNode,
            cron_task_uuid: entities.CronTaskUUID,
            start_time: datetime = None
    ):
        ...

    @abstractmethod
    async def get_node_task_list(self, node: SchedulerNode) -> List[entities.CronTaskUUID]:
        ...

    @abstractmethod
    async def remove_task_from_node(
            self,
            node: SchedulerNode,
            cron_task_uuid: entities.CronTaskUUID,
            start_time: datetime = None
    ):
        ...

    @abstractmethod
    async def get_all_nodes(self) -> List[SchedulerNode]:
        ...


class Cron:
    @abstractmethod
    async def every_n_seconds(self, name: str, n: int, task: Callable, at: str = None, *args, **kwargs):
        ...

    @abstractmethod
    async def every_n_minutes(self, name: str, n: int, task: Callable, at: str = None, *args, **kwargs):
        ...

    @abstractmethod
    async def every_n_hours(self, name: str, n: int, task: Callable, at: str = None, *args, **kwargs):
        ...

    @abstractmethod
    async def every_n_days(self, name: str, n: int, task: Callable, at: str = None, *args, **kwargs):
        ...

    @abstractmethod
    async def every_n_weeks(self, name: str, n: int, task: Callable, at: str = None, *args, **kwargs):
        ...

    @abstractmethod
    async def every_n_millisecond(self, name: str, n: int, task: Callable, *args, **kwargs):
        ...

    @abstractmethod
    async def cancel(self, name: str):
        ...

    @abstractmethod
    async def get_all(self) -> List[str]:
        ...


class RollbackException(Exception):
    ...


class Repository:
    @abstractmethod
    async def session_ctx(self):
        """
        should be context manager \n
        e.g. `with session_ctx() as session: `\n
        if ctx yield RollbackException, should rollback
        """
        ...

    @abstractmethod
    async def get_task_from_uuid(
            self,
            task_uuid: entities.TaskUUID,
            session=None
    ) -> entities.Task:
        ...

    @abstractmethod
    async def get_cron_task_from_uuid(
            self,
            task_uuid: entities.CronTaskUUID,
            session=None
    ) -> entities.CronTask:
        ...

    @abstractmethod
    async def get_all_cron_task(self, session=None) -> List[entities.CronTask]:
        ...

    @abstractmethod
    async def add_task(self, task: entities.Task, session=None) -> entities.TaskUUID:
        ...

    @abstractmethod
    async def change_task_status(self, task_uuid: entities.TaskUUID, status: entities.TaskStatus, session=None):
        ...

    @abstractmethod
    async def add_func(self, func: entities.Func, session=None) -> entities.FuncUUID:
        ...

    @abstractmethod
    async def add_worker(self, worker: entities.Worker, session=None) -> entities.WorkerUUID:
        ...

    @abstractmethod
    async def get_worker_from_uuid(self, task_uuid: entities.WorkerUUID, session=None) -> entities.Task:
        ...

    @abstractmethod
    async def change_worker_status(self, worker_uuid: entities.WorkerUUID, status: entities.WorkerStatus, session=None):
        ...

    @abstractmethod
    async def add_cron_task(self, task: entities.CronTask, session=None) -> entities.TaskUUID:
        ...

    @abstractmethod
    async def add_func_parameter_schema(
            self,
            func_parameter_schema: entities.FuncParameterSchema,
            session=None
    ) -> entities.FuncParameterSchemaUUID:
        ...

    @abstractmethod
    async def update_task_uuid_in_manager(
            self,
            task_uuid: entities.TaskUUID,
            task_uuid_in_manager: entities.TaskUUID,
            session=None
    ):
        ...

    @abstractmethod
    async def update_task(
            self,
            task_uuid: entities.TaskUUID,
            value: Dict[str, Any],
            session=None
    ):
        ...

    @abstractmethod
    async def update_worker_last_heart_beat_time(self, worker_uuid: entities.WorkerUUID, t: datetime, session=None):
        ...

    @abstractmethod
    async def get_workers_from_tags(
            self,
            tags: List[str],
            session=None
    ) -> List[entities.Worker]:
        ...


class Scheduler:
    @abstractmethod
    async def assign_task(self, task_uuid: entities.TaskUUID):
        ...

    @abstractmethod
    async def remove_cron_task(self, task_uuid: entities.CronTaskUUID) -> bool:
        """
        remove a task from scheduler, return if remove success
        :param task_uuid: task uuid
        :return:
        """
        ...

    @abstractmethod
    async def assign_cron_task(self, task_uuid: entities.CronTaskUUID):
        ...

    @abstractmethod
    async def get_all_cron_task(self) -> List[entities.CronTaskUUID]:
        ...

    @abstractmethod
    async def process_new_status(self, status_report: StatusReport):
        ...


class LeaderScheduler:
    @abstractmethod
    async def scheduler_node_change(self, scheduler_nodes: List[SchedulerNode]):
        ...

    @abstractmethod
    async def rebalance(self, rebalance_date: datetime):
        ...


class WebServer:
    @abstractmethod
    async def trigger_func(self, func: entities.Func, argument: entities.FuncArgument) -> entities.TaskUUID:
        ...

    @abstractmethod
    async def get_task_by_uuid(self, task_uuid: entities.TaskUUID) -> entities.Task | None:
        ...

    @abstractmethod
    async def get_tasks(
            self,
            tags: List[str] = None,
            func: entities.FuncUUID = None,
            cursor: entities.TaskQueryCursor = None,
            limit: int = None
    ) -> Tuple[List[entities.Task], entities.TaskQueryCursor]:
        ...

    @abstractmethod
    async def get_funcs(
            self,
            tags: List[str] = None,
            include_tmp: bool = False
    ) -> List[entities.Func]:
        ...

    @abstractmethod
    async def trigger_func_group(
            self,
            func_group: entities.FuncGroup,
            argument_group: entities.FuncArgumentGroup
    ) -> entities.TaskGroupUUID:
        ...

    @abstractmethod
    async def add_func_group(self, func_group: entities.FuncGroup) -> entities.FuncGroupUUID:
        ...

    @abstractmethod
    async def trigger_repeated_func(
            self,
            time_points: entities.TimePoint,
            func: entities.Func,
            argument: entities.FuncArgument
    ) -> entities.CronTaskUUID:
        ...

    @abstractmethod
    async def trigger_repeated_func_group(
            self,
            time_points: entities.TimePoint,
            func_group: entities.FuncGroup,
            argument_group: entities.FuncArgumentGroup
    ) -> entities.CronTaskUUID:
        ...

    @abstractmethod
    async def add_func(self, func: entities.Func) -> entities.FuncUUID:
        ...

    @abstractmethod
    async def add_parameter_schema(
            self,
            parameter_schema: entities.FuncParameterSchema
    ) -> entities.FuncParameterSchemaUUID:
        ...


class TimeoutException(Exception):
    ...


class DistributeLock:
    @contextmanager
    async def lock(self, name: str, timeout: float = None) -> Generator[None, None, None]:
        """
        lock a value, if timeout raise a TimeoutException, default no timeout
        :param name: global lock name
        :param timeout: timeout
        :return
        """
        ...

    @contextmanager
    async def try_lock(self, name: str) -> Generator[bool, None, None]:
        """
        try to get a lock, if lock occupied yield false
        :param name: global lock name
        :return:
        """
