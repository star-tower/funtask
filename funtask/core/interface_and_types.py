from abc import abstractmethod
from contextlib import asynccontextmanager
from dataclasses import field
import time
from datetime import datetime
from enum import unique, auto
from typing import Callable, List, Generic, TypeVar, Dict, AsyncIterator, Tuple, Any, Awaitable

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
        yield

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
    async def peek_front_cache(self) -> _T | None:
        """
        get first element of queue, if queue is empty, raise an EmptyQueueException
        :return:
        """
        ...

    @abstractmethod
    async def watch_and_get(self, break_ref: BreakRef, timeout: None | float = None) -> _T | None:
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


@unique
class LogType(AutoName):
    TEXT = auto()
    MARKDOWN = auto()
    HTML = auto()


class Logger:
    @abstractmethod
    async def with_worker(self, worker: entities.WorkerUUID) -> 'Logger':
        """
        generate a new logger for specific worker
        :param worker: worker uuid
        :return: new logger
        """
        ...

    @abstractmethod
    async def with_task(self, task: entities.TaskUUID) -> 'Logger':
        """
        generate a new logger for specific task
        :param task: task uuid
        :return: new logger
        """
        ...

    @abstractmethod
    async def create_model_schema(self):
        ...

    @abstractmethod
    async def drop_model_schema(self):
        ...

    @abstractmethod
    @asynccontextmanager
    async def task_context(self, task_uuid: entities.TaskUUID):
        ...

    @abstractmethod
    async def log(self, msg: str, level: LogLevel, tags: List[str], type_: LogType = LogType.TEXT):
        ...

    async def info(self, msg: str, tags: List[str], type_: LogType = LogType.TEXT):
        await self.log(msg, LogLevel.INFO, tags, type_)

    async def debug(self, msg: str, tags: List[str], type_: LogType = LogType.TEXT):
        await self.log(msg, LogLevel.DEBUG, tags, type_)

    async def warning(self, msg: str, tags: List[str], type_: LogType = LogType.TEXT):
        await self.log(msg, LogLevel.WARNING, tags, type_)

    async def error(self, msg: str, tags: List[str], type_: LogType = LogType.TEXT):
        await self.log(msg, LogLevel.ERROR, tags, type_)

    @abstractmethod
    def sync_log(self, msg: str, level: LogLevel, tags: List[str], type_: LogType = LogType.TEXT):
        ...

    def sync_info(self, msg: str, tags: List[str], type_: LogType = LogType.TEXT):
        self.sync_log(msg, LogLevel.INFO, tags, type_)

    def sync_debug(self, msg: str, tags: List[str], type_: LogType = LogType.TEXT):
        self.sync_log(msg, LogLevel.DEBUG, tags, type_)

    def sync_warning(self, msg: str, tags: List[str], type_: LogType = LogType.TEXT):
        self.sync_log(msg, LogLevel.WARNING, tags, type_)

    def sync_error(self, msg: str, tags: List[str], type_: LogType = LogType.TEXT):
        self.sync_log(msg, LogLevel.ERROR, tags, type_)


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

FuncTask = Callable[[Any, Logger, VarArg(Any)], _T] | Callable[[
    Any, Logger, VarArg(Any)], Awaitable[_T]]


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
    # status uuid is none means this is a worker heart beat
    status: entities.TaskStatus | entities.WorkerStatus | None
    content: Any
    create_timestamp: float


TaskInput = Tuple[FuncTask, List[str]] | None | FuncTask


class FunTaskManager:
    @abstractmethod
    async def increase_workers(
            self,
            number: int | None = None,
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
            task_uuid: entities.TaskUUID | None = None,
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


class FunTaskManagerRPC:
    @abstractmethod
    async def increase_workers(
            self,
            number: int | None = None
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
            argument: entities.FuncArgument | None,
            task_uuid: entities.TaskUUID | None = None
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
    ) -> AsyncIterator[StatusReport]:
        """
        timeout is None: block
        timeout is not None and timeout: return None
        queue not empty: iterator
        """
        yield

    @abstractmethod
    async def get_task_queue_size(self, worker: entities.WorkerUUID) -> int:
        ...


class ManagerNodeControl:
    """
    control service for task_worker_manager, manage all task_worker_manager
    """

    @abstractmethod
    async def get_all_nodes(self) -> List[entities.TaskWorkerManagerNode]:
        ...


class LeaderSchedulerControl:
    """
    control service for leader scheduler, manage all scheduler nodes and leader election
    """

    @abstractmethod
    async def get_leader(self) -> entities.SchedulerNode | None:
        ...

    @abstractmethod
    async def elect_leader(self, uuid: entities.SchedulerNodeUUID) -> bool:
        ...

    @abstractmethod
    async def is_he_leader(self, uuid: entities.SchedulerNodeUUID) -> bool:
        ...

    @abstractmethod
    async def get_all_nodes(self) -> List[entities.SchedulerNode]:
        ...

    @abstractmethod
    async def get_cluster_id(self) -> entities.ClusterUUID:
        ...


class LeaderSchedulerRPC:
    @abstractmethod
    async def assign_task_to_node(
            self,
            node: entities.SchedulerNode,
            cron_task_uuid: entities.CronTaskUUID | None = None,
            task_uuid: entities.TaskUUID | None = None,
            start_time: datetime | None = None
    ):
        ...

    @abstractmethod
    async def get_node_task_list(self, node: entities.SchedulerNode) -> List[entities.CronTaskUUID]:
        ...

    @abstractmethod
    async def remove_task_from_node(
            self,
            node: entities.SchedulerNode,
            cron_task_uuid: entities.CronTaskUUID,
            start_time: datetime | None = None
    ):
        ...


class Cron:
    @abstractmethod
    async def run(self):
        ...

    @abstractmethod
    async def every_n_seconds(self, name: str, n: int, task: Callable, at: str | None = None, *args, **kwargs):
        ...

    @abstractmethod
    async def every_n_minutes(self, name: str, n: int, task: Callable, at: str | None = None, *args, **kwargs):
        ...

    @abstractmethod
    async def every_n_hours(self, name: str, n: int, task: Callable, at: str | None = None, *args, **kwargs):
        ...

    @abstractmethod
    async def every_n_days(self, name: str, n: int, task: Callable, at: str | None = None, *args, **kwargs):
        ...

    @abstractmethod
    async def every_n_weeks(self, name: str, n: int, task: Callable, at: str | None = None, *args, **kwargs):
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
        if ctx yield RollbackException, should roll back
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
    async def add_task(self, task: entities.Task, session=None):
        ...

    @abstractmethod
    async def change_task_status(self, task_uuid: entities.TaskUUID, status: entities.TaskStatus, session=None):
        ...

    @abstractmethod
    async def change_task_status_from_uuid(
            self,
            task_uuid: entities.TaskUUID,
            status: entities.TaskStatus,
            session=None
    ):
        ...

    @abstractmethod
    async def add_func(self, func: entities.Func, session=None) -> int:
        ...

    @abstractmethod
    async def add_worker(self, worker: entities.Worker, session=None):
        ...

    @abstractmethod
    async def match_workers_from_name(
            self,
            name: str,
            limit: int,
            cursor: int | None = None,
            session=None
    ) -> Tuple[List[entities.Worker], int]:
        ...

    @abstractmethod
    async def match_functions_from_name(
            self,
            name: str,
            limit: int,
            cursor: int | None = None,
            session=None
    ) -> Tuple[List[entities.Func], int]:
        ...

    @abstractmethod
    async def get_functions_from_cursor(
            self,
            limit: int,
            cursor: int | None = None,
            session=None
    ) -> Tuple[List[entities.Func], int]:
        ...

    @abstractmethod
    async def get_workers_from_cursor(
            self,
            limit: int,
            cursor: int | None = None,
            session=None
    ) -> Tuple[List[entities.Worker], int]:
        """
        get n worker entities from cursor, first query cursor can be None

        :param limit: number of worker
        :type limit: int
        :param cursor: cursor return from last query
        :type cursor: int
        :param session: session for query
        :type session: any
        :return: workers and cursor for query
        """
        ...

    @abstractmethod
    async def get_worker_from_uuid(self, worker_uuid: entities.WorkerUUID, session=None) -> entities.Task:
        ...

    @abstractmethod
    async def get_worker_from_name(self, name: str, session=None) -> entities.Worker:
        ...

    @abstractmethod
    async def change_worker_status(self, worker_uuid: entities.WorkerUUID, status: entities.WorkerStatus, session=None):
        ...

    @abstractmethod
    async def add_cron_task(self, task: entities.CronTask, session=None):
        ...

    @abstractmethod
    async def add_func_parameter_schema(
            self,
            func_parameter_schema: entities.ParameterSchema,
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
            tags: List[entities.Tag],
            session=None
    ) -> List[entities.Worker]:
        ...

    @abstractmethod
    async def drop_model_schema(self):
        ...

    @abstractmethod
    async def create_model_schema(self):
        ...

    @abstractmethod
    async def get_function_from_uuid(self, func_uuid: entities.FuncUUID, session=None) -> entities.Func:
        ...


class WorkerScheduler:
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
    async def scheduler_node_change(self, scheduler_nodes: List[entities.SchedulerNode]):
        ...

    @abstractmethod
    async def rebalance(self, rebalance_date: datetime):
        ...


class NoNodeException(Exception):
    ...


class RPCChannelSelector(Generic[_T]):
    @abstractmethod
    def channel_node_changes(self, nodes: List[Any]):
        ...

    @abstractmethod
    async def get_channel(self, key: bytes | None = None) -> _T:
        """
        choose channel for rpc
        :param key: choose channel depends on key
        :type key: bytearray
        :return: rpc channel
        """
        ...


class TimeoutException(Exception):
    ...


class DistributeLock:
    @asynccontextmanager
    @abstractmethod
    async def lock(self, name: str, timeout: float | None = None) -> AsyncIterator[None]:
        """
        lock a value, if timeout raise a TimeoutException, default no timeout
        :param name: global lock name
        :param timeout: timeout
        :return
        """
        yield

    @asynccontextmanager
    @abstractmethod
    async def try_lock(self, name: str) -> AsyncIterator[bool]:
        """
        try to get a lock, if lock occupied yield false
        :param name: global lock name
        :return:
        """
        yield True
