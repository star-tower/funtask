from abc import abstractmethod
from pydantic.dataclasses import dataclass
from typing import NewType, Callable, List

# same value with task uuid in worker_manager but in different component
SchedulerTaskUUID = NewType("SchedulerTaskUUID", str)
FuncGroupUUID = NewType("FuncGroupUUID", str)
SchedulerTaskGroupUUID = NewType("SchedulerTaskGroupUUID", str)
CronTaskUUID = NewType("CronTaskUUID", str)
SchedulerNodeUUID = NewType("SchedulerNodeUUID", str)
ClusterUUID = NewType("ClusterUUID", str)
FuncUUID = NewType("FuncUUID", str)
FuncSchemaUUID = NewType("FuncSchema", str)


class SchedulerNode:
    uuid: SchedulerNodeUUID
    ip: str
    port: int


class LeaderControl:
    @abstractmethod
    async def get_leader(self) -> SchedulerNode:
        ...

    @abstractmethod
    async def elect_leader(self) -> bool:
        ...

    @abstractmethod
    async def is_he_leader(self, uuid: SchedulerNodeUUID) -> bool:
        ...

    @abstractmethod
    async def get_all_nodes(self) -> List[SchedulerNode]:
        ...

    @abstractmethod
    async def get_cluster_id(self) -> ClusterUUID:
        ...


class LeaderSchedulerRPC:
    @abstractmethod
    async def assign_task_to_node(self, node: SchedulerNode, cron_task_uuid: CronTaskUUID):
        ...

    @abstractmethod
    async def get_node_task_list(self, node: SchedulerNode) -> List[CronTaskUUID]:
        ...

    @abstractmethod
    async def remove_task_from_node(self, node: SchedulerNode, cron_task_uuid: CronTaskUUID):
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
    async def cancel(self, name: str):
        ...

    @abstractmethod
    async def get_all(self) -> List[str]:
        ...


@dataclass
class SchedulerTask:
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


class Scheduler:
    @abstractmethod
    async def trigger_func(self, func: Func, argument: FuncArgument) -> SchedulerTaskUUID:
        ...

    @abstractmethod
    async def trigger_func_group(
            self,
            func_group: FuncGroup,
            argument_group: FuncArgumentGroup
    ) -> SchedulerTaskGroupUUID:
        ...

    @abstractmethod
    async def add_func_group(self, func_group: FuncGroup) -> FuncGroupUUID:
        ...

    @abstractmethod
    async def trigger_repeated_func(
            self,
            time_points: TimePoints,
            func: Func,
            argument: FuncArgument
    ) -> CronTaskUUID:
        ...

    @abstractmethod
    async def trigger_repeated_func_group(
            self,
            time_points: TimePoints,
            func_group: FuncGroup,
            argument_group: FuncArgumentGroup
    ) -> CronTaskUUID:
        ...

    @abstractmethod
    async def add_func(self, func: Func) -> FuncUUID:
        ...

    @abstractmethod
    async def add_parameter_schema(self, parameter_schema: FuncParameterSchema) -> FuncSchemaUUID:
        ...


class LeaderScheduler:
    ...
