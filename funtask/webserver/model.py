from datetime import datetime

from pydantic import Field

from funtask.core import entities
from typing import List

from pydantic.dataclasses import dataclass


@dataclass
class IncreaseWorkersReq:
    number: int
    tags: List[entities.Tag]
    name: str | None = Field(nullable=True)


@dataclass
class WorkersWithCursor:
    workers: List[entities.Worker]
    cursor: int


@dataclass
class NewTaskReq:
    timeout: int
    dependencies: List[str]
    change_state: bool
    description: str
    func_uuid: entities.FuncUUID
    name: str | None = Field(nullable=True)
    worker_uuids: List[entities.WorkerUUID] | None = Field(nullable=True)
    worker_tags: List[entities.Tag] | None = Field(nullable=True)


@dataclass
class NewCronTaskReq:
    function_uuid: entities.FuncUUID
    timepoints: List[entities.TimePoint]
    worker_uuid: entities.WorkerUUID


@dataclass
class NewFuncReq:
    description: str
    dependencies: List[str]
    name: str | None = Field(nullable=True)
    func_base64: str | None = Field(nullable=True)


@dataclass
class FuncWithCursor:
    funcs: List[entities.Func]
    cursor: int


@dataclass
class TaskDescribe:
    start_time: datetime | None
    create_time: datetime
    task_uuid: entities.TaskUUID
    status: entities.TaskStatus
    name: str | None = Field(nullable=True)
    stop_time: datetime | None = Field(nullable=True)
    parent_task_uuid: entities.CronTaskUUID | entities.TaskUUID | None = Field(nullable=True)
    parent_task_type: entities.TaskType | None = Field(nullable=True)


@dataclass
class TaskDescribesWithCursor:
    task_describes: List[TaskDescribe]
    cursor: int
