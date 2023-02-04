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
class NewFuncInstanceReq:
    timeout: int
    dependencies: List[str]
    change_state: bool
    description: str
    func_description: str
    func_base64: str | None = Field(nullable=True)
    func_uuid: entities.FuncUUID | None = Field(nullable=True)
    name: str | None = Field(nullable=True)
    worker_uuids: List[entities.WorkerUUID] | None = Field(nullable=True)
    worker_tags: List[entities.Tag] | None = Field(nullable=True)
