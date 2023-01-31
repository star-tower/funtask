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
class BatchQueryReq:
    limit: int
    cursor: int | None = Field(nullable=True)


@dataclass
class WorkersWithCursor:
    workers: List[entities.Worker]
    cursor: int | None = Field(nullable=True)


@dataclass
class NewFuncInstanceReq:
    func_base64: str
    worker_name: str | None = Field(nullable=True)
    worker_tags: List[entities.Tag] | None = Field(nullable=True)
