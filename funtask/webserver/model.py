from pydantic import Field

from funtask.core import entities
from typing import List

from pydantic.dataclasses import dataclass


@dataclass
class IncreaseWorkerReq:
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
    cursor: int
