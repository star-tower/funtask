from funtask.core import entities
from typing import List

from pydantic.dataclasses import dataclass


@dataclass
class IncreaseWorkerReq:
    name: str | None
    number: int
    tags: List[entities.Tag]


@dataclass
class BatchQueryReq:
    cursor: int | None
    limit: int


@dataclass
class WorkersWithCursor:
    workers: List[entities.Worker]
    cursor: int
