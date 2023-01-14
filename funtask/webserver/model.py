from funtask.core import entities
from typing import List, Dict

from pydantic.dataclasses import dataclass


@dataclass
class IncreaseWorkerReq:
    name: str | None
    number: int
    tags: List[Dict[str, str] | str]


@dataclass
class BatchQueryReq:
    cursor: int | None
    limit: int


@dataclass
class WorkersWithCursor:
    workers: List[entities.Worker]
    cursor: int
