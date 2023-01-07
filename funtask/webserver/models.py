from typing import List

from pydantic.dataclasses import dataclass


@dataclass
class IncreaseWorkerReq:
    name: str
    tags: List[str]
