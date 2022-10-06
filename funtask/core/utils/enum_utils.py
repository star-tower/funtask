from enum import Enum
from typing import List


class AutoName(Enum):
    @staticmethod
    def _generate_next_value_(name: str, start: int, count: int, last_values: List) -> str:
        return name
