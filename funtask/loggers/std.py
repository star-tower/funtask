from typing import List

from funtask.core.funtask_types import Logger, LogLevel


class StdLogger(Logger):

    async def log(self, msg: str, level: LogLevel = LogLevel.INFO, tags: List[str] = None):
        tags = tags or ["default"]
        print(f"{level.value}-{tags}: {msg}")
