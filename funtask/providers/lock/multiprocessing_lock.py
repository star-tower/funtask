from typing import AsyncIterator

from funtask.core import interface_and_types as interface


class MultiprocessingLock(interface.DistributeLock):
    async def lock(self, name: str, timeout: float | None = None) -> AsyncIterator[None]:
        yield

    async def try_lock(self, name: str) -> AsyncIterator[bool]:
        yield True
