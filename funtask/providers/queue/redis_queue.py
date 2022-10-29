import asyncio
import time
from typing import TypeVar

import dill

from funtask import Queue
from funtask.core.interface_and_types import BreakRef
from funtask.providers.queue.common import NeverBreak

try:
    import redis.asyncio as redis
except ImportError:
    raise ImportError(
        "to use redis kv-db, please make sure install redis~=4.3.4 or install with funtask[redis] feature"
    )

_T = TypeVar('_T')


class RedisQueue(Queue):
    def __init__(self, queue_id: str, *args, **kwargs):
        self.r = redis.Redis(*args, **kwargs)
        self.qid = queue_id

    async def get_front(self) -> _T | None:
        raise NotImplemented()

    @staticmethod
    def from_url(queue_id: str, url: str) -> 'RedisQueue':
        kv_redis = RedisQueue(queue_id)
        kv_redis.r = redis.Redis.from_url(url)
        return kv_redis

    async def put(self, obj: _T):
        await self.r.sadd(dill.dumps(obj))

    async def get(self, timeout: None | float = None) -> _T:
        return await self.watch_and_get(NeverBreak(), timeout)

    async def watch_and_get(self, break_ref: BreakRef, timeout: None | float = None) -> _T:
        start_time = time.time()
        while True:
            if timeout and time.time() - start_time >= timeout:
                return None
            content = self.r.spop(self.qid)
            if break_ref.if_break_now():
                return None
            if content is None:
                await asyncio.sleep(.01)
            else:
                return dill.loads(content)

    async def qsize(self) -> int:
        return await self.r.scard(self.qid)

    async def empty(self) -> bool:
        return not self.qsize()
