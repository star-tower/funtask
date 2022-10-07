import asyncio

import dill

from funtask import Queue
from funtask.core.funtask_types import BreakRef, _T
from funtask.queue.common import NeverBreak

try:
    import redis.asyncio as redis
except ImportError:
    raise ImportError(
        "to use redis kv-db, please make sure install redis~=4.3.4 or install with funtask[redis] feature"
    )


class RedisQueue(Queue):
    def __init__(self, queue_id: str, *args, **kwargs):
        self.r = redis.Redis(*args, **kwargs)
        self.qid = queue_id

    @staticmethod
    def from_url(queue_id: str, url: str) -> 'RedisQueue':
        kv_redis = RedisQueue(queue_id)
        kv_redis.r = redis.Redis.from_url(url)
        return kv_redis

    async def put(self, obj: _T):
        await self.r.sadd(dill.dumps(obj))

    async def get(self) -> _T:
        return await self.watch_and_get(NeverBreak())

    async def watch_and_get(self, break_ref: BreakRef) -> _T:
        while True:
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
