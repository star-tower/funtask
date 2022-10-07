from typing import AsyncIterator

from funtask.core.funtask_types import KVDB

try:
    import redis.asyncio as redis
except ImportError:
    raise ImportError(
        "to use redis kv-db, please make sure install redis~=4.3.4 or install with funtask[redis] feature"
    )


class KVRedis(KVDB):
    def __init__(self, *args, **kwargs):
        self.r = redis.Redis(*args, **kwargs)

    @staticmethod
    def from_url(url: str) -> 'KVRedis':
        kv_redis = KVRedis()
        kv_redis.r = redis.Redis.from_url(url)
        return kv_redis

    async def delete(self, key: str):
        await self.r.delete(key)

    async def push(self, key: str, *value: bytes):
        await self.r.sadd(key, *value)

    async def random_pop(self, key: str) -> bytes:
        res = await self.r.spop(key)
        if isinstance(res, bytes):
            return res

    async def remove(self, key: str, *items: bytes):
        await self.r.srem(key, *items)

    async def set(self, key: str, value: str | bytes) -> bool | None:
        return await self.r.set(key, value)

    async def get(self, key: str) -> str | bytes:
        return await self.r.get(key)

    async def foreach(self, key: str) -> AsyncIterator[str | bytes]:
        for each in await self.r.sscan_iter(key):
            yield each
