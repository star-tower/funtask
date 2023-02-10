import asyncio
import base64
import contextlib
import time
from dataclasses import asdict

import aiohttp
from collections import defaultdict
from queue import Queue as Q
from queue import Empty
from typing import Dict, Generic, Tuple, Any, AsyncIterator
import dill
import uvicorn
from fastapi import FastAPI, HTTPException
from fire import Fire
from loguru import logger
from pydantic.dataclasses import dataclass

from funtask.core.interface_and_types import Queue, _T, BreakRef
from funtask.providers.queue.common import NeverBreak
from funtask.webserver.utils import SelfPointer, self_wrapper

app = FastAPI()
queue_service_pointer = SelfPointer()


class _QueueContentEnDe:
    @staticmethod
    def encode(obj: Any):
        return base64.b64encode(dill.dumps(obj)).decode('utf8')

    @staticmethod
    def decode(content: str):
        return dill.loads(base64.b64decode(content))


@contextlib.asynccontextmanager
async def async_http(url: str, method: str = 'get', *args, **kwargs) -> AsyncIterator[aiohttp.ClientResponse]:
    assert method.lower() in {'get', 'post'}
    async with aiohttp.ClientSession() as session:
        func = session.get if method.lower() == 'get' else session.post
        async with func(url, *args, **kwargs) as resp:
            resp: aiohttp.ClientResponse
            yield resp


@dataclass
class PutItem:
    content_b64: str


class QueueControl:
    def __init__(self):
        self.q_pool: Dict[str, Tuple[Any, Q]] = defaultdict(lambda: (None, Q()))

    @app.get('/front/{name}')
    @self_wrapper(queue_service_pointer)
    async def peek_front_cache(self, name: str):
        front, _ = self.q_pool[name]
        return _QueueContentEnDe.encode(front)

    @app.post('/item/{name}')
    @self_wrapper(queue_service_pointer)
    async def put(self, name: str, item: PutItem):
        _, q = self.q_pool[name]
        front = _QueueContentEnDe.decode(item.content_b64)
        q.put_nowait(front)
        self.q_pool[name] = (front, q)

    @app.get('/item/{name}')
    @self_wrapper(queue_service_pointer)
    async def get(self, name: str):
        _, q = self.q_pool[name]
        try:
            return _QueueContentEnDe.encode(q.get_nowait())
        except Empty:
            raise HTTPException(status_code=201, detail='queue empty')

    @app.get('/qsize/{name}')
    @self_wrapper(queue_service_pointer)
    async def size(self, name: str):
        _, q = self.q_pool[name]
        return q.qsize()


class MultiprocessingQueueFactory:
    def __init__(
            self,
            host: str,
            port: int
    ):
        self.host = host
        self.port = port

    def factory(self, name: str) -> 'MultiprocessingQueue':
        return MultiprocessingQueue(name, host=self.host, port=self.port)

    def __call__(self, name: str):
        return self.factory(name)


class MultiprocessingQueue(Queue, Generic[_T]):
    def __init__(self, name: str, host: str, port: int):
        self.name = name
        self.uri = f'http://{host}:{port}'
        self.type = 'multiprocessing'
        self.config = {}

    async def peek_front_cache(self) -> _T | None:
        async with async_http(f'{self.uri}/front/{self.name}') as resp:
            resp: aiohttp.ClientResponse
            return _QueueContentEnDe.decode(await resp.text('utf8'))

    async def watch_and_get(self, break_ref: BreakRef, timeout: None | float = None) -> _T | None:
        start_time = time.time()
        while True:
            if timeout and time.time() - start_time >= timeout:
                return None
            async with async_http(f'{self.uri}/item/{self.name}') as resp:
                resp: aiohttp.ClientResponse
                # 201 is empty queue
                if resp.status == 201:
                    if break_ref.if_break_now():
                        break
                    await asyncio.sleep(.1)
                    continue
                return _QueueContentEnDe.decode(await resp.text('utf8'))

    async def put(self, obj: _T):
        async with async_http(
                f'{self.uri}/item/{self.name}',
                'post',
                json=asdict(PutItem(
                    content_b64=_QueueContentEnDe.encode(obj)
                ))
        ):
            return

    async def get(self, timeout: None | float = None) -> _T | None:
        return await self.watch_and_get(NeverBreak(), timeout)

    async def qsize(self) -> int:
        async with async_http(f'{self.uri}/qsize/{self.name}') as resp:
            resp: aiohttp.ClientResponse
            return await resp.json(encoding='utf8')

    async def empty(self) -> bool:
        return await self.qsize() == 0


def run_broker(
        host: str = 'localhost',
        port: int = 3333
):
    queue_control = QueueControl()
    queue_service_pointer.set_self(queue_control)
    logger.opt(colors=True).info(
        "starting remote multiprocessing broker <cyan>{host}:{port}</cyan>",
        host=host,
        port=port
    )
    uvicorn.run(app, host=host, port=port)


if __name__ == '__main__':
    Fire(run_broker)
