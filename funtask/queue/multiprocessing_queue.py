import asyncio
from multiprocessing import Queue as MltQueue
from queue import Empty

from funtask.funtask_types import Queue, _T, BreakRef


class MultiprocessingQueue(Queue):
    def __init__(self):
        self.q = MltQueue()
        self.type = 'multiprocessing'
        self.config = {}

    async def watch_and_get(self, break_ref: BreakRef) -> _T:
        while True:
            try:
                return self.q.get(timeout=0.001, block=True)
            except Empty:
                if break_ref.if_break_now():
                    break
                await asyncio.sleep(0.01)

    async def put(self, obj: _T):
        self.q.put(obj)

    async def get(self) -> _T:
        while True:
            try:
                return self.q.get(timeout=0.001, block=True)
            except Empty:
                await asyncio.sleep(0.01)

    async def qsize(self) -> int:
        return self.q.qsize()

    async def empty(self) -> bool:
        return self.q.empty()
