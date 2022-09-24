from multiprocessing import Queue as MltQueue

from funtask.funtask_types import Queue, _T


class MultiprocessingQueue(Queue):
    def __init__(self):
        self.q = MltQueue()

    async def put(self, obj: _T):
        self.q.put(obj)

    async def get(self) -> _T:
        return self.q.get()

    async def qsize(self) -> int:
        return self.q.qsize()

    async def empty(self) -> bool:
        return self.q.empty()
