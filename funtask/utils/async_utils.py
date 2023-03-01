import asyncio
import time
from asyncio import Future
from threading import Thread
from typing import Coroutine


def _start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    if not loop.is_running():
        loop.run_forever()


def background_loop_thread(loop: asyncio.AbstractEventLoop) -> Thread:
    return Thread(target=_start_background_loop, args=(loop,), daemon=True)


def await_sync(task):
    while not task.done():
        time.sleep(.001)
    return task.result()


class BackgroundAsyncRunner:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self._thread_started = False
        self.thread: Thread = background_loop_thread(self.loop)

    def start(self):
        if self._thread_started:
            raise RuntimeError('background thread already started')
        self.thread = self.thread.start()

    def run_async_sync(self, coroutine: Future | Coroutine):
        loop = asyncio.get_event_loop()
        # current loop is running, must use loop in thread
        if loop.is_running():
            if not self.thread.isAlive():
                self.thread.start()
            background_loop_thread(self.loop).start()
            task = asyncio.run_coroutine_threadsafe(coroutine, self.loop)
            return await_sync(task)
        return loop.run_until_complete(coroutine)
