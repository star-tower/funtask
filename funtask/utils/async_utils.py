import asyncio
from asyncio import Future
from typing import Coroutine


def run_async_sync(coroutine: Future | Coroutine):
    loop = asyncio.get_event_loop()
    if loop.is_running():
        return loop.create_task(coroutine)
    return loop.run_until_complete(coroutine)
