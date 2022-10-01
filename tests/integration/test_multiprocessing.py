import asyncio
import os

from funtask.fun_task_manager import FunTaskManager
from funtask.loggers.std import StdLogger
from funtask.queue.multiprocessing_queue import MultiprocessingQueue
from funtask.worker_manager.multiprocessing_manager import MultiprocessingManager
import pytest


@pytest.mark.timeout(5)
@pytest.mark.asyncio
class TestMultiprocessing:
    async def test_worker_up_stop(self):
        manager = FunTaskManager(
            namespace="test",
            worker_manager=MultiprocessingManager(StdLogger()),
            task_queue_factory=lambda _: MultiprocessingQueue(),
            task_status_queue_factory=lambda _: MultiprocessingQueue(),
            control_queue_factory=lambda _: MultiprocessingQueue(),
        )
        workers = await manager.increase_workers(10)
        [await worker.stop() for worker in workers]

    async def test_worker_up_kill(self):
        manager = FunTaskManager(
            namespace="test",
            worker_manager=MultiprocessingManager(StdLogger()),
            task_queue_factory=lambda _: MultiprocessingQueue(),
            task_status_queue_factory=lambda _: MultiprocessingQueue(),
            control_queue_factory=lambda _: MultiprocessingQueue(),
        )
        workers = await manager.increase_workers(10)
        [await worker.kill() for worker in workers]

    async def test_task_exec(self):
        def task(status, logger):
            with open('task_done_flag', 'w'):
                ...

        manager = FunTaskManager(
            namespace="test",
            worker_manager=MultiprocessingManager(StdLogger()),
            task_queue_factory=lambda _: MultiprocessingQueue(),
            task_status_queue_factory=lambda _: MultiprocessingQueue(),
            control_queue_factory=lambda _: MultiprocessingQueue(),
        )
        worker = await manager.increase_worker()
        await worker.dispatch_fun_task(task)
        await asyncio.sleep(1)
        await worker.kill()
        assert 'task_done_flag' in os.listdir()
        os.remove('task_done_flag')
