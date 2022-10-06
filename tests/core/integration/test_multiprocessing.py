import asyncio
import os

from funtask.core.task_worker_manager import FunTaskManager
from funtask.loggers.std import StdLogger
from funtask.queue.multiprocessing_queue import MultiprocessingQueue
from funtask.worker_manager.multiprocessing_manager import MultiprocessingManager
import pytest


@pytest.fixture
def manager() -> FunTaskManager:
    manager = FunTaskManager(
        namespace="test",
        worker_manager=MultiprocessingManager(StdLogger()),
        task_queue_factory=lambda _: MultiprocessingQueue(),
        task_status_queue_factory=lambda _: MultiprocessingQueue(),
        control_queue_factory=lambda _: MultiprocessingQueue(),
    )
    yield manager


@pytest.mark.timeout(5)
@pytest.mark.asyncio
class TestMultiprocessing:
    async def test_worker_up_stop(self, manager: FunTaskManager):
        workers = await manager.increase_workers(10)
        [await worker.stop() for worker in workers]

    async def test_worker_up_kill(self, manager: FunTaskManager):
        workers = await manager.increase_workers(10)
        [await worker.kill() for worker in workers]

    async def test_task_exec(self, manager: FunTaskManager):
        def task(status, logger):
            with open('task_done_flag', 'w'):
                ...

        worker = await manager.increase_worker()
        await worker.dispatch_fun_task(task)
        await asyncio.sleep(1)
        await worker.kill()
        assert 'task_done_flag' in os.listdir()
        os.remove('task_done_flag')

    async def test_async_task_exec(self, manager: FunTaskManager):
        async def async_task(status, logger, task_id: int):
            await asyncio.sleep(3)
            with open(f'task_{task_id}_done_flag', 'w'):
                ...

        worker = await manager.increase_worker()
        await worker.dispatch_fun_task(
            (async_task, 'tests.core.integration.test_multiprocessing'),
            task_id=0
        )
        await worker.dispatch_fun_task(
            (async_task, 'tests.core.integration.test_multiprocessing'),
            task_id=1
        )
        await asyncio.sleep(4)
        await worker.kill()
        assert os.path.exists('task_0_done_flag') and os.path.exists('task_1_done_flag')
        os.remove('task_0_done_flag')
        os.remove('task_1_done_flag')
