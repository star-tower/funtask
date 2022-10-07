import asyncio
import os

from funtask.core.funtask_types import Logger
from funtask.core.task_worker_manager import FunTaskManager
from funtask.loggers.std import StdLogger
from funtask.queue.multiprocessing_queue import MultiprocessingQueue, MultiprocessingQueueFactory
from funtask.worker_manager.multiprocessing_manager import MultiprocessingManager
import pytest

THIS_FILE_IMPORT_PATH = 'tests.core.integration.test_multiprocessing'


@pytest.fixture
def manager() -> FunTaskManager:
    task_status_queue = MultiprocessingQueue()
    manager = FunTaskManager(
        namespace="test",
        worker_manager=MultiprocessingManager(
            StdLogger(),
            task_queue_factory=MultiprocessingQueueFactory().factory,
            control_queue_factory=MultiprocessingQueueFactory().factory,
            task_status_queue=task_status_queue
        ),
        task_status_queue=task_status_queue

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
        def task(_, __):
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
            (async_task, THIS_FILE_IMPORT_PATH),
            task_id=0
        )
        await worker.dispatch_fun_task(
            (async_task, THIS_FILE_IMPORT_PATH),
            task_id=1
        )
        await asyncio.sleep(4)
        await worker.kill()
        assert os.path.exists('task_0_done_flag') and os.path.exists('task_1_done_flag')
        os.remove('task_0_done_flag')
        os.remove('task_1_done_flag')

    async def test_worker_state_change(self, manager: FunTaskManager):
        def set_status(status: int | None, logger: Logger):
            return (status or 1) + 1

        def write_status_to_file(status: int, logger: Logger):
            with open('status', 'w') as f:
                f.write(str(status))

        worker = await manager.increase_worker()
        await worker.regenerate_state((set_status, THIS_FILE_IMPORT_PATH))
        await worker.regenerate_state((set_status, THIS_FILE_IMPORT_PATH))
        await worker.dispatch_fun_task(write_status_to_file)
        await asyncio.sleep(.1)
        await worker.kill()
        with open('status') as f:
            assert f.read() == '3'
        os.remove('status')

    async def test_worker_task_with_status_regenerate_dependency(self, manager: FunTaskManager):
        def set_none_with_dependency(_, __):
            _ = pytest.mark
            return None

        def still_can_use_sys(_, __):
            _ = pytest.mark
            with open('task_dependency_done_flag', 'w'):
                ...

        worker = await manager.increase_worker()
        await worker.regenerate_state((set_none_with_dependency, THIS_FILE_IMPORT_PATH))
        await worker.dispatch_fun_task(still_can_use_sys)
        await asyncio.sleep(.1)
        await worker.kill()
        assert os.path.exists('task_dependency_done_flag')
        os.remove('task_dependency_done_flag')

    async def test_worker_task_with_temp_dependency(self, manager: FunTaskManager):
        def can_use_sys_with_temp_dependency(_, __):
            _ = pytest.mark
            with open('with_dependency', 'w'):
                ...

        def cannot_use_sys_with_no_temp_dependency(_, __):
            _ = pytest.mark
            with open('with_no_dependency', 'w'):
                ...

        worker = await manager.increase_worker()
        await worker.dispatch_fun_task((can_use_sys_with_temp_dependency, THIS_FILE_IMPORT_PATH))
        await worker.dispatch_fun_task(cannot_use_sys_with_no_temp_dependency)
        await asyncio.sleep(.1)
        await worker.kill()
        exist_no_sys_flag = os.path.exists('with_no_dependency')
        exist_sys_flag = os.path.exists('with_dependency')
        exist_sys_flag and os.remove('with_dependency')
        exist_no_sys_flag and os.remove('with_no_dependency')
        assert not exist_no_sys_flag
        assert exist_sys_flag
