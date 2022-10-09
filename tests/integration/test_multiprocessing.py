import asyncio
import os
from typing import Dict

from funtask.core.funtask_types.task_worker_manager import Logger, TaskStatus
from funtask.core.task_worker_manager import FunTaskManager
from funtask.providers.loggers.std import StdLogger
from funtask.providers.queue.multiprocessing_queue import MultiprocessingQueue, MultiprocessingQueueFactory
from funtask.providers.worker_manager.multiprocessing_manager import MultiprocessingManager
import pytest

THIS_FILE_IMPORT_PATH = 'tests.integration.test_multiprocessing'


async def get_status(manager: FunTaskManager, status_map: Dict[str, TaskStatus]):
    while True:
        status = await manager.get_queued_status(.1)
        if status is not None and status.task_uuid:
            status_map[status.task_uuid] = status.status
        if status is None:
            break


@pytest.fixture
def manager() -> FunTaskManager:
    task_status_queue = MultiprocessingQueue()
    manager = FunTaskManager(
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
        workers_uuid = await manager.increase_workers(10)
        [await manager.stop_worker(worker_uuid) for worker_uuid in workers_uuid]

    async def test_worker_up_kill(self, manager: FunTaskManager):
        workers_uuid = await manager.increase_workers(10)
        [await manager.kill_worker(worker_uuid) for worker_uuid in workers_uuid]

    async def test_task_exec(self, manager: FunTaskManager):
        def task(_, __):
            with open('task_done_flag', 'w'):
                ...

        worker_uuid = await manager.increase_worker()
        await manager.dispatch_fun_task(worker_uuid, task)
        await asyncio.sleep(1)
        await manager.kill_worker(worker_uuid)
        assert 'task_done_flag' in os.listdir()
        os.remove('task_done_flag')

    async def test_async_task_exec(self, manager: FunTaskManager):
        async def async_task(status, logger, task_id: int):
            await asyncio.sleep(3)
            with open(f'task_{task_id}_done_flag', 'w'):
                ...

        worker_uuid = await manager.increase_worker()
        await manager.dispatch_fun_task(
            worker_uuid,
            (async_task, THIS_FILE_IMPORT_PATH),
            task_id=0
        )
        await manager.dispatch_fun_task(
            worker_uuid,
            (async_task, THIS_FILE_IMPORT_PATH),
            task_id=1
        )
        await asyncio.sleep(4)
        await manager.kill_worker(worker_uuid)
        assert os.path.exists('task_0_done_flag') and os.path.exists('task_1_done_flag')
        os.remove('task_0_done_flag')
        os.remove('task_1_done_flag')

    async def test_worker_state_change(self, manager: FunTaskManager):
        def set_status(status: int | None, logger: Logger):
            return (status or 1) + 1

        def write_status_to_file(status: int, logger: Logger):
            with open('status', 'w') as f:
                f.write(str(status))

        worker_uuid = await manager.increase_worker()
        await manager.generate_worker_state(worker_uuid, (set_status, THIS_FILE_IMPORT_PATH))
        await manager.generate_worker_state(worker_uuid, (set_status, THIS_FILE_IMPORT_PATH))
        await manager.dispatch_fun_task(worker_uuid, write_status_to_file)
        await asyncio.sleep(.1)
        await manager.kill_worker(worker_uuid)
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

        worker_uuid = await manager.increase_worker()
        await manager.generate_worker_state(worker_uuid, (set_none_with_dependency, THIS_FILE_IMPORT_PATH))
        await manager.dispatch_fun_task(worker_uuid, still_can_use_sys)
        await asyncio.sleep(.1)
        await manager.kill_worker(worker_uuid)
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

        worker_uuid = await manager.increase_worker()
        await manager.dispatch_fun_task(worker_uuid, (can_use_sys_with_temp_dependency, THIS_FILE_IMPORT_PATH))
        await manager.dispatch_fun_task(worker_uuid, cannot_use_sys_with_no_temp_dependency)
        await asyncio.sleep(.1)
        await manager.kill_worker(worker_uuid)
        exist_no_sys_flag = os.path.exists('with_no_dependency')
        exist_sys_flag = os.path.exists('with_dependency')
        exist_sys_flag and os.remove('with_dependency')
        exist_no_sys_flag and os.remove('with_no_dependency')
        assert not exist_no_sys_flag
        assert exist_sys_flag

    async def test_task_status(self, manager: FunTaskManager):
        def sleep(_, __):
            import time
            time.sleep(1)

        def err(_, __):
            raise Exception('just err')

        worker_uuid = await manager.increase_worker()
        task1_uuid = await manager.dispatch_fun_task(worker_uuid, sleep)
        task2_uuid = await manager.dispatch_fun_task(worker_uuid, sleep)
        task_err_uuid = await manager.dispatch_fun_task(worker_uuid, err)
        await asyncio.sleep(.5)
        task_status_map: Dict[str, TaskStatus] = {}
        await get_status(manager, task_status_map)
        assert task1_uuid in task_status_map
        assert task2_uuid in task_status_map
        assert task_status_map[task1_uuid] == TaskStatus.RUNNING
        assert task_status_map[task2_uuid] == TaskStatus.QUEUED
        await asyncio.sleep(1)
        await get_status(manager, task_status_map)
        assert task_status_map[task1_uuid] == TaskStatus.SUCCESS
        assert task_status_map[task2_uuid] == TaskStatus.RUNNING
        await asyncio.sleep(1)
        await get_status(manager, task_status_map)
        await manager.kill_worker(worker_uuid)
        assert task_status_map[task_err_uuid] == TaskStatus.ERROR
        assert task_status_map[task2_uuid] == TaskStatus.SUCCESS
