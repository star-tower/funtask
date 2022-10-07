from dataclasses import dataclass
from uuid import uuid4 as uuid_generator
from typing import List, TypeVar, Tuple, Generic, Any

import dill

from funtask.core.funtask_types import FuncTask, \
    TaskStatus, \
    WorkerManager, Queue, TaskControl, TransTask, TransTaskMeta, WorkerStatus

_T = TypeVar('_T')


@dataclass
class Task(Generic[_T]):
    uuid: str
    worker_uuid: str
    _task_manager: 'FunTaskManager'

    async def stop(
            self
    ):
        await self._task_manager.stop_task(self.worker_uuid, self.uuid)


TaskInput = Tuple[FuncTask, List[str]] | None | FuncTask


def _split_task_and_dependencies(
        state_generator: TaskInput
) -> Tuple[FuncTask, List[str]]:
    """
    change StateGeneratorWithDependencies to (StateGenerator, [dependencies_str...])
    :param state_generator: StateGeneratorWithDependencies
    :return: (StateGenerator, [dependencies_str...])
    """
    if isinstance(state_generator, Tuple):
        task, dependencies = state_generator
        if isinstance(dependencies, str):
            dependencies = [dependencies]
    elif state_generator is None:
        task, dependencies = lambda _: None, []
    else:
        task, dependencies = state_generator, []
    return task, dependencies


def _exec_none(*args, **kwargs):
    return None


def _warp_to_trans_task(
        uuid: str,
        task: TaskInput,
        result_as_state: bool
) -> TransTask:
    """
    warp state_generator to callable with is_state_regenerator and dependencies props
    """
    task, dependencies = _split_task_and_dependencies(task)

    if task is None:
        none_is_executable_wrapper = _exec_none
    else:
        none_is_executable_wrapper = task

    return TransTask(
        uuid=uuid,
        task=none_is_executable_wrapper,
        dependencies=dependencies,
        result_as_state=result_as_state
    )


@dataclass
class StatusReport:
    worker_uuid: str
    task_uuid: str | None
    status: TaskStatus | WorkerStatus
    content: Any


@dataclass
class Worker:
    uuid: str
    _task_manager: 'FunTaskManager'

    async def dispatch_fun_task(
            self,
            func_task: 'FuncTask',
            *arguments,
            **kwargs
    ) -> 'Task[_T]':
        return await self._task_manager.dispatch_fun_task(self.uuid, func_task, *arguments, **kwargs)

    async def regenerate_state(
            self,
            state_generator: 'FuncTask'
    ):
        return await self._task_manager.generate_worker_state(self.uuid, state_generator)

    async def stop(
            self
    ):
        await self._task_manager.stop_worker(self.uuid)

    async def kill(self):
        await self._task_manager.kill_worker(self.uuid)


class FunTaskManager:
    def __init__(
            self,
            *,
            worker_manager: WorkerManager,
            # worker_uuid, task_uuid, status, content
            task_status_queue: Queue[Tuple[str, str | None, TaskStatus | WorkerStatus, Any]]
    ):
        self.worker_manager = worker_manager
        self.task_status_queue = task_status_queue

    async def increase_workers(
            self,
            number: int = None,
            *args,
            **kwargs
    ) -> List[Worker]:
        workers = []
        for i in range(number - 1):
            workers.append(await self.increase_worker(*args, **kwargs))
        return workers

    async def increase_worker(
            self,
            *args,
            **kwargs
    ) -> Worker:
        uuid = await self.worker_manager.increase_worker(
            *args,
            **kwargs
        )
        worker = Worker(
            uuid,
            self
        )
        return worker

    async def dispatch_fun_task(
            self,
            worker_uuid: str,
            func_task: TaskInput,
            change_status=False,
            timeout=None,
            *arguments,
            **kwargs
    ) -> Task[_T]:
        assert func_task, Exception(f"func_task can't be {func_task}")
        task_queue = await self.worker_manager.get_task_queue(worker_uuid)
        task_uuid = str(uuid_generator())
        await task_queue.put(
            (
                dill.dumps(_warp_to_trans_task(task_uuid, func_task, change_status)),
                TransTaskMeta(arguments, kwargs, timeout)
            )
        )
        await self.task_status_queue.put((worker_uuid, task_uuid, TaskStatus.QUEUED, None))
        task = Task(
            task_uuid,
            worker_uuid,
            self
        )
        return task

    async def generate_worker_state(
            self,
            worker_uuid: str,
            state_generator: TaskInput,
            timeout=None,
            *arguments
    ) -> Task[_T]:
        return await self.dispatch_fun_task(
            worker_uuid,
            state_generator,
            change_status=True,
            timeout=timeout,
            *arguments
        )

    async def stop_task(
            self,
            worker_uuid: str,
            task_uuid: str
    ):
        worker_control_queue = await self.worker_manager.get_control_queue(worker_uuid)
        await worker_control_queue.put((task_uuid, TaskControl.KILL))

    async def stop_worker(
            self,
            worker_uuid: str
    ):
        worker_control_queue = await self.worker_manager.get_control_queue(worker_uuid)
        await self.worker_manager.stop_worker(worker_uuid)
        await worker_control_queue.put((worker_uuid, TaskControl.KILL))

    async def kill_worker(
            self,
            worker_uuid: str
    ):
        await self.worker_manager.kill_worker(worker_uuid)

    async def get_queued_status(
            self,
            timeout: None | float = None
    ) -> StatusReport | None:
        res = await self.task_status_queue.get(timeout)
        if res is None:
            return None
        worker_uuid, task_uuid, status, content = res
        return StatusReport(
            worker_uuid,
            task_uuid,
            status,
            content
        )
