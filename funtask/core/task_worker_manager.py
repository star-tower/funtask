from uuid import uuid4 as uuid_generator
from typing import List, TypeVar, Tuple, cast

from funtask.core.funtask_types.task_worker_manager import FuncTask, TaskStatus, StatusQueueMessage, \
    WorkerManager, Queue, TaskControl, Task, TaskMeta, ControlQueueMessage, TaskQueueMessage, TaskInput, \
    StatusReport, FunTaskManager as FunTaskManagerAbs, WorkerUUID, TaskUUID

_T = TypeVar('_T')


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
        uuid: TaskUUID,
        task: TaskInput,
        result_as_state: bool
) -> Task:
    """
    warp state_generator to callable with is_state_regenerator and dependencies props
    """
    task, dependencies = _split_task_and_dependencies(task)

    if task is None:
        none_is_executable_wrapper = _exec_none
    else:
        none_is_executable_wrapper = task

    return Task(
        uuid=uuid,
        task=none_is_executable_wrapper,
        dependencies=dependencies,
        result_as_state=result_as_state
    )


class FunTaskManager(FunTaskManagerAbs):
    def __init__(
            self,
            *,
            worker_manager: WorkerManager,
            # worker_uuid, task_uuid, status, content
            task_status_queue: Queue[StatusQueueMessage]
    ):
        self.worker_manager = worker_manager
        self.task_status_queue = task_status_queue

    async def increase_workers(
            self,
            number: int = None,
            *args,
            **kwargs
    ) -> List[WorkerUUID]:
        workers_uuid = []
        for i in range(number - 1):
            workers_uuid.append(await self.increase_worker(*args, **kwargs))
        return workers_uuid

    async def increase_worker(
            self,
            *args,
            **kwargs
    ) -> WorkerUUID:
        uuid = await self.worker_manager.increase_worker(
            *args,
            **kwargs
        )
        return uuid

    async def dispatch_fun_task(
            self,
            worker_uuid: WorkerUUID,
            func_task: TaskInput,
            change_status=False,
            timeout=None,
            *arguments,
            **kwargs
    ) -> TaskUUID:
        assert func_task, Exception(f"func_task can't be {func_task}")
        task_queue = await self.worker_manager.get_task_queue(worker_uuid)
        task_uuid = cast(TaskUUID, uuid_generator())
        await task_queue.put(
            TaskQueueMessage(
                _warp_to_trans_task(task_uuid, func_task, change_status),
                TaskMeta(arguments, kwargs, timeout)
            )
        )
        await self.task_status_queue.put(StatusQueueMessage(worker_uuid, task_uuid, TaskStatus.QUEUED, None))
        return task_uuid

    async def generate_worker_state(
            self,
            worker_uuid: WorkerUUID,
            state_generator: TaskInput,
            timeout=None,
            *arguments
    ) -> TaskUUID:
        return await self.dispatch_fun_task(
            worker_uuid,
            state_generator,
            change_status=True,
            timeout=timeout,
            *arguments
        )

    async def stop_task(
            self,
            worker_uuid: WorkerUUID,
            task_uuid: TaskUUID
    ):
        worker_control_queue = await self.worker_manager.get_control_queue(worker_uuid)
        await worker_control_queue.put(ControlQueueMessage(
            cast(WorkerUUID, task_uuid),
            TaskControl.KILL
        ))

    async def stop_worker(
            self,
            worker_uuid: WorkerUUID
    ):
        worker_control_queue = await self.worker_manager.get_control_queue(worker_uuid)
        await self.worker_manager.stop_worker(worker_uuid)
        await worker_control_queue.put(ControlQueueMessage(worker_uuid, TaskControl.KILL))

    async def kill_worker(
            self,
            worker_uuid: WorkerUUID
    ):
        await self.worker_manager.kill_worker(worker_uuid)

    async def get_queued_status(
            self,
            timeout: None | float = None
    ) -> StatusReport | None:
        res = await self.task_status_queue.get(timeout)
        if res is None:
            return None
        return StatusReport(
            res.worker_uuid,
            res.task_uuid,
            res.status,
            res.content,
            res.create_timestamp
        )
