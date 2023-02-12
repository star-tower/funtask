from uuid import uuid4 as uuid_generator
from typing import List, TypeVar, Tuple, cast

from funtask.core import entities
from funtask.core import interface_and_types as interface

_T = TypeVar('_T')


def _split_task_and_dependencies(
        state_generator: interface.TaskInput
) -> Tuple[interface.FuncTask, List[str]]:
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
    return task, dependencies  # type: ignore


def _exec_none(*args, **kwargs):
    return None


def _warp_to_trans_task(
        uuid: entities.TaskUUID,
        task: interface.TaskInput,
        result_as_state: bool
) -> interface.InnerTask:
    """
    warp state_generator to callable with is_state_regenerator and dependencies props
    """
    task, dependencies = _split_task_and_dependencies(task)

    if task is None:
        none_is_executable_wrapper = _exec_none
    else:
        none_is_executable_wrapper = task

    return interface.InnerTask(
        uuid=uuid,
        task=none_is_executable_wrapper,
        dependencies=dependencies,
        result_as_state=result_as_state
    )


class FunTaskManager(interface.FunTaskManager):
    def __init__(
            self,
            *,
            worker_manager: interface.WorkerManager,
            # worker_uuid, task_uuid, status, content
            task_status_queue: interface.Queue[interface.StatusQueueMessage]
    ):
        self.worker_manager = worker_manager
        self.task_status_queue = task_status_queue

    async def increase_workers(
            self,
            number: int | None = None,
            *args,
            **kwargs
    ) -> List[entities.WorkerUUID]:
        workers_uuid = []
        for i in range(number or 1):
            workers_uuid.append(await self.increase_worker(*args, **kwargs))
        return workers_uuid

    async def increase_worker(
            self,
            *args,
            **kwargs
    ) -> entities.WorkerUUID:
        uuid = await self.worker_manager.increase_worker(
            *args,
            **kwargs
        )
        return uuid

    async def dispatch_fun_task(
            self,
            worker_uuid: entities.WorkerUUID,
            func_task: interface.TaskInput,
            change_status=False,
            timeout=None,
            *arguments,
            **kwargs
    ) -> entities.TaskUUID:
        assert func_task, Exception(f"func_task can't be {func_task}")
        task_queue = await self.worker_manager.get_task_queue(worker_uuid)
        task_uuid = cast(entities.TaskUUID, str(uuid_generator()))
        await self.task_status_queue.put(
            interface.StatusQueueMessage(
                worker_uuid, task_uuid, entities.TaskStatus.QUEUED, None)
        )
        await task_queue.put(
            interface.TaskQueueMessage(
                _warp_to_trans_task(task_uuid, func_task, change_status),
                interface.InnerTaskMeta(arguments, kwargs, timeout)
            )
        )
        return task_uuid

    async def generate_worker_state(
            self,
            worker_uuid: entities.WorkerUUID,
            state_generator: interface.TaskInput,
            timeout=None,
            *arguments
    ) -> entities.TaskUUID:
        return await self.dispatch_fun_task(
            worker_uuid,
            state_generator,
            change_status=True,
            timeout=timeout,
            *arguments
        )

    async def stop_task(
            self,
            worker_uuid: entities.WorkerUUID,
            task_uuid: entities.TaskUUID
    ):
        worker_control_queue = await self.worker_manager.get_control_queue(worker_uuid)
        await worker_control_queue.put(interface.ControlQueueMessage(
            cast(entities.WorkerUUID, task_uuid),
            interface.TaskControl.KILL
        ))

    async def stop_worker(
            self,
            worker_uuid: entities.WorkerUUID
    ):
        worker_control_queue = await self.worker_manager.get_control_queue(worker_uuid)
        await self.worker_manager.stop_worker(worker_uuid)
        await worker_control_queue.put(interface.ControlQueueMessage(worker_uuid, interface.TaskControl.KILL))

    async def kill_worker(
            self,
            worker_uuid: entities.WorkerUUID
    ):
        await self.worker_manager.kill_worker(worker_uuid)

    async def get_queued_status(
            self,
            timeout: None | float = None
    ) -> interface.StatusReport | None:
        res = await self.task_status_queue.get(timeout)
        if res is None:
            return None
        return interface.StatusReport(
            res.worker_uuid,
            res.task_uuid,
            res.status,
            res.content,
            res.create_timestamp
        )
