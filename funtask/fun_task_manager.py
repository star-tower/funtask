import functools
from abc import abstractmethod
from dataclasses import dataclass
from uuid import uuid4 as uuid_generator
from typing import List, TypeVar, Tuple, Generic, Any

import dill

from funtask.funtask_types import StateGeneratorWithDependencies, StateGenerator, TransStateGenerator, FuncTask, TaskStatus, \
    WorkerManager, QueueFactory, Queue, TaskMeta, TaskControl

_T = TypeVar('_T')


@dataclass
class Task(Generic[_T]):
    uuid: str
    worker_uuid: str
    _task_manager: 'FunTaskManager'

    @abstractmethod
    async def get_status(self) -> TaskStatus:
        ...

    @abstractmethod
    async def get_result(self) -> _T:
        ...

    @abstractmethod
    async def get_log(self) -> str | List[str]:
        ...

    async def stop(
            self
    ):
        await self._task_manager.stop_task(self.worker_uuid, self.uuid)


def with_namespace(namespace: str, *values: str) -> str:
    return '#'.join([namespace, *values])


def _split_generator_and_dependencies(
        state_generator: 'StateGeneratorWithDependencies'
) -> Tuple['StateGenerator', List[str]]:
    """
    change StateGeneratorWithDependencies to (StateGenerator, [dependencies_str...])
    :param state_generator: StateGeneratorWithDependencies
    :return: (StateGenerator, [dependencies_str...])
    """
    if isinstance(state_generator, Tuple):
        generator, dependencies = state_generator
        if isinstance(dependencies, str):
            dependencies = [dependencies]
    elif state_generator is None:
        generator, dependencies = lambda _: None, []
    else:
        generator, dependencies = state_generator, []
    return generator, dependencies


def _warp_state_generator(state_generator: 'StateGeneratorWithDependencies') -> 'TransStateGenerator':
    """
    warp state_generator to callable with is_state_regenerator and dependencies props
    """
    state_generator, dependencies = _split_generator_and_dependencies(state_generator)

    @functools.wraps(state_generator)
    def generator_wrapper(state):
        return state_generator and state_generator(state)

    generator_wrapper.dependencies = dependencies
    return generator_wrapper


@dataclass
class Worker:
    uuid: str
    _task_manager: 'FunTaskManager'

    async def dispatch_fun_task(
            self,
            func_task: 'FuncTask',
            *arguments
    ) -> 'Task[_T]':
        return await self._task_manager.dispatch_fun_task(self.uuid, func_task, *arguments)

    async def regenerate_state(
            self,
            state_generator: 'StateGeneratorWithDependencies'
    ):
        return await self._task_manager.generate_worker_state(self.uuid, state_generator)

    async def stop(
            self
    ):
        await self._task_manager.stop_worker(self.uuid)


class FunTaskManager:
    def __init__(
            self,
            *,
            namespace: str,
            worker_manager: WorkerManager,
            task_queue_factory: QueueFactory,
            task_status_queue_factory: QueueFactory,
            control_queue_factory: QueueFactory,
            task_status_queue: Queue[Tuple[str, TaskStatus, Any]] = None
    ):
        self.worker_manager = worker_manager
        self.task_queue_factory = task_queue_factory
        self.control_queue_factory = control_queue_factory
        self.namespace = namespace
        self.task_status_queue = task_status_queue or task_status_queue_factory(namespace)

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
        uuid = str(uuid_generator())
        worker_task_queue = self.task_queue_factory(with_namespace(self.namespace, 'worker', uuid))
        self.worker_manager.increase_worker(
            uuid,
            worker_task_queue,
            self.task_status_queue,
            self.control_queue_factory(self.namespace),
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
            func_task: FuncTask,
            *arguments
    ) -> Task[_T]:
        assert func_task, Exception(f"func_task can't be {func_task}")
        task_queue = self.worker_manager.get_task_queue(worker_uuid)
        task_uuid = str(uuid_generator())
        await task_queue.put((dill.dumps(func_task), TaskMeta(task_uuid, arguments)))
        await self.task_status_queue.put((task_uuid, TaskStatus.QUEUED, None))
        task = Task(
            task_uuid,
            worker_uuid,
            self
        )
        return task

    async def generate_worker_state(
            self,
            worker_uuid: str,
            state_generator: StateGeneratorWithDependencies
    ) -> Task[_T]:
        state_generator = _warp_state_generator(state_generator)

        return await self.dispatch_fun_task(worker_uuid, state_generator)

    async def stop_task(
            self,
            worker_uuid: str,
            task_uuid: str
    ):
        worker_control_queue = self.worker_manager.get_control_queue(worker_uuid)
        await worker_control_queue.put((task_uuid, TaskControl.KILL))

    async def stop_worker(
            self,
            worker_uuid: str
    ):
        worker_control_queue = self.worker_manager.get_control_queue(worker_uuid)
        self.worker_manager.stop_worker(worker_uuid)
        await worker_control_queue.put((worker_uuid, TaskControl.KILL))

    async def kill_worker(
            self,
            worker_uuid: str
    ):
        self.worker_manager.kill_worker(worker_uuid)
