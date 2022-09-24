import threading
import time
import traceback
from typing import Tuple, Any, List, Dict, Callable, TypeVar
import asyncio
import dill
from dataclasses import dataclass

from funtask.funtask_types import TaskStatus, Queue, TaskMeta, TaskControl, TransStateGenerator, FuncTask, Logger, \
    LogLevel, WorkerStatus
from funtask.utils import ImportMixInGlobal, killable

_T = TypeVar('_T')


def raise_exception(e: Exception):
    raise e


@dataclass
class WorkerQueue:
    task_queue: Queue[Tuple[bytes, TaskMeta]]
    status_queue: Queue[Tuple[str, str, TaskStatus | WorkerStatus, Any]]
    control_queue: Queue[Tuple[str, TaskControl]]


async def get_task_from_queue(task_queue: Queue[Tuple[bytes, TaskMeta]]):
    task, task_meta = await task_queue.get()
    func_task: FuncTask | TransStateGenerator = dill.loads(task)
    return func_task, task_meta


class WorkerRunner:
    def __init__(
            self,
            queue: WorkerQueue,
            worker_uuid: str,
            logger: Logger,
    ):
        self.queue = queue
        self.worker_uuid = worker_uuid
        self.logger = logger
        self.stopped = False
        self.state_generator = lambda: None
        self.dependencies = []
        self.state_version = 0
        self.running_tasks: Dict[str, Callable] = {}
        self.global_id2mixin: Dict[int, Tuple[ImportMixInGlobal, int]] = {}
        self._global_upsert_dependencies(
            self.state_generator.__globals__,
            self.dependencies,
            self.state_version
        )
        self.state = None

    def _global_upsert_dependencies(
            self,
            globals_: Dict,
            dependencies: List[str],
            dependencies_version: int
    ):
        generator_global_id = id(globals_)
        if generator_global_id not in self.global_id2mixin:
            self.global_id2mixin[generator_global_id] = ImportMixInGlobal(), dependencies_version
            self.global_id2mixin[generator_global_id][0].import_module_globally(
                dependencies,
                globals_
            )
        elif self.global_id2mixin[generator_global_id][1] != dependencies_version:
            mixin, _ = self.global_id2mixin[generator_global_id]
            mixin.import_module_globally(
                dependencies,
                globals_
            )
            self.global_id2mixin[generator_global_id] = mixin, dependencies_version

    async def kill_sign_monitor(self):
        last_heart_beat = time.time()
        while True:
            try:
                task_or_worker_uuid, sig = await self.queue.control_queue.get()
                if time.time() - last_heart_beat > 5:
                    await self.queue.status_queue.put((
                        self.worker_uuid,
                        'worker',
                        WorkerStatus.HEARTBEAT,
                        Exception('worker stop signal')
                    ))
                    last_heart_beat = time.time()
                match sig:
                    case TaskControl.KILL:
                        if task_or_worker_uuid == self.worker_uuid:
                            self.stopped = True
                            await self.queue.status_queue.put((
                                self.worker_uuid,
                                'worker',
                                TaskStatus.ERROR,
                                Exception('worker stop signal')
                            ))
                        else:
                            self.running_tasks.get(task_or_worker_uuid, lambda: ...)()
            except Exception as e:
                await self.logger.log(str(e) + '\n' + traceback.format_exc(), LogLevel.ERROR, ['signal'])

    async def _async_task_caller(self, func_task: FuncTask, task_meta: TaskMeta):
        try:
            self.running_tasks[task_meta.uuid] = lambda: raise_exception(Exception('cannot kill a async task'))
            if task_meta.is_state_generator:
                self._global_upsert_dependencies(
                    func_task.__globals__,
                    func_task.dependencies,
                    self.state_version + 1
                )
                self.dependencies = func_task.dependencies
                self.state_version += 1
                self.state = await func_task(self.state, self.logger, *task_meta.arguments)
                await self.queue.status_queue.put((self.worker_uuid, task_meta.uuid, TaskStatus.SUCCESS, None))
            else:
                self._global_upsert_dependencies(
                    func_task.__globals__,
                    self.dependencies,
                    self.state_version
                )
                result = await func_task(self.state, self.logger, *task_meta.arguments)
                await self.queue.status_queue.put((self.worker_uuid, task_meta.uuid, TaskStatus.SUCCESS, result))
        except Exception as e:
            task_meta and await self.queue.status_queue.put((self.worker_uuid, task_meta.uuid, TaskStatus.ERROR, e))
        finally:
            self.running_tasks.pop(task_meta.uuid, None)

    async def _task_caller(self, func_task: FuncTask, task_meta: TaskMeta):
        try:
            with killable(task_meta.timeout, mute=False) as kill:
                self.running_tasks[task_meta.uuid] = kill
                if task_meta.is_state_generator:
                    self._global_upsert_dependencies(
                        func_task.__globals__,
                        func_task.dependencies,
                        self.state_version + 1
                    )
                    self.dependencies = func_task.dependencies
                    self.state_version += 1
                    self.state = func_task(self.state, self.logger, *task_meta.arguments)
                    await self.queue.status_queue.put((self.worker_uuid, task_meta.uuid, TaskStatus.SUCCESS, None))
                else:
                    self._global_upsert_dependencies(
                        func_task.__globals__,
                        self.dependencies,
                        self.state_version
                    )
                    result = func_task(self.state, self.logger, *task_meta.arguments)
                    await self.queue.status_queue.put((self.worker_uuid, task_meta.uuid, TaskStatus.SUCCESS, result))
        except Exception as e:
            task_meta and await self.queue.status_queue.put((self.worker_uuid, task_meta.uuid, TaskStatus.ERROR, e))
        finally:
            self.running_tasks.pop(task_meta.uuid, None)

    async def run(self):
        running_tasks = set()
        threading.Thread(target=lambda: asyncio.run(self.kill_sign_monitor())).start()
        while not self.stopped:
            try:
                fun_task, task_meta = await get_task_from_queue(self.queue.task_queue)
                await self.queue.status_queue.put((self.worker_uuid, task_meta.uuid, TaskStatus.RUNNING, None))
                if asyncio.iscoroutinefunction(fun_task):
                    task = asyncio.create_task(self._async_task_caller(fun_task, task_meta), name=task_meta.uuid)
                    running_tasks.add(task)
                    task.add_done_callback(lambda t: running_tasks.remove(t))
                else:
                    await self._task_caller(fun_task, task_meta)

            except Exception as e:
                await self.logger.log(str(e) + '\n' + traceback.format_exc(), LogLevel.ERROR, ["worker", "exception"])
