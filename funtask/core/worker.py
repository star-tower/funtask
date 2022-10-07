import threading
import time
import traceback
from typing import Tuple, Any, Dict, Callable, TypeVar
import asyncio
import dill
from dataclasses import dataclass

from funtask.core.funtask_types import TaskStatus, Queue, TaskControl, Logger, LogLevel, WorkerStatus, \
    TransTaskMeta, TransTask, BreakRef
from funtask.core.utils.killable import killable
from funtask.core.utils.sandbox import UnsafeSandbox

_T = TypeVar('_T')


def raise_exception(e: Exception):
    raise e


class KillSigCauseBreakGet(BreakRef):
    def __init__(self, with_stopped):
        self.with_stopped = with_stopped

    def if_break_now(self) -> bool:
        return self.with_stopped.stopped


@dataclass
class WorkerQueue:
    task_queue: Queue[Tuple[bytes, TransTaskMeta]]
    # worker_uuid, task_uuid, status, content
    status_queue: Queue[Tuple[str, str | None, TaskStatus | WorkerStatus, Any]]
    control_queue: Queue[Tuple[str, TaskControl]]


async def get_task_from_queue(task_queue: Queue[Tuple[bytes, TransTaskMeta]], kill_sig_breaker: KillSigCauseBreakGet):
    none_or_task_and_meta = await task_queue.watch_and_get(kill_sig_breaker)
    if none_or_task_and_meta is not None:
        task, task_meta = none_or_task_and_meta
    else:
        return None, None
    func_task: TransTask = dill.loads(task)
    return func_task, task_meta


class Worker:
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
        self.sandbox = UnsafeSandbox()
        self.running_tasks: Dict[str, Callable] = {}
        self.state = None

    async def kill_sign_monitor(self):
        last_heart_beat = time.time()
        while True:
            try:
                control = await self.queue.control_queue.get(timeout=1)
                if time.time() - last_heart_beat > 5:
                    await self.queue.status_queue.put((
                        self.worker_uuid,
                        None,
                        WorkerStatus.HEARTBEAT,
                        None
                    ))
                    last_heart_beat = time.time()
                if control is None:
                    continue
                task_or_worker_uuid, sig = control
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
                            break
                        else:
                            self.running_tasks.get(task_or_worker_uuid, lambda: ...)()
            except Exception as e:
                await self.logger.log(str(e) + '\n' + traceback.format_exc(), LogLevel.ERROR, ['signal'])

    async def _async_task_caller(self, func_task: TransTask, task_meta: TransTaskMeta):
        try:
            self.running_tasks[func_task.uuid] = lambda: raise_exception(Exception('cannot kill a async task'))
            if func_task.result_as_state:
                self.sandbox = UnsafeSandbox(
                    func_task.dependencies
                )
                self.state, _ = await self.sandbox.async_call_with(
                    [],
                    func_task.task,
                    self.state, self.logger, *task_meta.arguments, **task_meta.kw_arguments
                )
                await self.queue.status_queue.put((self.worker_uuid, func_task.uuid, TaskStatus.SUCCESS, None))
            else:
                result, _ = await self.sandbox.async_call_with(
                    func_task.dependencies,
                    func_task.task,
                    self.state, self.logger, *task_meta.arguments, **task_meta.kw_arguments
                )
                await self.queue.status_queue.put((self.worker_uuid, func_task.uuid, TaskStatus.SUCCESS, result))
        except Exception as e:
            print(e)
            task_meta and await self.queue.status_queue.put((self.worker_uuid, func_task.uuid, TaskStatus.ERROR, e))
        finally:
            self.running_tasks.pop(func_task.uuid, None)

    async def _task_caller(self, func_task: TransTask, task_meta: TransTaskMeta):
        try:
            with killable(task_meta.timeout, mute=False) as kill:
                self.running_tasks[func_task.uuid] = kill
                if func_task.result_as_state:
                    self.sandbox = UnsafeSandbox(
                        func_task.dependencies
                    )
                    self.state, _ = self.sandbox.call_with(
                        [],
                        func_task.task,
                        self.state, self.logger, *task_meta.arguments, **task_meta.kw_arguments
                    )
                    await self.queue.status_queue.put((self.worker_uuid, func_task.uuid, TaskStatus.SUCCESS, None))
                else:
                    result, _ = self.sandbox.call_with(
                        func_task.dependencies,
                        func_task.task,
                        self.state, self.logger, *task_meta.arguments, **task_meta.kw_arguments
                    )
                    await self.queue.status_queue.put((self.worker_uuid, func_task.uuid, TaskStatus.SUCCESS, result))
        except Exception as e:
            task_meta and await self.queue.status_queue.put((self.worker_uuid, func_task.uuid, TaskStatus.ERROR, e))
        finally:
            self.running_tasks.pop(func_task.uuid, None)

    async def run(self):
        running_tasks = set()
        threading.Thread(target=lambda: asyncio.run(self.kill_sign_monitor())).start()
        while not self.stopped:
            try:
                func_task, task_meta = await get_task_from_queue(
                    self.queue.task_queue,
                    KillSigCauseBreakGet(self)
                )
                # if (func_task, task_meta) is (None, None) the self.stopped must be True
                # because KillSigCauseBreakGet will set it
                if self.stopped:
                    break
                await self.queue.status_queue.put((self.worker_uuid, func_task.uuid, TaskStatus.RUNNING, None))
                if asyncio.iscoroutinefunction(func_task.task):
                    task = asyncio.create_task(self._async_task_caller(func_task, task_meta), name=func_task.uuid)
                    running_tasks.add(task)
                    task.add_done_callback(lambda t: running_tasks.remove(t))
                else:
                    await self._task_caller(func_task, task_meta)

            except Exception as e:
                await self.logger.log(str(e) + '\n' + traceback.format_exc(), LogLevel.ERROR, ["worker", "exception"])
