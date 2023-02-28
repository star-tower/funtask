import threading
import time
import traceback
from typing import Dict, Callable, TypeVar
import asyncio
from funtask.core import entities, interface_and_types as interface
from funtask.utils.killable import killable
from funtask.utils.sandbox import UnsafeSandbox

_T = TypeVar('_T')


def raise_exception(e: Exception):
    raise e


class KillSigCauseBreakGet(interface.BreakRef):
    def __init__(self, with_stopped):
        self.with_stopped = with_stopped

    def if_break_now(self) -> bool:
        return self.with_stopped.stopped


async def get_task_from_queue(
        task_queue: interface.Queue[interface.TaskQueueMessage],
        kill_sig_breaker: KillSigCauseBreakGet
):
    task_queue_msg = await task_queue.watch_and_get(kill_sig_breaker)
    if task_queue_msg is None:
        return None, None
    return task_queue_msg.task, task_queue_msg.task_meta


class Worker:
    def __init__(
            self,
            queue: interface.WorkerQueue,
            worker_uuid: entities.WorkerUUID,
            logger: interface.Logger,
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
                    # heart beat status is None
                    status_message = interface.StatusQueueMessage(
                        self.worker_uuid,
                        None,
                        None,
                        None
                    )
                    await self.queue.status_queue.put(status_message)
                    last_heart_beat = time.time()
                if control is None:
                    await asyncio.sleep(.01)
                    continue
                match control.control_sig:
                    case interface.TaskControl.KILL:
                        if control.worker_uuid == self.worker_uuid:
                            self.stopped = True
                            await self.queue.status_queue.put(interface.StatusQueueMessage(
                                self.worker_uuid,
                                None,
                                entities.TaskStatus.ERROR,
                                Exception('worker stop signal')
                            ))
                            break
                        else:
                            self.running_tasks.get(control.worker_uuid, lambda: ...)()
            except Exception as e:
                await self.logger.log(str(e) + '\n' + traceback.format_exc(), interface.LogLevel.ERROR, ['signal'])

    async def _async_task_caller(self, func_task: interface.InnerTask, task_meta: interface.InnerTaskMeta):
        async with self.logger.task_context():
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
                    await self.queue.status_queue.put(
                        interface.StatusQueueMessage(
                            self.worker_uuid,
                            func_task.uuid,
                            entities.TaskStatus.SUCCESS,
                            None
                        )
                    )
                else:
                    result, _ = await self.sandbox.async_call_with(
                        func_task.dependencies,
                        func_task.task,
                        self.state, self.logger, *task_meta.arguments, **task_meta.kw_arguments
                    )
                    await self.queue.status_queue.put(
                        interface.StatusQueueMessage(
                            self.worker_uuid,
                            func_task.uuid,
                            entities.TaskStatus.SUCCESS,
                            result
                        )
                    )
            except Exception as e:
                task_meta and await self.queue.status_queue.put(
                    interface.StatusQueueMessage(
                        self.worker_uuid,
                        func_task.uuid,
                        entities.TaskStatus.ERROR,
                        e
                    )
                )
            finally:
                self.running_tasks.pop(func_task.uuid, None)

    async def _task_caller(
            self,
            func_task: interface.InnerTask,
            task_meta: interface.InnerTaskMeta,
            logger: interface.Logger
    ):
        async with self.logger.task_context():
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
                            self.state, logger, *task_meta.arguments, **task_meta.kw_arguments
                        )
                        await self.queue.status_queue.put(
                            interface.StatusQueueMessage(
                                self.worker_uuid,
                                func_task.uuid,
                                entities.TaskStatus.SUCCESS,
                                None
                            )
                        )
                    else:
                        result, _ = self.sandbox.call_with(
                            func_task.dependencies,
                            func_task.task,
                            self.state, logger, *task_meta.arguments, **task_meta.kw_arguments
                        )
                        await self.queue.status_queue.put(
                            interface.StatusQueueMessage(
                                self.worker_uuid,
                                func_task.uuid,
                                entities.TaskStatus.SUCCESS,
                                result
                            )
                        )
            except Exception as e:
                task_meta and await self.queue.status_queue.put(
                    interface.StatusQueueMessage(
                        self.worker_uuid,
                        func_task.uuid,
                        entities.TaskStatus.ERROR,
                        e
                    )
                )
                await logger.error(
                    str(e) + '\n' + traceback.format_exc(),
                    ["task", "exception"]
                )
            finally:
                self.running_tasks.pop(func_task.uuid, None)

    async def run(self):
        self.logger = await self.logger.with_worker(self.worker_uuid)
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
                await self.queue.status_queue.put(
                    interface.StatusQueueMessage(
                        self.worker_uuid,
                        func_task.uuid,
                        entities.TaskStatus.RUNNING,
                        None
                    )
                )
                if asyncio.iscoroutinefunction(func_task.task):
                    task = asyncio.create_task(self._async_task_caller(func_task, task_meta), name=func_task.uuid)
                    running_tasks.add(task)
                    task.add_done_callback(lambda t: running_tasks.remove(t))
                else:
                    await self._task_caller(func_task, task_meta, await self.logger.with_task(func_task.uuid))

            except Exception as e:
                await self.logger.error(
                    str(e) + '\n' + traceback.format_exc(),
                    ["worker", "exception"]
                )
