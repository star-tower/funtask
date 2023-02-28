import asyncio
import uuid

from multiprocessing import Process
import multiprocessing
from typing import Dict, cast

from funtask.utils.namespace import with_namespace
from funtask.core.worker import Worker
from funtask.core import interface_and_types as interface, entities

multiprocessing.set_start_method('fork')


class MultiprocessingManager(interface.WorkerManager):
    def __init__(
            self,
            logger: interface.Logger,
            task_queue_factory: interface.QueueFactory,
            control_queue_factory: interface.QueueFactory,
            task_status_queue: interface.Queue
    ):
        self.logger = logger
        # task_uuid -> (process, task_queue, control_queue)
        self.worker_id2process: Dict[str, Process] = {}
        self.task_queue_factory = task_queue_factory
        self.control_queue_factory = control_queue_factory
        self.task_status_queue = task_status_queue

    async def increase_worker(
            self
    ) -> str:
        worker_uuid = cast(entities.WorkerUUID, str(uuid.uuid4()))
        task_queue = self.task_queue_factory(with_namespace('task_queue', worker_uuid))
        control_queue = self.control_queue_factory(with_namespace('control_queue', worker_uuid))
        worker_runner = Worker(interface.WorkerQueue(
            task_queue=task_queue,
            status_queue=self.task_status_queue,
            control_queue=control_queue,
        ), worker_uuid, self.logger)
        process = Process(target=lambda: asyncio.run(worker_runner.run()), name=worker_uuid, daemon=True)
        process.start()
        self.worker_id2process[worker_uuid] = process
        return worker_uuid

    async def kill_worker(self, worker_uuid: str):
        self.worker_id2process[worker_uuid].kill()

    async def stop_worker(self, worker_uuid: str):
        ...

    async def get_task_queue(self, worker_uuid: str) -> 'interface.Queue[interface.TaskQueueMessage]':
        return self.task_queue_factory(with_namespace('task_queue', worker_uuid))

    async def get_control_queue(self, worker_uuid: str) -> 'interface.Queue[interface.ControlQueueMessage]':
        return self.control_queue_factory(with_namespace('control_queue', worker_uuid))

    def __del__(self):
        # release all process after manager been deleted
        for process in self.worker_id2process.values():
            process.kill()
