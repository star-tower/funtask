import asyncio
import uuid

from funtask.core.funtask_types import WorkerManager, Logger, QueueFactory, TaskQueueMessage, ControlQueueMessage
from funtask import Queue
from multiprocessing import Process
import multiprocessing
from typing import Dict

from funtask.utils.namespace import with_namespace
from funtask.core.worker import Worker, WorkerQueue

multiprocessing.set_start_method('fork')


class MultiprocessingManager(WorkerManager):
    def __init__(
            self,
            logger: Logger,
            task_queue_factory: QueueFactory,
            control_queue_factory: QueueFactory,
            task_status_queue: Queue
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
        worker_uuid = str(uuid.uuid4())
        task_queue = self.task_queue_factory(with_namespace('task_queue', worker_uuid))
        control_queue = self.control_queue_factory(with_namespace('control_queue', worker_uuid))
        worker_runner = Worker(WorkerQueue(
            task_queue=task_queue,
            status_queue=self.task_status_queue,
            control_queue=control_queue,
        ), worker_uuid, self.logger)
        process = Process(target=lambda: asyncio.run(worker_runner.run()), name=worker_uuid)
        process.start()
        self.worker_id2process[worker_uuid] = process
        return worker_uuid

    async def kill_worker(self, worker_uuid: str):
        self.worker_id2process[worker_uuid].kill()

    async def stop_worker(self, worker_uuid: str):
        ...

    async def get_task_queue(self, worker_uuid: str) -> 'Queue[TaskQueueMessage]':
        return self.task_queue_factory(with_namespace('task_queue', worker_uuid))

    async def get_control_queue(self, worker_uuid: str) -> 'Queue[ControlQueueMessage]':
        return self.control_queue_factory(with_namespace('control_queue', worker_uuid))
