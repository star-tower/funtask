import asyncio

from funtask.funtask_types import WorkerManager, Logger
from funtask import Queue, TaskMeta, TaskStatus, WorkerStatus, TaskControl
from multiprocessing import Process
from typing import Tuple, Any, Dict

from funtask.queue.multiprocessing_queue import MultiprocessingQueue
from funtask.worker_runner import WorkerRunner, WorkerQueue


class MultiprocessingManager(WorkerManager):
    def __init__(self, logger: Logger):
        self.logger = logger
        # task_uuid -> (process, task_queue, control_queue)
        self.worker2queues: Dict[str, Tuple[Process, Queue, Queue]] = {}

    def increase_worker(
            self, worker_uuid: str, task_queue: 'Queue[Tuple[str, TaskMeta]]',
            task_status_queue: 'Queue[Tuple[str, str, TaskStatus | WorkerStatus, Any]]',
            control_queue: 'Queue[Tuple[str, TaskControl]]', *args, **kwargs
    ):
        task_queue = MultiprocessingQueue()
        status_queue = MultiprocessingQueue()
        control_queue = MultiprocessingQueue()
        worker_runner = WorkerRunner(WorkerQueue(
            task_queue=task_queue,
            status_queue=status_queue,
            control_queue=control_queue,
        ), worker_uuid, self.logger)
        process = Process(target=lambda: asyncio.run(worker_runner.run()), name=worker_uuid)
        process.start()
        self.worker2queues[worker_uuid] = (process, task_queue, control_queue)

    def kill_worker(self, worker_uuid: str):
        self.worker2queues[worker_uuid][0].kill()

    def stop_worker(self, worker_uuid: str):
        ...

    def get_task_queue(self, worker_uuid: str) -> 'Queue[Tuple[str, TaskMeta]]':
        _, task_queue, _ = self.worker2queues[worker_uuid]
        return task_queue

    def get_control_queue(self, worker_uuid: str) -> 'Queue[Tuple[str, TaskControl]]':
        _, _, control_queue = self.worker2queues[worker_uuid]
        return control_queue
