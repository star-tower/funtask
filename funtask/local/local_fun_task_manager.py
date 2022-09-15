import functools
from enum import auto
from uuid import uuid4 as uuid_generator
from multiprocessing import Queue, Process
from typing import List, Tuple, Dict
import dill

from funtask.fun_task_manager import FunTaskManager, ScopeGenerator, Task, Logger, _T, Worker, WorkerStatus, TaskStatus, \
    FuncTask
from funtask.utils import AutoName


class Flags(AutoName):
    STOP = auto()  # type: ignore


def processor_generator(logger: Logger, scope_generator: ScopeGenerator | None):
    def processor_helper(task_queue: Queue, result_queue: Queue):
        scope = scope_generator and scope_generator(None)
        while True:
            task, task_uuid = task_queue.get()
            task = dill.loads(task)
            if task is Flags.STOP:
                break
            else:
                try:
                    result = task(scope, logger)
                    if getattr(task, "is_scope_regenerator", None):
                        scope = result
                        continue
                except Exception as e:
                    result = e
                result_queue.put((task_uuid, result))

    return processor_helper


class LocalFunTaskManager(FunTaskManager):
    def __init__(self, logger: Logger):
        self.logger = logger
        self.result_queue = Queue()
        self.workers: Dict[str, Tuple[Process, Queue, Worker]] = {}
        self.tasks: Dict[str, Task] = {}

    def increase_workers(
            self, scope_generator: ScopeGenerator | List[ScopeGenerator] | None,
            number: int = None,
            *args,
            **kwargs
    ) -> List[Worker]:
        assert isinstance(scope_generator, List) or number, Exception("scope_generator is list or number is not None")
        workers = []
        if isinstance(scope_generator, List):
            for scope_generator_ in scope_generator:
                workers.append(self.increase_worker(scope_generator_, *args, **kwargs))
        else:
            for i in range(number):
                workers.append(self.increase_worker(scope_generator, *args, **kwargs))
        return workers

    def increase_worker(
            self,
            scope_generator: ScopeGenerator | List[ScopeGenerator] | None,
            *args,
            **kwargs
    ) -> Worker:
        uuid = str(uuid_generator())
        task_queue = Queue()
        process = Process(target=processor_generator(self.logger, scope_generator), args=(task_queue, self.result_queue))
        worker = Worker(
            uuid,
            WorkerStatus.RUNNING,
            self
        )
        self.workers[uuid] = (process, task_queue, worker)
        process.start()
        return worker

    def get_worker_from_uuid(self, uuid: str) -> Worker:
        return self.workers[uuid][2]

    def dispatch_fun_task(self, worker_uuid: str, func_task: FuncTask) -> Task[_T]:
        assert func_task, Exception(f"func_task can't be {func_task}")
        _, q, _ = self.workers[worker_uuid]
        task_uuid = str(uuid_generator())
        q.put_nowait((dill.dumps(func_task), task_uuid))
        task = Task(
            task_uuid,
            TaskStatus.RUNNING,
            self
        )
        self.tasks[task_uuid] = task
        return task

    def get_task_from_uuid(self, uuid: str) -> Task:
        return self.tasks[uuid]

    def regenerate_worker_scope(self, worker_uuid: str, scope_generator: ScopeGenerator | None):
        @functools.wraps(scope_generator)
        def generator_wrapper(scope):
            return scope_generator and scope_generator(scope)

        generator_wrapper.is_scope_regenerator = True

        self.dispatch_fun_task(worker_uuid, generator_wrapper)

    def stop_task(self, task_uuid: str):
        raise NotImplementedError("can't stop task in local_func_task_manager")

    def stop_worker(self, worker_uuid: str):
        _, q, _ = self.workers[worker_uuid]
        task_uuid = str(uuid_generator())
        q.put_nowait((dill.dumps(None), task_uuid))
