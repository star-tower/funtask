import asyncio
from enum import auto
from uuid import uuid4 as uuid_generator
from multiprocessing import Queue, Process
from typing import List, Tuple, Dict
import dill
from threading import Thread
from funtask.fun_task_manager import FunTaskManager, Task, Logger, _T, Worker, \
    WorkerStatus, TaskStatus, FuncTask, ScopeGeneratorWithDependencies, _warp_scope_generator, TransScopeGenerator
from funtask.utils import AutoName, ImportMixInGlobal


class Flags(AutoName):
    STOP = auto()  # type: ignore


class LocalFunTask(Task):
    result = ...
    error = ...

    async def get_result(self) -> _T:
        while self.result is ... and self.error is ...:
            await asyncio.sleep(0.02)
        if self.result is not ...:
            return self.result
        else:
            raise self.error

    async def get_log(self) -> str | List[str]:
        pass


def _global_upsert_dependencies(
        global_id2mixin: Dict[int, Tuple[ImportMixInGlobal, int]],
        globals_: Dict,
        dependencies: List[str],
        dependencies_version: int
):
    generator_global_id = id(globals_)
    if generator_global_id not in global_id2mixin:
        global_id2mixin[generator_global_id] = ImportMixInGlobal(), dependencies_version
        global_id2mixin[generator_global_id][0].import_module_globally(
            dependencies,
            globals_
        )
    elif global_id2mixin[generator_global_id][1] != dependencies_version:
        mixin, _ = global_id2mixin[generator_global_id]
        mixin.import_module_globally(
            dependencies,
            globals_
        )
        global_id2mixin[generator_global_id] = mixin, dependencies_version


def processor_generator(logger: Logger, scope_generator: TransScopeGenerator):
    def processor_helper(task_queue: Queue, status_queue: Queue):
        dependencies = scope_generator.dependencies
        dependencies_version = 0
        # global_id -> (import_mixin, version)
        global_id2mixin: Dict[int, Tuple[ImportMixInGlobal, int]] = {}
        _global_upsert_dependencies(global_id2mixin, scope_generator.__globals__, dependencies, dependencies_version)
        scope = scope_generator(None)
        while True:
            task, task_uuid = task_queue.get()
            task: FuncTask | TransScopeGenerator
            task = dill.loads(task)
            if task is Flags.STOP:
                break
            else:
                try:
                    status_queue.put((task_uuid, TaskStatus.RUNNING, None))
                    if getattr(task, "is_scope_regenerator", None):
                        dependencies = task.dependencies
                        dependencies_version += 1
                        _global_upsert_dependencies(
                            global_id2mixin,
                            task.__globals__,
                            dependencies,
                            dependencies_version
                        )
                        scope = task(scope)
                        status_queue.put((task_uuid, TaskStatus.SUCCESS, None))
                    else:
                        _global_upsert_dependencies(
                            global_id2mixin,
                            task.__globals__,
                            dependencies,
                            dependencies_version
                        )
                        result = task(scope, logger)
                        status_queue.put((task_uuid, TaskStatus.SUCCESS, result))
                except Exception as e:
                    status_queue.put((task_uuid, TaskStatus.ERROR, e))

    return processor_helper


class LocalFunTaskManager(FunTaskManager):
    def __init__(self, logger: Logger):
        self.logger = logger
        self.task_status_queue = Queue()
        self.workers: Dict[str, Tuple[Process, Queue, Worker]] = {}
        self.tasks: Dict[str, LocalFunTask] = {}

        def result_consumer():
            while True:
                task_uuid, status, meta = self.task_status_queue.get(True)
                task = self.tasks[task_uuid]
                task.status = status
                match status:
                    case TaskStatus.SUCCESS:
                        task.result = meta
                    case TaskStatus.ERROR:
                        task.error = meta

        Thread(target=result_consumer).start()

    def increase_workers(
            self,
            scope_generator: ScopeGeneratorWithDependencies | List[
                ScopeGeneratorWithDependencies
            ] | None,
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
            scope_generator: ScopeGeneratorWithDependencies,
            *args,
            **kwargs
    ) -> Worker:
        uuid = str(uuid_generator())
        task_queue = Queue()
        process = Process(target=processor_generator(self.logger, _warp_scope_generator(scope_generator)),
                          args=(task_queue, self.task_status_queue))
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
        task = LocalFunTask(
            task_uuid,
            TaskStatus.QUEUED,
            self
        )
        self.tasks[task_uuid] = task
        return task

    def get_task_from_uuid(self, uuid: str) -> Task:
        return self.tasks[uuid]

    def regenerate_worker_scope(
            self,
            worker_uuid: str,
            scope_generator: ScopeGeneratorWithDependencies
    ):
        scope_generator = _warp_scope_generator(scope_generator)

        self.dispatch_fun_task(worker_uuid, scope_generator)

    def stop_task(self, task_uuid: str):
        raise NotImplementedError("can't stop task in local_func_task_manager")

    def stop_worker(self, worker_uuid: str):
        _, q, _ = self.workers[worker_uuid]
        task_uuid = str(uuid_generator())
        q.put_nowait((dill.dumps(None), task_uuid))
