from typing import AsyncIterator, cast
from asyncio import exceptions as asyncio_exceptions

import dill
from dependency_injector.wiring import Provide, inject
from grpclib.server import Server
from loguru import logger

from funtask.common.grpc import load_args, core_status2rpc_status
from funtask.core.entities import WorkerUUID, TaskUUID
from funtask.core.task_worker_manager import FunTaskManager
from funtask.generated import Worker, Task, StatusReport, Empty
from funtask.generated.manager import TaskWorkerManagerBase, IncreaseWorkerRequest, \
    IncreaseWorkersRequest, IncreaseWorkerResponse, IncreaseWorkersResponse, DispatchFunTaskResponse, \
    DispatchFunTaskRequest, StopTaskRequest, StopWorkerRequest, \
    KillWorkerRequest, GetQueuedStatusResponse


class ManagerService(TaskWorkerManagerBase):
    def __init__(self, fun_task_manager: FunTaskManager):
        self.fun_task_manager = fun_task_manager

    async def increase_workers(self, increase_workers_request: "IncreaseWorkersRequest") -> "IncreaseWorkersResponse":
        args, kwargs = load_args(increase_workers_request.other_args)
        workers_uuid = await self.fun_task_manager.increase_workers(
            increase_workers_request.number,
            *args,
            **kwargs
        )
        return IncreaseWorkersResponse(
            [Worker(worker_uuid) for worker_uuid in workers_uuid]
        )

    async def increase_worker(self, increase_worker_request: "IncreaseWorkerRequest") -> "IncreaseWorkerResponse":
        args, kwargs = load_args(
            increase_worker_request.args
        )
        return IncreaseWorkerResponse(
            Worker((await self.fun_task_manager.increase_worker(*args, **kwargs)))
        )

    async def dispatch_fun_task(self, dispatch_fun_task_request: "DispatchFunTaskRequest") -> "DispatchFunTaskResponse":
        args, kwargs = load_args(dispatch_fun_task_request.other_args)
        task_uuid = await self.fun_task_manager.dispatch_fun_task(
            cast(WorkerUUID, dispatch_fun_task_request.worker_uuid),
            dill.loads(dispatch_fun_task_request.serialized_fun_task),
            dispatch_fun_task_request.change_status,
            dispatch_fun_task_request.timeout,
            *args,
            **kwargs
        )
        return DispatchFunTaskResponse(Task(task_uuid))

    async def stop_task(self, stop_task_request: "StopTaskRequest") -> "Empty":
        await self.fun_task_manager.stop_task(
            cast(WorkerUUID, stop_task_request.worker_uuid),
            cast(TaskUUID, stop_task_request.task_uuid)
        )
        return Empty()

    async def stop_worker(self, stop_worker_request: "StopWorkerRequest") -> "Empty":
        await self.fun_task_manager.stop_worker(cast(WorkerUUID, stop_worker_request.worker_uuid))
        return Empty()

    async def kill_worker(self, kill_worker_request: "KillWorkerRequest") -> "Empty":
        await self.fun_task_manager.kill_worker(cast(WorkerUUID, kill_worker_request.worker_uuid))
        return Empty()

    async def get_queued_status(self, empty: "Empty") -> AsyncIterator["GetQueuedStatusResponse"]:
        while True:
            status = await self.fun_task_manager.get_queued_status(.3)
            if status is None:
                return
            try:
                yield GetQueuedStatusResponse(StatusReport(
                    worker_uuid=status.worker_uuid,
                    task_uuid=status.task_uuid,
                    serialized_content=dill.dumps(status.content),
                    create_timestamp=status.create_timestamp,
                    **core_status2rpc_status(status.status)
                ))
            except asyncio_exceptions.TimeoutError:
                ...


class ManagerServiceRunner:
    @inject
    def __init__(
            self,
            fun_task_manager: FunTaskManager = Provide['task_worker_manager.fun_task_manager'],
            host: str = Provide['task_worker_manager.rpc.host'],
            port: int = Provide['task_worker_manager.rpc.port']
    ):
        self.fun_task_manager = fun_task_manager
        self.host = host
        self.port = port

    async def run(self):
        server = Server([ManagerService(
            self.fun_task_manager
        )])
        logger.opt(colors=True).info(
            "starting grpc service <cyan>{host}:{port}</cyan>",
            host=self.host,
            port=self.port
        )
        await server.start(self.host, self.port)
        logger.info("service start successful")
        await server.wait_closed()
