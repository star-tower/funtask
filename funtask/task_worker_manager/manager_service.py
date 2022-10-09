from typing import AsyncIterator

import dill
from dependency_injector.wiring import Provide, inject
from grpclib.server import Server
from loguru import logger

from funtask.common.grpc import load_args, core_status2rpc_status
from funtask.core.task_worker_manager import FunTaskManager
from funtask.generated.manager import TaskWorkerManagerBase, IncreaseWorkerRequest, \
    IncreaseWorkersRequest, IncreaseWorkerResponse, IncreaseWorkersResponse, DispatchFunTaskResponse, \
    DispatchFunTaskRequest, StatusReport, StopTaskRequest, StopWorkerRequest, \
    Empty, KillWorkerRequest, GetQueuedStatusResponse, Worker, Task


class ManagerService(TaskWorkerManagerBase):
    def __init__(self, fun_task_manager: FunTaskManager):
        self.fun_task_manager = fun_task_manager

    async def increase_workers(self, increase_workers_request: "IncreaseWorkersRequest") -> "IncreaseWorkersResponse":
        args, kwargs = load_args(increase_workers_request.other_args)
        workers = await self.fun_task_manager.increase_workers(
            increase_workers_request.number,
            *args,
            **kwargs
        )
        return IncreaseWorkersResponse(
            [Worker(worker.uuid) for worker in workers]
        )

    async def increase_worker(self, increase_worker_request: "IncreaseWorkerRequest") -> "IncreaseWorkerResponse":
        args, kwargs = load_args(increase_worker_request.other_args)
        return IncreaseWorkerResponse(
            Worker((await self.fun_task_manager.increase_worker(*args, **kwargs)).uuid)
        )

    async def dispatch_fun_task(self, dispatch_fun_task_request: "DispatchFunTaskRequest") -> "DispatchFunTaskResponse":
        args, kwargs = load_args(dispatch_fun_task_request.other_args)
        task = await self.fun_task_manager.dispatch_fun_task(
            dispatch_fun_task_request.worker_uuid,
            dill.loads(dispatch_fun_task_request.serialized_fun_task),
            dispatch_fun_task_request.change_status,
            dispatch_fun_task_request.timeout,
            *args,
            **kwargs
        )
        return DispatchFunTaskResponse(Task(task.uuid))

    async def stop_task(self, stop_task_request: "StopTaskRequest") -> "Empty":
        await self.fun_task_manager.stop_task(stop_task_request.worker_uuid, stop_task_request.task_uuid)
        return Empty()

    async def stop_worker(self, stop_worker_request: "StopWorkerRequest") -> "Empty":
        await self.fun_task_manager.stop_worker(stop_worker_request.worker_uuid)
        return Empty()

    async def kill_worker(self, kill_worker_request: "KillWorkerRequest") -> "Empty":
        await self.fun_task_manager.kill_worker(kill_worker_request.worker_uuid)
        return Empty()

    async def get_queued_status(self, empty: "Empty") -> AsyncIterator["GetQueuedStatusResponse"]:
        while True:
            status = await self.fun_task_manager.get_queued_status()
            yield GetQueuedStatusResponse(StatusReport(
                worker_uuid=status.worker_uuid,
                task_uuid=status.task_uuid,
                serialized_content=dill.dumps(status.content),
                create_timestamp=status.create_timestamp,
                **core_status2rpc_status(status.status)
            ))


class ManagerServiceRunner:
    @inject
    def __init__(
            self,
            fun_task_manager: FunTaskManager = Provide['task_worker_manager.fun_task_manager'],
            address: str = Provide['task_worker_manager.rpc_address'],
            port: int = Provide['task_worker_manager.rpc_port']
    ):
        self.fun_task_manager = fun_task_manager
        self.address = address
        self.port = port

    async def run(self):
        server = Server([ManagerService(
            self.fun_task_manager
        )])
        logger.opt(colors=True).info(
            "starting grpc service <cyan>{address}:{port}</cyan>",
            address=self.address,
            port=self.port
        )
        await server.start(self.address, self.port)
        logger.info("service start successful")
        await server.wait_closed()
