from typing import List

from funtask.core import interface_and_types as interface, entities
from funtask.core.interface_and_types import StatusReport


class ManagerRPCClient(interface.FunTaskManagerRPC):

    async def increase_workers(self, number: int | None = None) -> List[entities.WorkerUUID]:
        pass

    async def increase_worker(self) -> entities.WorkerUUID:
        pass

    async def dispatch_fun_task(self, worker_uuid: entities.WorkerUUID, func_task: bytes, dependencies: List[str],
                                change_status: bool, timeout: float,
                                argument: entities.FuncArgument | None) -> entities.TaskUUID:
        pass

    async def stop_task(self, worker_uuid: entities.WorkerUUID, task_uuid: entities.TaskUUID):
        pass

    async def stop_worker(self, worker_uuid: entities.WorkerUUID):
        pass

    async def kill_worker(self, worker_uuid: entities.WorkerUUID):
        pass

    async def get_queued_status(self, timeout: None | float = None) -> StatusReport | None:
        pass

    async def get_task_queue_size(self, worker: entities.WorkerUUID) -> int:
        pass
