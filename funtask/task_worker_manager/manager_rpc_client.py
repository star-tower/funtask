from typing import List, cast, AsyncIterator, Any
from uuid import uuid4
from grpclib.client import Channel

from funtask.generated import manager as task_worker_manager_rpc
from funtask import generated as types
from funtask.core import interface_and_types as interface, entities
from funtask.core.interface_and_types import StatusReport


def bytes_uuid() -> bytes:
    return uuid4().hex.encode()


async def get_rpc(
        chooser: interface.RPCChannelChooser[Channel],
        key: bytes | None = None
) -> task_worker_manager_rpc.TaskWorkerManagerStub:
    channel = await chooser.get_channel(key)
    return task_worker_manager_rpc.TaskWorkerManagerStub(channel)


class HashRPRChooser(interface.RPCChannelChooser[Channel]):
    def __init__(self, nodes: List[entities.TaskWorkerManagerNode | entities.SchedulerNode]):
        self.nodes = nodes

    def channel_node_changes(self, nodes: List[entities.TaskWorkerManagerNode | entities.SchedulerNode]):
        self.nodes = nodes

    async def get_channel(self, key: bytes | None = None) -> Channel:
        node_idx = hash(key) % len(self.nodes)
        node = self.nodes[node_idx]
        return Channel(host=node.host, port=node.port)


class ManagerRPCClient(interface.FunTaskManagerRPC):
    def __init__(self, rpc_chooser: interface.RPCChannelChooser[Channel]):
        self.rpc_chooser = rpc_chooser

    async def increase_workers(self, number: int | None = None) -> List[entities.WorkerUUID]:
        rpc = await get_rpc(self.rpc_chooser, bytes_uuid())
        res = await rpc.increase_workers(task_worker_manager_rpc.IncreaseWorkersRequest(number))
        return [cast(worker.uuid, entities.WorkerUUID) for worker in res.workers]

    async def increase_worker(self) -> entities.WorkerUUID:
        rpc = await get_rpc(self.rpc_chooser, bytes_uuid())
        res = await rpc.increase_worker(task_worker_manager_rpc.IncreaseWorkerRequest())
        return cast(res.worker.uuid, entities.WorkerUUID)

    async def dispatch_fun_task(self, worker_uuid: entities.WorkerUUID, func_task: bytes, dependencies: List[str],
                                change_status: bool, timeout: float,
                                argument: entities.FuncArgument | None) -> entities.TaskUUID:
        rpc = await get_rpc(self.rpc_chooser, worker_uuid.encode())
        res = await rpc.dispatch_fun_task(task_worker_manager_rpc.DispatchFunTaskRequest(
            worker_uuid,
            func_task,
            dependencies,
            change_status,
            timeout,
            argument and types.Args(
                argument.args,
                [types.KwArgs(k, v) for k, v in argument.kwargs]
            )
        ))
        return cast(res.task.uuid, entities.TaskUUID)

    async def stop_task(self, worker_uuid: entities.WorkerUUID, task_uuid: entities.TaskUUID):
        rpc = await get_rpc(self.rpc_chooser, (worker_uuid + task_uuid).encode())
        await rpc.stop_task(task_worker_manager_rpc.StopTaskRequest(
            worker_uuid,
            task_uuid
        ))

    async def stop_worker(self, worker_uuid: entities.WorkerUUID):
        rpc = await get_rpc(self.rpc_chooser, worker_uuid.encode())
        await rpc.stop_worker(task_worker_manager_rpc.StopWorkerRequest(
            worker_uuid
        ))

    async def kill_worker(self, worker_uuid: entities.WorkerUUID):
        rpc = await get_rpc(self.rpc_chooser, worker_uuid.encode())
        await rpc.kill_worker(task_worker_manager_rpc.KillWorkerRequest(worker_uuid))

    async def get_queued_status(self, timeout: None | float = None) -> AsyncIterator[StatusReport]:
        rpc = await get_rpc(self.rpc_chooser, bytes_uuid())
        try:
            async for res in rpc.get_queued_status(task_worker_manager_rpc.Empty(), timeout=timeout):
                status_report = res.status_report
                yield StatusReport(
                    task_uuid=cast(status_report.task_uuid, entities.TaskUUID),
                    worker_uuid=cast(status_report.worker_uuid, entities.WorkerUUID),
                    status=status_report.task_status or status_report.worker_status,  # type: ignore
                    content=status_report.serialized_content,
                    create_timestamp=status_report.create_timestamp
                )
        except:
            yield None

    async def get_task_queue_size(self, worker: entities.WorkerUUID) -> int:
        rpc = await get_rpc(self.rpc_chooser, worker.encode())
        raise NotImplementedError('get task queue size not impl')
