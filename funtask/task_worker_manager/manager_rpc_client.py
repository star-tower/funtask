import asyncio
import time
import traceback
from typing import List, cast, AsyncIterator, Dict
from uuid import uuid4

import grpclib.exceptions as grpc_exceptions
from grpclib.client import Channel
from asyncio.exceptions import TimeoutError as AsyncTimeoutError
import contextlib

from grpclib.metadata import Deadline
from loguru import logger

from funtask.generated import manager as task_worker_manager_rpc
from funtask.generated import Empty
from funtask import generated as types
from funtask.core import interface_and_types as interface, entities
from funtask.core.interface_and_types import StatusReport


def bytes_uuid() -> bytes:
    return uuid4().hex.encode()


@contextlib.asynccontextmanager
async def get_rpc(
        selector: interface.RPCChannelSelector[Channel],
        key: bytes | None = None
) -> task_worker_manager_rpc.TaskWorkerManagerStub:
    channel = await selector.get_channel(key)
    try:
        yield task_worker_manager_rpc.TaskWorkerManagerStub(
            channel
        )
    except Exception as e:
        raise e
    finally:
        channel.close()


class ManagerRPCClient(interface.FunTaskManagerRPC):
    def __init__(
            self,
            rpc_selector: interface.RPCChannelSelector[Channel],
            manager_control: interface.ManagerNodeControl
    ):
        self.rpc_selector = rpc_selector
        self.node_update_lock = asyncio.Lock()
        self.latest_selector_node_update = -1
        self.manager_control = manager_control

    async def update_selector_nodes(self):
        async with self.node_update_lock:
            curr_time = time.time()
            if self.latest_selector_node_update < curr_time - 1:
                nodes = await self.manager_control.get_all_nodes()
                self.rpc_selector.channel_node_changes(nodes)
                self.latest_selector_node_update = curr_time

    async def increase_workers(self, number: int | None = None) -> List[entities.WorkerUUID]:
        await self.update_selector_nodes()
        async with get_rpc(self.rpc_selector, bytes_uuid()) as rpc:
            res = await rpc.increase_workers(task_worker_manager_rpc.IncreaseWorkersRequest(number))
            return [cast(entities.WorkerUUID, worker.uuid) for worker in res.workers]

    async def increase_worker(self) -> entities.WorkerUUID:
        await self.update_selector_nodes()
        async with get_rpc(self.rpc_selector, bytes_uuid()) as rpc:
            res = await rpc.increase_worker(task_worker_manager_rpc.IncreaseWorkerRequest())
            return cast(entities.WorkerUUID, res.worker.uuid)

    async def dispatch_fun_task(self, worker_uuid: entities.WorkerUUID, func_task: bytes, dependencies: List[str],
                                change_status: bool, timeout: float,
                                argument: entities.FuncArgument | None) -> entities.TaskUUID:
        await self.update_selector_nodes()
        async with get_rpc(self.rpc_selector, bytes_uuid()) as rpc:
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
            return cast(entities.TaskUUID, res.task.uuid)

    async def stop_task(self, worker_uuid: entities.WorkerUUID, task_uuid: entities.TaskUUID):
        await self.update_selector_nodes()
        async with get_rpc(self.rpc_selector, bytes_uuid()) as rpc:
            await rpc.stop_task(task_worker_manager_rpc.StopTaskRequest(
                worker_uuid,
                task_uuid
            ))

    async def stop_worker(self, worker_uuid: entities.WorkerUUID):
        await self.update_selector_nodes()
        async with get_rpc(self.rpc_selector, bytes_uuid()) as rpc:
            await rpc.stop_worker(task_worker_manager_rpc.StopWorkerRequest(
                worker_uuid
            ))

    async def kill_worker(self, worker_uuid: entities.WorkerUUID):
        await self.update_selector_nodes()
        async with get_rpc(self.rpc_selector, bytes_uuid()) as rpc:
            await rpc.kill_worker(task_worker_manager_rpc.KillWorkerRequest(worker_uuid))

    async def get_queued_status(self, timeout: None | float = None) -> AsyncIterator[StatusReport]:
        await self.update_selector_nodes()
        async with get_rpc(self.rpc_selector, bytes_uuid()) as rpc:
            try:
                async for res in rpc.get_queued_status(Empty(), timeout=timeout):
                    status_report = res.status_report
                    status_report_dict = res.status_report.to_dict()

                    yield StatusReport(
                        task_uuid=cast(entities.TaskUUID, status_report.task_uuid),
                        worker_uuid=cast(entities.WorkerUUID, status_report.worker_uuid),
                        status=get_report_entity_status(status_report_dict),
                        content=status_report.serialized_content,
                        create_timestamp=status_report.create_timestamp
                    )
            except AsyncTimeoutError:
                ...
            except grpc_exceptions.GRPCError as e:
                if e.status == grpc_exceptions.Status.DEADLINE_EXCEEDED:
                    ...
                else:
                    logger.error(str(e) + '\n' + traceback.format_exc())

    async def get_task_queue_size(self, worker: entities.WorkerUUID) -> int:
        await self.update_selector_nodes()
        async with get_rpc(self.rpc_selector, bytes_uuid()) as rpc:
            raise NotImplementedError('get task queue size not impl')


def get_report_entity_status(report_dict: Dict) -> entities.TaskStatus | entities.WorkerStatus | None:
    if 'workerStatus' in report_dict:
        status = report_dict['workerStatus']
        if status == 'HEARTBEAT':
            return None
        return entities.WorkerStatus(status)
    if 'taskStatus' in report_dict:
        return entities.TaskStatus(report_dict['taskStatus'])
    raise ValueError(f'either `workerStatus` nor `taskStatus` in {report_dict}')
