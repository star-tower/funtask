from datetime import datetime
from typing import List, cast
from functools import lru_cache
from funtask.generated import scheduler as scheduler_rpc
from funtask import generated as rpc_types
from grpclib.client import Channel

from funtask.core import interface_and_types as interface, entities


@lru_cache(256)
def _gen_stub(node: entities.SchedulerNode) -> scheduler_rpc.SchedulerStub:
    channel = Channel(
        host=node.host,
        port=node.port
    )
    return scheduler_rpc.SchedulerStub(channel)


def _datetime2ms_timestamp(t: datetime) -> int:
    return int(t.timestamp() * 1000)


class LeaderSchedulerGRPC(interface.LeaderSchedulerRPC):
    async def assign_task_to_node(
            self,
            node: entities.SchedulerNode,
            cron_task_uuid: entities.CronTaskUUID | None = None,
            task_uuid: entities.TaskUUID | None = None,
            start_time: datetime | None = None
    ):
        stub = _gen_stub(node)
        await stub.assign_task(scheduler_rpc.AssignTaskRequest(
            cron_task_uuid,
            task_uuid,
            start_time and _datetime2ms_timestamp(start_time)
        ))

    async def get_node_task_list(self, node: entities.SchedulerNode) -> List[entities.CronTaskUUID]:
        stub = _gen_stub(node)
        resp = await stub.get_task_list(rpc_types.Empty())
        return cast(List[entities.CronTaskUUID], resp.cron_tasks_uuid)

    async def remove_task_from_node(
            self,
            node: entities.SchedulerNode,
            cron_task_uuid: entities.CronTaskUUID,
            start_time: datetime | None = None
    ):
        stub = _gen_stub(node)
        await stub.remove_task(scheduler_rpc.RemoveTaskRequest(
            cron_task_uuid,
            _datetime2ms_timestamp(start_time)
        ))
