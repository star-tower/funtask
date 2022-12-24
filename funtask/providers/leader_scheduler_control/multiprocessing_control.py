from typing import List, cast

from dependency_injector.wiring import Provide
from funtask.core import interface_and_types as interface, entities


class MultiprocessingSchedulerControl(interface.LeaderSchedulerControl):
    def __init__(
            self,
            leader_node: entities.SchedulerNode = Provide['scheduler_control.leader_node']
    ):
        self.leader_node = leader_node

    async def get_leader(self) -> entities.SchedulerNode | None:
        return self.leader_node

    async def elect_leader(self, uuid: entities.SchedulerNodeUUID) -> bool:
        return uuid == self.leader_node.uuid

    async def is_he_leader(self, uuid: entities.SchedulerNodeUUID) -> bool:
        return uuid == self.leader_node.uuid

    async def get_all_nodes(self) -> List[entities.SchedulerNode]:
        ...

    async def get_cluster_id(self) -> entities.ClusterUUID:
        return cast(entities.ClusterUUID, 'multiprocessing')
