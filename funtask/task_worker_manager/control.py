from typing import List

from funtask.core import interface_and_types as interface, entities


class StaticNodeControl(interface.NodeControl):
    def __init__(self, nodes: List[entities.TaskWorkerManagerNode]):
        self.nodes = nodes

    async def get_all_nodes(self) -> List[entities.TaskWorkerManagerNode]:
        return self.nodes
