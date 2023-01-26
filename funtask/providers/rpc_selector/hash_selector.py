from grpclib.client import Channel

from funtask.core import interface_and_types as interface, entities
from typing import TypeVar, List

from funtask.core.interface_and_types import NoNodeException

_Channel = TypeVar('_Channel')

_Node = entities.TaskWorkerManagerNodeUUID | entities.SchedulerNode


class HashRPSelector(interface.RPCChannelSelector[_Channel]):
    def __init__(self):
        self.nodes: List[_Node] = []

    def channel_node_changes(self, nodes: List[_Node]):
        self.nodes = nodes

    async def get_channel(self, key: bytes | None = None) -> _Channel:
        if not len(self.nodes):
            raise NoNodeException('no rpc node for channel')

        node_idx = hash(key) % len(self.nodes)
        node = self.nodes[node_idx]
        return Channel(host=node.host, port=node.port)
