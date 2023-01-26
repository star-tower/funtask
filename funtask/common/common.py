from typing import List, Dict, cast
from funtask.core import entities


def list_dict2nodes(nodes: List[Dict[str, str | int]]):
    return [entities.TaskWorkerManagerNode(
        uuid=cast(entities.TaskWorkerManagerNodeUUID, node['uuid']),
        host=node['host'],
        port=node['port']
    ) for node in nodes]
