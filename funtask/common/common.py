from collections import OrderedDict
from typing import List, Dict, cast, Any, TypeVar, Generic
from funtask.core import entities


def list_dict2nodes(nodes: List[Dict[str, str | int]]):
    return [entities.TaskWorkerManagerNode(
        uuid=cast(entities.TaskWorkerManagerNodeUUID, node['uuid']),
        host=node['host'],
        port=node['port']
    ) for node in nodes]


_T = TypeVar('_T')


class LRUCache(Generic[_T]):
    """
    >>> cache = LRUCache(2)
    >>> cache.put("2333", "4555")
    >>> cache.get("2333")
    '4555'
    >>> cache.put("6777", "8999")
    >>> cache.get("6777")
    '8999'
    >>> cache.put("1111", "2222")
    >>> cache.get("2333")
    """
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key: Any) -> _T | None:
        if key not in self.cache:
            return None
        else:
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key: Any, value: _T):
        self.cache[key] = value
        self.cache.move_to_end(key)
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)
