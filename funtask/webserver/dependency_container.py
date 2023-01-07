from typing import List, Dict, cast
from dependency_injector import containers, providers
from funtask.core import entities
from funtask.task_worker_manager import manager_rpc_client
from funtask.providers.db.sql.infrastructure import Repository


def nodes_generator(nodes: List[Dict[str, str | int]]):
    return [entities.TaskWorkerManagerNode(
        uuid=cast(entities.TaskWorkerManagerNodeUUID, node['uuid']),
        host=node['host'],
        port=node['port']
    ) for node in nodes]


class WebServerContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    repository = providers.Singleton(
        Repository,
        uri=config.repository.uri
    )
    rpc_chooser = providers.Selector(
        config.rpc_chooser.type,
        hash=providers.Singleton(
            manager_rpc_client.HashRPChooser,
            nodes=providers.Factory(
                nodes_generator,
                config.task_worker_manager_nodes
            )
        )
    )
    manager_rpc = providers.Singleton(
        manager_rpc_client.ManagerRPCClient,
        rpc_chooser=rpc_chooser
    )
    service = config.service
