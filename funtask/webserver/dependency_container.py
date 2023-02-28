from typing import List, Dict

from dependency_injector import containers, providers

from funtask.common.common import list_dict2nodes
from funtask.core import entities, interface_and_types as interface
from funtask.providers.leader_scheduler.grpc_leader_scheduler import LeaderSchedulerGRPC
from funtask.providers.leader_scheduler_control.static_control import StaticSchedulerControl
from funtask.providers.manager_control.static_control import StaticManagerControl
from funtask.providers.rpc_selector.hash_selector import HashRPSelector
from funtask.task_worker_manager import manager_rpc_client
from funtask.providers.db.sql.infrastructure import Repository


def generate_nodes(nodes: List[Dict]) -> List[entities.SchedulerNode]:
    return [entities.SchedulerNode(
        **node
    ) for node in nodes]


class WebServerContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    repository: interface.Repository = providers.Selector(
        config.repository.type,
        sql=providers.Singleton(
            Repository,
            uri=config.repository.uri
        )
    )
    scheduler_rpc = providers.Singleton(
        LeaderSchedulerGRPC
    )
    scheduler_control = providers.Selector(
        config.scheduler_control.type,
        static=providers.Singleton(
            StaticSchedulerControl,
            leader_node=providers.Factory(
                entities.SchedulerNode,
                port=config.scheduler_control.leader_node.port,
                host=config.scheduler_control.leader_node.host,
                uuid=config.scheduler_control.leader_node.uuid,
            ),
            worker_nodes=providers.Factory(
                generate_nodes,
                config.scheduler_control.worker_nodes
            )
        )
    )
    rpc_selector = providers.Selector(
        config.rpc_selector.type,
        hash=providers.Singleton(
            HashRPSelector
        )
    )
    manager_control = providers.Selector(
        config.manager_control.type,
        static=providers.Singleton(
            StaticManagerControl,
            nodes=providers.Factory(
                list_dict2nodes,
                config.manager_control.nodes
            )
        )
    )
    manager_rpc = providers.Singleton(
        manager_rpc_client.ManagerRPCClient,
        rpc_selector=rpc_selector,
        manager_control=manager_control
    )
    service = config.service
