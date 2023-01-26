from dependency_injector import containers, providers

from funtask.common.common import list_dict2nodes
from funtask.providers.manager_control.static_control import StaticManagerControl
from funtask.providers.rpc_selector.hash_selector import HashRPSelector
from funtask.task_worker_manager import manager_rpc_client
from funtask.providers.db.sql.infrastructure import Repository


class WebServerContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    repository = providers.Singleton(
        Repository,
        uri=config.repository.uri
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
