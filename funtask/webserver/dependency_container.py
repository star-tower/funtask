from dependency_injector import containers, providers
from funtask.task_worker_manager import manager_rpc_client
from funtask.providers.db.sql.infrastructure import Repository


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
            nodes=[]
        )
    )
    manager_rpc = providers.Singleton(
        manager_rpc_client.ManagerRPCClient,
        rpc_chooser=rpc_chooser
    )
    service = config.service
