from dependency_injector import containers, providers
from funtask.providers.db.sql.infrastructure import Repository


class WebServerContainer(containers):
    config = providers.Configuration()
    repository = providers.Singleton(
        Repository,
        **config.get('repository')
    )
    rpc_channel_chooser = providers.Singleton(

    )
