from dependency_injector import providers, containers

from funtask.scheduler.dependency_container import SchedulerContainer
from funtask.task_worker_manager.dependency_container import TaskWorkerManagerContainer
from funtask.webserver.dependency_container import WebServerContainer


class DependencyContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    task_worker_manager = providers.Container(
        TaskWorkerManagerContainer,
        config=config.config[config.scope]
    )
    scheduler = providers.Container(
        SchedulerContainer,
        config=config.config[config.scope]
    )
    webserver = providers.Container(
        WebServerContainer,
        config=config.config[config.scope]
    )
