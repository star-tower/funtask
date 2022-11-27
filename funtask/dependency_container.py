from dependency_injector import providers, containers

from funtask.scheduler.dependency_container import SchedulerContainer
from funtask.task_worker_manager.dependency_container import TaskWorkerManagerContainer
from funtask.task_worker_manager.manager_service import ManagerServiceRunner


class DependencyContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    task_worker_manager = providers.Container(
        TaskWorkerManagerContainer,
        config=config.task_worker_manager,
    )
    task_worker_manager_service = providers.Singleton(
        ManagerServiceRunner
    )
    scheduler = providers.Container(
        SchedulerContainer,
        config=config.scheduler
    )
