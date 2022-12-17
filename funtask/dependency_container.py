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
        ManagerServiceRunner,
        fun_task_manager=task_worker_manager.fun_task_manager,
        address=task_worker_manager.rpc.address,
        port=task_worker_manager.rpc.port
    )
    scheduler = providers.Container(
        SchedulerContainer,
        config=config.scheduler
    )
