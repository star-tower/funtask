from dependency_injector import containers, providers
from funtask.scheduler.scheduler_service import SchedulerService
from funtask.providers.queue.multiprocessing_queue import MultiprocessingQueueFactory


class SchedulerContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    scheduler = providers.Singleton(
        SchedulerService
    )
