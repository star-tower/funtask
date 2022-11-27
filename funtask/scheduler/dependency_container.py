from dependency_injector import containers, providers

from funtask.providers.queue.multiprocessing_queue import MultiprocessingQueueFactory


class SchedulerContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    # TODO: config
