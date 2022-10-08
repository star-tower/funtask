from dependency_injector import containers, providers

from funtask.core.task_worker_manager import FunTaskManager
from funtask.providers.loggers.std import StdLogger
from funtask.providers.queue.multiprocessing_queue import MultiprocessingQueueFactory
from funtask.providers.worker_manager.multiprocessing_manager import MultiprocessingManager

_queue_factories = {
    'multiprocessing': providers.Object(MultiprocessingQueueFactory().factory)
}


class TaskWorkerManagerContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    rpc_address = config.service.address
    rpc_port = config.service.port
    task_status_queue = providers.Singleton(
        providers.Selector(
            config.get('queue.task.type') or config.queue.type,
            multiprocessing=providers.Factory(
                MultiprocessingQueueFactory().factory,
                name="task_queue"
            )
        )
    )
    logger = providers.Selector(
        config.logger.type,
        std=providers.Factory(StdLogger)
    )
    fun_task_manager = providers.Factory(
        FunTaskManager,
        worker_manager=providers.FactoryAggregate(
            multiprocessing=providers.Factory(
                MultiprocessingManager,
                logger=logger,
                task_queue_factory=providers.Selector(
                    config.queue.type,
                    **_queue_factories
                ),
                control_queue_factory=providers.Selector(
                    config.queue.type,
                    **_queue_factories
                ),
                task_status_queue=task_status_queue
            )
        ),
        task_status_queue=task_status_queue
    )
