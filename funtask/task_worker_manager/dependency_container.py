from dependency_injector import containers, providers
from funtask.core.task_worker_manager import FunTaskManager
from funtask.providers.loggers.rotate_file import RotateFileLogger
from funtask.providers.loggers.sql_infrastructure import Repository as RotateFileRepository
from funtask.providers.queue.multiprocessing_queue import MultiprocessingQueueFactory
from funtask.providers.worker_manager.multiprocessing_manager import MultiprocessingManager

config = providers.Configuration()

_queue_factories = {
    'multiprocessing': providers.Singleton(
        MultiprocessingQueueFactory,
        host=config.queue.host,
        port=config.queue.port
    )
}


class TaskWorkerManagerContainer(containers.DeclarativeContainer):
    config = config
    rpc = config.rpc
    task_status_queue = providers.Singleton(
        providers.Selector(
            config.queue.type,
            multiprocessing=providers.Factory(
                lambda host, port, manager_uuid: MultiprocessingQueueFactory(host, port)(
                    f"task_status_queue-{manager_uuid}"
                ),
                host=config.queue.host,
                port=config.queue.port,
                manager_uuid=config.rpc.uuid
            )
        )
    )
    logger = providers.Selector(
        config.logger.type,
        file=providers.Factory(
            RotateFileLogger,
            repository=providers.Selector(
                config.logger.repository.type,
                sql=providers.Singleton(
                    RotateFileRepository,
                    uri=config.logger.repository.uri
                )
            ),
            log_dir=config.logger.log_dir,
            rotate_line_num=config.logger.rotate_line_num
        )
    )
    fun_task_manager = providers.Factory(
        FunTaskManager,
        worker_manager=providers.Selector(
            config.manager.type,
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
