from dependency_injector import containers, providers
from funtask.scheduler.scheduler_service import SchedulerService
from funtask.core import entities
from funtask.providers.cron.schedule import SchedulerCron
from funtask.providers.queue.multiprocessing_queue import MultiprocessingQueueFactory
from funtask.providers.lock.multiprocessing_lock import MultiprocessingLock
from funtask.providers.db.sql import infrastructure
from funtask.task_worker_manager import manager_rpc_client


class SchedulerContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    node = providers.Factory(
        entities.SchedulerNode,
        uuid=config.curr_node.uuid,
        ip=config.curr_node.host,
        port=config.curr_node.port
    )
    funtask_manager_rpc = providers.Singleton(
        manager_rpc_client.ManagerRPCClient,
        rpc_chooser=providers.Selector(
            config.rpc_chooser,
            hash=providers.Factory(
                manager_rpc_client.HashRPRChooser,
                nodes=[]
            )
        )
    )
    repository = providers.Singleton(
        infrastructure.Repository,
        uri=config.sql.uri
    )
    cron = providers.Selector(
        config.cron_scheduler.type,
        schedule=providers.Factory(
            SchedulerCron
        )
    )
    argument_queue_factory = providers.Selector(
        config.argument_queue.type,
        multiprocessing=providers.Factory(MultiprocessingQueueFactory)
    )
    lock = providers.Selector(
        config.lock.type,
        multiprocessing=providers.Singleton(
            MultiprocessingLock
        )
    )
    # TODO: scheduler rpc, leader_control, config, manager_rpc
    scheduler = providers.Singleton(
        SchedulerService
    )
