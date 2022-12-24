from dependency_injector import containers, providers

from funtask.providers.leader_scheduler.grpc_leader_scheduler import GRPCLeaderScheduler
from funtask.providers.leader_scheduler_control.multiprocessing_control import MultiprocessingSchedulerControl
from funtask.scheduler.scheduler_service import SchedulerService
from funtask.core import entities
from funtask.providers.cron.schedule_cron import SchedulerCron
from funtask.providers.queue.multiprocessing_queue import MultiprocessingQueueFactory
from funtask.providers.lock.multiprocessing_lock import MultiprocessingLock
from funtask.providers.db.sql import infrastructure
from funtask.task_worker_manager import manager_rpc_client
from funtask.task_worker_manager.manager_rpc_client import ManagerRPCClient, HashRPChooser


class SchedulerContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    node = providers.Factory(
        entities.SchedulerNode,
        uuid=config.curr_node.uuid,
        host=config.curr_node.host,
        port=config.curr_node.port
    )
    funtask_manager_rpc = providers.Singleton(
        manager_rpc_client.ManagerRPCClient,
        rpc_chooser=providers.Selector(
            config.rpc_chooser,
            hash=providers.Factory(
                manager_rpc_client.HashRPChooser,
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
    leader_scheduler_rpc = providers.Singleton(
        GRPCLeaderScheduler
    )
    leader_control = providers.Selector(
        config.control,
        multiprocessing=providers.Singleton(
            MultiprocessingSchedulerControl,
            config.control.leader_node
        )
    )
    task_manager_rpc = providers.Singleton(
        ManagerRPCClient,
        rpc_chooser=providers.Selector(
            config.rpc_chooser,
            hash=providers.Singleton(
                HashRPChooser
            )
        )
    )
    scheduler = providers.Singleton(
        SchedulerService
    )
