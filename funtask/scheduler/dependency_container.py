from typing import Dict, List

from dependency_injector import containers, providers

from funtask.common.common import list_dict2nodes
from funtask.core.scheduler import Scheduler
from funtask.providers.leader_scheduler.grpc_leader_scheduler import LeaderSchedulerGRPC
from funtask.providers.db.sql.infrastructure import Repository
from funtask.providers.leader_scheduler_control.static_control import StaticSchedulerControl
from funtask.core import entities
from funtask.providers.cron.schedule_cron import SchedulerCron
from funtask.providers.manager_control.static_control import StaticManagerControl
from funtask.providers.queue.multiprocessing_queue import MultiprocessingQueueFactory
from funtask.providers.lock.multiprocessing_lock import MultiprocessingLock
from funtask.providers.rpc_selector.hash_selector import HashRPSelector
from funtask.task_worker_manager import manager_rpc_client


def generate_nodes(nodes: List[Dict]) -> List[entities.SchedulerNode]:
    return [entities.SchedulerNode(
        **node
    ) for node in nodes]


class SchedulerContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    node = providers.Factory(
        entities.SchedulerNode,
        uuid=config.curr_node.uuid,
        host=config.curr_node.host,
        port=config.curr_node.port
    )
    rpc_selector = providers.Selector(
        config.rpc_selector.type,
        hash=providers.Singleton(
            HashRPSelector
        )
    )
    manager_control = providers.Selector(
        config.manager_control.type,
        static=providers.Singleton(
            StaticManagerControl,
            nodes=providers.Factory(
                list_dict2nodes,
                config.manager_control.nodes
            )
        )
    )
    manager_rpc = providers.Singleton(
        manager_rpc_client.ManagerRPCClient,
        rpc_selector=rpc_selector,
        manager_control=manager_control
    )
    repository = providers.Singleton(
        Repository,
        uri=config.repository.uri
    )
    cron = providers.Selector(
        config.cron_scheduler.type,
        schedule=providers.Factory(
            SchedulerCron
        )
    )
    argument_queue_factory = providers.Selector(
        config.argument_queue.type,
        multiprocessing=providers.Factory(
            MultiprocessingQueueFactory,
            host=config.argument_queue.host,
            port=config.argument_queue.port
        )
    )
    lock = providers.Selector(
        config.lock.type,
        multiprocessing=providers.Singleton(
            MultiprocessingLock
        )
    )
    leader_scheduler_rpc = providers.Singleton(
        LeaderSchedulerGRPC
    )
    leader_control = providers.Selector(
        config.scheduler_control.type,
        static=providers.Singleton(
            StaticSchedulerControl,
            leader_node=providers.Factory(
                entities.SchedulerNode,
                port=config.scheduler_control.leader_node.port,
                host=config.scheduler_control.leader_node.host,
                uuid=config.scheduler_control.leader_node.uuid,
            ),
            worker_nodes=providers.Factory(
                generate_nodes,
                config.scheduler_control.worker_nodes
            )
        )
    )
    scheduler = providers.Singleton(
        Scheduler
    )
