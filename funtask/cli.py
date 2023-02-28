import sys
import time

import fire as fire
from funtask.scheduler.scheduler_service import SchedulerServiceRunner
from funtask.webserver import webserver_service
from funtask.dependency_container import DependencyContainer
from funtask.core import interface_and_types as interface
from loguru import logger

from funtask.task_worker_manager.manager_service import ManagerServiceRunner

logger.remove()
logger.add(
    sys.stdout,
    format="<blue>{time:YYYY-MM-DD at HH:mm:ss}</blue> <green>{message}</green>",
    level="INFO",
    colorize=True
)


class FormatException(Exception):
    ...


def load_config(config_path: str, container: DependencyContainer):
    match config_path.split('.')[-1].lower():
        case 'yaml' | 'yml':
            container.config.from_yaml(config_path, required=True)
        case 'ini':
            container.config.from_ini(config_path)
        case 'json':
            container.config.from_json(config_path)
        case format_:
            raise FormatException(f'cannot parse config format "{format_}"')


def gen_container(config_path: str, scope: str) -> DependencyContainer:
    container = DependencyContainer()
    logger.info("loading config '{config}'", config=config_path)
    container.config.from_dict({
        'scope': scope
    })
    load_config(config_path, container)
    return container


class TaskWorkerManager:
    @staticmethod
    @logger.catch
    async def run(config: str):
        container = gen_container(config, 'task_worker_manager')
        container.wire(modules=[
            'funtask.task_worker_manager.manager_service'
        ])
        rpc_service = ManagerServiceRunner()
        await rpc_service.run()


class Scheduler:
    @staticmethod
    @logger.catch
    async def run(
            config: str,
            scope: str | None = None
    ):
        """
        run a scheduler according to config
        :param config: config file path
        :type config: str
        :param scope: config scope in config file, for same config contains multiple config
        :type scope: str
        """
        container = gen_container(config, scope or 'scheduler')

        container.wire(modules=[
            'funtask.scheduler.scheduler_service',
            'funtask.core.scheduler'
        ])
        s = SchedulerServiceRunner()
        await s.run()


class WebServer:
    @staticmethod
    @logger.catch
    def run(config: str):
        container = gen_container(config, 'webserver')
        container.wire(modules=[
            'funtask.webserver.webserver_service'
        ])
        server = webserver_service.Webserver()
        server.run()


class Funtask:
    task_worker_manager = TaskWorkerManager()
    scheduler = Scheduler()
    webserver = WebServer()

    @staticmethod
    async def init(config: str):
        """
        initialize the database in config
        :param config: config file path
        :type config: str
        """
        logger.warning('this will truncate all related table, may cause data loss')
        time.sleep(1)
        agree = input('Continue? [Y]/N') or 'Y'
        if agree.lower() != 'y':
            print(f'user input {agree}, cancel.')

        # user agree
        # init
        logger.info('preparing...')
        container = gen_container(config, 'webserver')
        repo: interface.Repository = container.webserver().repository()
        manager_logger: interface.Logger = gen_container(config, 'task_worker_manager').task_worker_manager().logger()

        # drop all table
        logger.info('drop all tables')
        await repo.drop_model_schema()
        await manager_logger.drop_model_schema()

        # create all table
        logger.info('creating tables according schema')
        await repo.create_model_schema()
        await manager_logger.create_model_schema()

        logger.info('done.')


if __name__ == '__main__':
    fire.Fire(Funtask)
