import sys

import fire as fire
from funtask.core import scheduler
from funtask.dependency_container import DependencyContainer
from loguru import logger

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


def gen_container(config_path: str) -> DependencyContainer:
    container = DependencyContainer()
    logger.info("loading config '{config}'", config=config_path)
    load_config(config_path, container)
    return container


class TaskWorkerManager:
    @staticmethod
    @logger.catch
    async def run(config: str):
        container = gen_container(config)
        container.wire(modules=[
            'funtask.task_worker_manager.manager_service'
        ])
        rpc_service = container.task_worker_manager_service()
        await rpc_service.run()


class Scheduler:
    @staticmethod
    @logger.catch
    async def run(config: str):
        container = gen_container(config)
        container.wire(modules=[
            'funtask.scheduler.scheduler_service',
            'funtask.core.scheduler'
        ])
        s = scheduler.Scheduler()
        await s.run()


class WebServer:
    @staticmethod
    @logger.catch
    async def run(config: str):
        container = gen_container(config)


class Funtask:
    task_worker_manager = TaskWorkerManager()
    scheduler = Scheduler()


if __name__ == '__main__':
    fire.Fire(Funtask)
