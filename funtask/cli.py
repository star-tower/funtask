import sys

import fire as fire

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


class TaskWorkerManager:
    @staticmethod
    @logger.catch
    async def run(config: str):
        container = DependencyContainer()
        logger.info("loading config '{config}'", config=config)
        match config.split('.')[-1].lower():
            case 'yaml' | 'yml':
                container.config.from_yaml(config, required=True)
            case 'ini':
                container.config.from_ini(config)
            case 'json':
                container.config.from_json(config)
            case format_:
                raise FormatException(f'cannot parse config format "{format_}"')
        container.wire(modules=[
            'funtask.task_worker_manager.manager_service'
        ])
        rpc_service = container.task_worker_manager_service()
        await rpc_service.run()


class Funtask:
    task_worker_manager = TaskWorkerManager()


if __name__ == '__main__':
    fire.Fire(Funtask)
