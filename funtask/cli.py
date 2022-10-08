import fire as fire

from funtask.dependency_container import DependencyContainer


class FormatException(Exception):
    ...


class Funtask:
    @staticmethod
    async def task_worker_manager(config: str):
        container = DependencyContainer()
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
        await container.task_worker_manager_service().run()


if __name__ == '__main__':
    fire.Fire(Funtask)
