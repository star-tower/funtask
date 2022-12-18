from typing import List, Tuple, cast
from grpclib.client import Channel
from funtask.core import interface_and_types as interface, entities
from dependency_injector.wiring import inject, Provide
from fastapi import FastAPI, APIRouter

app = FastAPI()
api = APIRouter(prefix='api')


class Webserver(interface.WebServer):
    @inject
    def __init__(
            self,
            rpc_channel_chooser: interface.RPCChannelChooser[Channel] = Provide['webserver.rpc_channel_chooser'],
            repository: interface.Repository = Provide['repository'],
            task_worker_manager_rpc: interface.FunTaskManagerRPC = Provide['task_worker_manager_rpc'],

    ):
        self.channel_chooser = rpc_channel_chooser
        self.repository = repository
        self.task_worker_manager_rpc = task_worker_manager_rpc

    @api.post('increase_worker')
    async def increase_worker(self, name: str, tags: List[str]) -> entities.Worker:
        worker_uuid = await self.task_worker_manager_rpc.increase_worker()
        worker = entities.Worker(
            uuid=worker_uuid,
            status=entities.WorkerStatus.RUNNING,
            name=name,
            tags=tags
        )
        await self.repository.add_worker(worker)
        return worker

    async def trigger_func(self, func: entities.Func, argument: entities.FuncArgument) -> entities.Task:
        pass

    async def get_task_by_uuid(self, task_uuid: entities.TaskUUID) -> entities.Task | None:
        pass

    async def get_tasks(
            self,
            tags: List[str] | None = None,
            func: entities.FuncUUID | None = None,
            cursor: entities.TaskQueryCursor | None = None,
            limit: int | None = None
    ) -> Tuple[List[entities.Task], entities.TaskQueryCursor]:
        pass

    async def get_funcs(self, tags: List[str] | None = None, include_tmp: bool = False) -> List[entities.Func]:
        pass

    async def trigger_func_group(self, func_group: entities.FuncGroup,
                                 argument_group: entities.FuncArgumentGroup) -> entities.TaskGroupUUID:
        pass

    async def add_func_group(self, func_group: entities.FuncGroup) -> entities.FuncGroupUUID:
        pass

    async def trigger_repeated_func(self, time_points: entities.TimePoint, func: entities.Func,
                                    argument: entities.FuncArgument) -> entities.CronTaskUUID:
        pass

    async def trigger_repeated_func_group(self, time_points: entities.TimePoint, func_group: entities.FuncGroup,
                                          argument_group: entities.FuncArgumentGroup) -> entities.CronTaskUUID:
        pass

    async def add_func(self, func: entities.Func) -> entities.FuncUUID:
        pass

    async def add_parameter_schema(self, parameter_schema: entities.ParameterSchema) -> entities.ParameterSchemaUUID:
        pass
