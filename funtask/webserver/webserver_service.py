import asyncio
import time
from typing import List, Tuple
from grpclib.client import Channel
from funtask.core import interface_and_types as interface, entities
from dependency_injector.wiring import inject, Provide
from fastapi import FastAPI, APIRouter
import uvicorn

from funtask.webserver.model import IncreaseWorkerReq, BatchQueryReq, WorkersWithCursor
from funtask.webserver.utils import self_wrapper, SelfPointer

api = APIRouter(prefix='/api', tags=['api'])
webserver_pointer = SelfPointer()


class Webserver:
    @inject
    def __init__(
            self,
            rpc_channel_selector: interface.RPCChannelSelector[Channel] = Provide['webserver.rpc_selector'],
            repository: interface.Repository = Provide['webserver.repository'],
            task_worker_manager_rpc: interface.FunTaskManagerRPC = Provide['webserver.manager_rpc'],
            manager_control: interface.ManagerNodeControl = Provide['webserver.manager_control'],
            host: str = Provide['webserver.service.host'],
            port: int = Provide['webserver.service.port']
    ):
        self.channel_selector = rpc_channel_selector
        self.repository = repository
        self.task_worker_manager_rpc = task_worker_manager_rpc
        self.host = host
        self.manager_control = manager_control
        self.port = port
        self.latest_selector_node_update = -1
        self.node_update_lock = asyncio.Lock()

    async def update_selector_nodes(self):
        async with self.node_update_lock:
            curr_time = time.time()
            if self.latest_selector_node_update < curr_time - 1:
                nodes = await self.manager_control.get_all_nodes()
                self.channel_selector.channel_node_changes(nodes)
                self.latest_selector_node_update = curr_time

    @api.post('/increase_worker', response_model=List[entities.Worker])
    @self_wrapper(webserver_pointer)
    async def increase_worker(self, req: IncreaseWorkerReq) -> List[entities.Worker]:
        await self.update_selector_nodes()
        try:
            worker_uuids = await self.task_worker_manager_rpc.increase_workers(req.number)
        except interface.NoNodeException:
            raise interface.NoNodeException(f'no task worker manager found')

        worker_entities: List[entities.Worker] = []

        for worker_uuid in worker_uuids:
            worker = entities.Worker(
                uuid=worker_uuid,
                status=entities.WorkerStatus.RUNNING,
                name=req.name,
                tags=req.tags
            )
            await self.repository.add_worker(worker)

            worker_entities.append(worker)
        return worker_entities

    @api.post('/get_workers', response_model=WorkersWithCursor)
    @self_wrapper(webserver_pointer)
    async def get_workers(self, req: BatchQueryReq):
        await self.update_selector_nodes()
        workers, cursor = await self.repository.get_workers_from_cursor(req.limit, req.cursor)
        return WorkersWithCursor(workers, cursor)

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

    def run(self):
        webserver_pointer.set_self(self)

        app = FastAPI()
        app.include_router(api)

        uvicorn.run(app, host=self.host, port=self.port)
