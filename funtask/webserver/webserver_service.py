import asyncio
import base64
import json
import random
import re
import time
import uuid
from dataclasses import asdict
from typing import List, Tuple, cast, Callable

import dill
from grpclib.client import Channel

from funtask.common.typing_and_schema import function_parameters2schema
from funtask.core import interface_and_types as interface, entities
from dependency_injector.wiring import inject, Provide
from fastapi import FastAPI, APIRouter
import uvicorn

from funtask.utils.base64_bytes import Base64Bytes
from funtask.webserver.model import IncreaseWorkersReq, WorkersWithCursor, NewTaskReq, NewFuncReq, FuncWithCursor, \
    NewCronTaskReq
from funtask.webserver.utils import self_wrapper, SelfPointer

api = APIRouter(prefix='/api', tags=['api'])
webserver_pointer = SelfPointer()

func_name_re = re.compile(r'def *(\w+) *\(')


def _extract_func(func_base64: str) -> Callable:
    func_str = base64.b64decode(func_base64.encode('utf8')).decode('utf8')
    name_match_res = func_name_re.findall(func_str.strip())
    if not name_match_res:
        raise ValueError('function syntax err, must start with `def ...([*args], [**kwargs]):`')
    gl_var = {}
    exec(func_str, gl_var, gl_var)
    return gl_var[name_match_res[0]]


def _extract_func_bytes(func_base64: str) -> bytes:
    func_str = base64.b64decode(func_base64.encode('utf8')).decode('utf8')
    name_match_res = func_name_re.findall(func_str.strip())
    if not name_match_res:
        raise ValueError('function syntax err, must start with `def ...([*args], [**kwargs]):`')
    local = {}
    exec(func_str, {}, local)
    func_bytes = dill.dumps(local[name_match_res[0]])
    return func_bytes


class Webserver:
    @inject
    def __init__(
            self,
            rpc_channel_selector: interface.RPCChannelSelector[Channel] = Provide['webserver.rpc_selector'],
            repository: interface.Repository = Provide['webserver.repository'],
            task_worker_manager_rpc: interface.FunTaskManagerRPC = Provide['webserver.manager_rpc'],
            scheduler_rpc: interface.LeaderSchedulerRPC = Provide['webserver.scheduler_rpc'],
            scheduler_control: interface.LeaderSchedulerControl = Provide['webserver.scheduler_control'],
            manager_control: interface.ManagerNodeControl = Provide['webserver.manager_control'],
            host: str = Provide['webserver.service.host'],
            port: int = Provide['webserver.service.port']
    ):
        self.channel_selector = rpc_channel_selector
        self.scheduler_rpc = scheduler_rpc
        self.repository = repository
        self.task_worker_manager_rpc = task_worker_manager_rpc
        self.scheduler_control = scheduler_control
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

    @api.post('/workers', response_model=List[entities.Worker])
    @self_wrapper(webserver_pointer)
    async def increase_worker(self, req: IncreaseWorkersReq) -> List[entities.Worker]:
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

    @api.get('/workers', response_model=WorkersWithCursor)
    @self_wrapper(webserver_pointer)
    async def get_workers(self, limit: int, cursor: int | None = None, fuzzy_name: str | None = None):
        await self.update_selector_nodes()
        if fuzzy_name:
            res = await self.repository.match_workers_from_name(fuzzy_name, limit, cursor)
        else:
            res = await self.repository.get_workers_from_cursor(limit, cursor)
        workers, cursor = res
        return WorkersWithCursor(workers, cursor)

    @api.post('/task')
    @self_wrapper(webserver_pointer)
    async def create_task(self, req: NewTaskReq) -> List[str]:
        await self.update_selector_nodes()
        assert (req.worker_uuids is None) or (req.worker_tags is None), ValueError(
            'worker uuid or tags can\'t both exist'
        )
        assert (req.worker_uuids or req.worker_tags) is not None, ValueError('name or tags must exist one')
        await self.update_selector_nodes()

        if req.worker_uuids is not None:
            worker_uuids = req.worker_uuids
        elif req.worker_tags:
            workers = await self.repository.get_workers_from_tags(req.worker_tags)
            worker_uuids = [worker.uuid for worker in workers]
        else:
            raise ValueError('worker uuid and tags both None')

        res_tasks = []
        for worker_uuid in worker_uuids:
            task_uuid = str(uuid.uuid4())
            await self.repository.add_task(entities.Task(
                uuid=cast(entities.TaskUUID, task_uuid),
                parent_task_uuid=None,
                status=entities.TaskStatus.UNSCHEDULED,
                worker_uuid=worker_uuid,
                name=req.name,
                func=cast(entities.FuncUUID, req.func_uuid),
                argument=None,
                description=req.description,
                result_as_state=False,
                timeout=1000
            ))
            node: entities.SchedulerNode = random.choice(await self.scheduler_control.get_all_nodes())
            await self.scheduler_rpc.assign_task_to_node(
                node,
                task_uuid=cast(entities.TaskUUID, task_uuid)
            )
            res_tasks.append(task_uuid)
        return res_tasks

    @api.post('/func', response_model=entities.Func)
    @self_wrapper(webserver_pointer)
    async def add_func(self, req: NewFuncReq) -> entities.Func:
        func_bytes = _extract_func_bytes(req.func_base64)
        func_uuid = cast(entities.FuncUUID, str(uuid.uuid4()))
        func_entity = entities.Func(
            uuid=cast(entities.FuncUUID, func_uuid),
            func=Base64Bytes(func_bytes),
            dependencies=req.dependencies,
            parameter_schema=None,
            description=req.description,
            tags=[],
            name=req.name
        )
        await self.repository.add_func(func_entity)
        return func_entity

    @api.get('/func_schema', response_model=entities.ParameterSchema)
    @self_wrapper(webserver_pointer)
    async def get_func_schema(
            self,
            func_base64: str | None
    ):
        if func_base64 is not None:
            func = _extract_func(func_base64)
            schema = function_parameters2schema(func)
            return entities.ParameterSchema(
                uuid=cast(entities.ParameterSchemaUUID, str(uuid.uuid4())),
                json_schema=json.dumps(schema, indent=1)
            )

    @api.get('/func', response_model=FuncWithCursor)
    @self_wrapper(webserver_pointer)
    async def get_func(self, limit: int, cursor: int | None = None, fuzzy_name: str | None = None) -> FuncWithCursor:
        if fuzzy_name:
            funcs, cursor = await self.repository.match_functions_from_name(
                name=fuzzy_name,
                limit=limit,
                cursor=cursor
            )
        else:
            funcs, cursor = await self.repository.get_functions_from_cursor(limit, cursor)

        enc_able_funcs = []
        for func in funcs:
            func_dict = asdict(func)
            func_dict['func'] = Base64Bytes(func.func)
            enc_able_funcs.append(entities.Func(**func_dict))
        return FuncWithCursor(enc_able_funcs, cursor)

    @api.post('/cron_task')
    @self_wrapper(webserver_pointer)
    async def create_cron_task(self, req: NewCronTaskReq):
        task_uuid = cast(entities.CronTaskUUID, str(uuid.uuid4()))
        await self.repository.add_cron_task(entities.CronTask(
            uuid=task_uuid,
            timepoints=[entities.TimePoint(
                unit=entities.TimeUnit.SECOND,
                n=1
            )],
            func=req.function_uuid,
            argument_generate_strategy=entities.ArgumentStrategy(
                strategy=entities.ArgumentGenerateStrategy.STATIC,
                static_argument=None,
                argument_queue=None,
                udf=None,
                udf_extra=None
            ),
            worker_choose_strategy=entities.WorkerStrategy(
                strategy=entities.WorkerChooseStrategy.STATIC,
                static_worker=req.worker_uuid,
                worker_tags=[],
                udf=None,
                udf_extra=None
            ),
            task_queue_strategy=entities.QueueStrategy(
                full_strategy=entities.QueueFullStrategy.SKIP,
                udf=None,
                udf_extra=None,
                max_size=100
            ),
            description=None,
            name="",
            tags=[],
            disabled=False,
            result_as_state=False,
            timeout=-1
        ))

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

    async def add_parameter_schema(self, parameter_schema: entities.ParameterSchema) -> entities.ParameterSchemaUUID:
        pass

    def run(self):
        webserver_pointer.set_self(self)

        app = FastAPI()
        app.include_router(api)

        uvicorn.run(app, host=self.host, port=self.port)
