from dataclasses import asdict
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Type, TypeVar
from sqlalchemy import and_
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload, Query, Load, lazyload, load_only
from sqlalchemy.sql import select, update, or_, functions as fn

from funtask.common.sql import BaseSQLRepository
from funtask.providers.db.sql import model

from funtask.core import interface_and_types as interface, entities

_T = TypeVar('_T')


def _obj_get_uuid(obj: Any) -> str | None:
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj
    return getattr(obj, 'uuid')


class Repository(BaseSQLRepository, interface.Repository):
    async def get_workers_from_cursor(
            self,
            limit: int,
            cursor: int | None = None,
            session: AsyncSession | None = None
    ) -> Tuple[List[entities.Worker], int]:
        async with self._ensure_session(session) as session:
            session: AsyncSession
            query = select(model.Worker)
            print(self.compile_query(query))

            if cursor:
                query = query.where(model.Worker.id > cursor)

            results = await session.execute(query.limit(limit))
            result_models: List[model.Worker] = [result[0] for result in results.unique()]

            next_cursor = max([result.id for result in result_models] + [0])
            return [worker_model.to_entity() for worker_model in result_models], next_cursor

    async def get_functions_from_cursor(
            self,
            limit: int,
            cursor: int | None = None,
            session=None
    ) -> Tuple[List[entities.Func], int]:
        async with self._ensure_session(session) as session:
            session: AsyncSession
            query = select(model.Function)

            if cursor:
                query = query.where(model.Function.id > cursor)

            results = (await session.execute(query.limit(limit))).unique()
            result_models: List[model.Function] = [result[0] for result in results]

            next_cursor = max([result.id for result in result_models] + [0])
            return [func_model.to_entity() for func_model in result_models], next_cursor

    async def get_function_from_uuid(
            self,
            func_uuid: entities.FuncUUID,
            session: AsyncSession | None = None
    ) -> entities.Func:
        return await self._get_entity_from_uuid(
            model.Function,
            func_uuid, session=session
        )

    async def get_worker_from_name(self, name: str, session: AsyncSession | None = None) -> entities.Worker:
        async with self._ensure_session(session) as session:
            session: AsyncSession
            query = select(model.Worker).where(model.Worker.name == name)
            result = (await session.execute(query)).first()

            if not result:
                raise interface.RecordNotFoundException(
                    f'worker name `{name}` not found'
                )
            worker: model.Worker = result[0]
            return worker.to_entity()

    async def match_workers_from_name(
            self,
            name: str,
            limit: int,
            cursor: int | None = None,
            session: AsyncSession | None = None
    ) -> Tuple[List[entities.Worker], int]:
        async with self._ensure_session(session) as session:
            session: AsyncSession
            query = select(model.Worker).where(
                model.Worker.name.like(f"%{name}%"),
            ).limit(limit)
            if cursor:
                query = query.where(model.Worker.id > cursor)
            result = (await session.execute(query)).unique()

            return [worker[0].to_entity() for worker in result], max([worker[0].id for worker in result] + [0])

    async def update_task_start_time(self, task_uuid: entities.TaskUUID, start_time: datetime, session=None):
        await self.update_task(task_uuid, {
            'start_time': start_time
        }, session)

    async def update_task_stop_time(self, task_uuid: entities.TaskUUID, stop_time: datetime, session=None):
        await self.update_task(task_uuid, {
            'stop_time': stop_time
        }, session)

    @staticmethod
    def _filter_open_range(query: Query, worker_inactive2dead_second: int, range_begin: datetime) -> Query:
        # task not stop and task's worker not dead
        return query.where(
            or_(
                # task and range both not end
                and_(
                    model.Task.stop_time == None,
                    model.Worker.last_heart_beat > datetime.now() + timedelta(
                        seconds=worker_inactive2dead_second
                    )
                ),
                # [ Task | ] range
                and_(
                    model.Task.stop_time > range_begin
                ),
                # | range [ Task ]
                and_(
                    model.Task.create_time > range_begin,
                    model.Task.stop_time != None
                )
            )
        )

    @staticmethod
    def _filter_range(query: Query, worker_inactive2dead_second: int, range_: Tuple[datetime, datetime]) -> Query:
        begin_time, end_time = range_
        return query.where(
            or_(
                # [ Task | ] range |
                and_(
                    model.Task.stop_time < end_time,
                    model.Task.stop_time > begin_time,
                ),
                # | range [ Task | ]
                and_(
                    model.Task.create_time < end_time,
                    model.Task.create_time > begin_time
                ),
                # [ Task | range |
                and_(
                    model.Task.create_time < begin_time,
                    model.Task.stop_time == None,
                    model.Worker.last_heart_beat > datetime.now() + timedelta(
                        seconds=worker_inactive2dead_second
                    )
                )
            )
        )

    async def get_brief_tasks_from_cursor(
            self,
            begin_time: datetime,
            worker_inactive2dead_second: int,
            end_time: datetime | None = None,
            worker_uuid: entities.WorkerUUID | None = None,
            include_cross_task: bool = False,
            limit: int | None = None,
            cursor: int | None = None,
            session: AsyncSession | None = None
    ) -> Tuple[List[entities.Task], int]:
        async with self._ensure_session(session) as session:
            session: AsyncSession
            query = (
                select(
                    model.Task,
                    model.Function.uuid
                )
                .join(model.Function, model.Function.id == model.Task.func_id, isouter=True)
                .join(model.Worker, model.Worker.id == model.Task.worker_id, isouter=True)
            )
            query = query.options(
                lazyload('*')
            )
            if worker_uuid:
                query = query.where(model.Worker.uuid == worker_uuid)

            if include_cross_task:
                # query to current time
                if not end_time:
                    query = self._filter_open_range(query, worker_inactive2dead_second, begin_time)
                # query to end_time
                else:
                    query = self._filter_range(query, worker_inactive2dead_second, (begin_time, end_time))
            else:
                query = query.where(model.Task.start_time > begin_time)
                if end_time:
                    query = query.where(model.Task.stop_time < end_time)

            if cursor:
                query = query.where(model.Task.id > cursor)

            result = (await session.execute(query))
            print(self.compile_query(query))
            res = [
                (task.id, entities.Task(
                    uuid=task.uuid,
                    parent_task_uuid=task.parent_task_uuid,
                    parent_task_type=task.parent_task_type,
                    status=task.status,
                    worker_uuid=worker_uuid,
                    func=func_uuid,
                    argument=None,
                    result_as_state=task.result_as_state,
                    start_time=task.start_time,
                    stop_time=task.stop_time,
                    timeout=task.timeout,
                    description=task.description,
                    result=task.result,
                    name=task.name,
                    create_time=task.create_time
                )) for task, func_uuid in result
            ]
            if not res:
                return [], 0
            ids, tasks = zip(*res)
            return list(tasks), max(ids)

    async def match_functions_from_name(
            self,
            name: str,
            limit: int,
            cursor: int | None = None,
            session=None
    ) -> Tuple[List[entities.Func], int]:
        async with self._ensure_session(session) as session:
            session: AsyncSession
            query = select(model.Function).where(
                model.Function.name.like(f"%{name}%")
            )
            if cursor:
                query = query.where(model.Function.id > cursor)
            result = (await session.execute(query)).unique()
            funcs = [(func[0].to_entity(), func[0].id) for func in result]
            if not funcs:
                return [], 0
            funcs, func_ids = zip(*funcs)

            return funcs, max(list(func_ids) + [0])

    async def get_task_from_uuid(self, task_uuid: entities.TaskUUID,
                                 session: AsyncSession | None = None) -> entities.Task:
        return await self._get_entity_from_uuid(
            model.Task,
            task_uuid,
            session=session
        )

    async def get_cron_task_from_uuid(
            self,
            task_uuid: entities.CronTaskUUID,
            session: AsyncSession | None = None
    ) -> entities.CronTask:
        return await self._get_entity_from_uuid(
            model.CronTask,
            task_uuid, session=session
        )

    async def get_all_cron_task(self, session: AsyncSession | None = None) -> List[entities.CronTask]:
        async with self._ensure_session(session) as session:
            result: List[Tuple[model.CronTask]] = (await session.execute(
                select(model.CronTask)
            )).unique()
            return [cronTask[0].to_entity() for cronTask in result]

    async def add_task(self, task: entities.Task, session: AsyncSession | None = None):
        async with self._ensure_session(session) as session:
            session: AsyncSession
            if isinstance(task.func, entities.Func):
                func_id = (await self._get_model_from_uuid(model.Function, task.func.uuid, session=session)).id
            else:
                func_id = (await self._get_model_from_uuid(model.Function, task.func, session=session)).id

            session.add(model.Task(
                uuid=task.uuid,
                status=task.status,
                parent_task_uuid=task.parent_task_uuid,
                parent_task_type=task.parent_task_type,
                worker_id=(await self._get_model_from_uuid(model.Worker, task.worker_uuid, session=session)).id,
                func_id=func_id,
                timeout=task.timeout,
                description=task.description,
                result=task.result,
                create_time=task.create_time,
                result_as_state=task.result_as_state
            ))

    async def change_task_status(
            self, task_uuid: entities.TaskUUID, status: entities.TaskStatus, session: AsyncSession | None = None):
        async with self._ensure_session(session) as session:
            session: AsyncSession
            task = await self._get_model_from_uuid(model.Task, task_uuid, session=session)
            task.status = status.value

    async def change_task_status_from_uuid(
            self,
            task_uuid: entities.TaskUUID,
            status: entities.TaskStatus,
            session=None
    ):
        async with self._ensure_session(session) as session:
            session: AsyncSession
            task = await self._get_model_from_column(
                model.Task,
                task_uuid,
                "uuid",
                session=session
            )
            task.status = status.value

    async def add_func(self, func: entities.Func, session: AsyncSession | None = None) -> int:
        async with self._ensure_session(session) as session:
            if func.parameter_schema is not None:
                schema_id = await self._model_uuid2id(model.ParameterSchema, func.parameter_schema.uuid, session)
            else:
                schema_id = None
            # TODO: add tag to func

            func_model = model.Function(
                uuid=func.uuid,
                description=func.description,
                dependencies=func.dependencies,
                parameter_schema_id=schema_id,
                function=func.func,
                name=func.name
            )
            session.add(func_model)
            return func_model.id

    async def add_worker(self, worker: entities.Worker, session: AsyncSession | None = None):
        async with self._ensure_session(session) as session:
            # TODO: tag process
            session: AsyncSession
            session.add(model.Worker(
                uuid=worker.uuid,
                status=worker.status.value,
                name=worker.name,
                last_heart_beat=datetime.now(),
                start_time=datetime.now()
            ))

    async def get_worker_from_uuid(self, worker_uuid: entities.WorkerUUID,
                                   session: AsyncSession | None = None) -> entities.Task:
        async with self._ensure_session(session) as session:
            session: AsyncSession
            return await self._get_entity_from_uuid(
                model.Worker,
                worker_uuid,
                [selectinload(model.Worker.tags)],
                session
            )

    async def change_worker_status(
            self, worker_uuid: entities.WorkerUUID, status: entities.WorkerStatus, session: AsyncSession | None = None):
        async with self._ensure_session(session) as session:
            session: AsyncSession
            worker = await self._get_model_from_uuid(model.Worker, worker_uuid, session=session)
            worker.status = status.value

    async def add_cron_task(self, task: entities.CronTask, session: AsyncSession | None = None):
        async with self._ensure_session(session) as session:
            session: AsyncSession
            static_argument_uuid = _obj_get_uuid(task.argument_generate_strategy.static_argument)
            static_argument_id = static_argument_uuid and await self._model_uuid2id(
                model.Argument,
                static_argument_uuid,
                session
            )

            queue_uuid = _obj_get_uuid(task.argument_generate_strategy.argument_queue)
            queue_id = queue_uuid and await self._model_uuid2id(
                model.Function,
                queue_uuid,
                session
            )

            arg_gen_udf_uuid = _obj_get_uuid(task.argument_generate_strategy.udf)
            arg_gen_udf_id = arg_gen_udf_uuid and await self._model_uuid2id(
                model.Function,
                arg_gen_udf_uuid,
                session
            )

            if isinstance(task.func, str):
                func_id = await self._model_uuid2id(model.Function, task.func, session)
            else:
                func_id = await self._model_uuid2id(model.Function, task.func.uuid, session)

            static_worker_uuid = _obj_get_uuid(task.worker_choose_strategy.static_worker)
            static_worker_id = static_worker_uuid and await self._model_uuid2id(
                model.Worker,
                static_worker_uuid,
                session
            )

            worker_choose_udf_uuid = _obj_get_uuid(task.worker_choose_strategy.udf)
            worker_choose_udf_id = worker_choose_udf_uuid and await self._model_uuid2id(
                model.Function,
                worker_choose_udf_uuid,
                session
            )

            task_queue_udf_uuid = _obj_get_uuid(task.task_queue_strategy.udf)
            task_queue_udf_id = task_queue_udf_uuid and await self._model_uuid2id(
                model.Function,
                task_queue_udf_uuid,
                session
            )

            session.add(model.CronTask(
                uuid=task.uuid,
                name=task.name,
                function_id=func_id,
                argument_generate_strategy=task.argument_generate_strategy.strategy.value,
                argument_generate_strategy_static_argument_id=static_argument_id,
                argument_generate_strategy_args_queue_id=queue_id,
                argument_generate_strategy_udf_id=arg_gen_udf_id,
                argument_generate_strategy_udf_extra=task.argument_generate_strategy.udf_extra,
                worker_choose_strategy=task.worker_choose_strategy.strategy.value,
                worker_choose_strategy_static_worker_id=static_worker_id,
                worker_choose_strategy_worker_tags=task.worker_choose_strategy.worker_tags,
                worker_choose_strategy_udf_id=worker_choose_udf_id,
                worker_choose_strategy_udf_extra=task.worker_choose_strategy.udf_extra,
                task_queue_strategy=task.task_queue_strategy.full_strategy.value,
                task_queue_max_size=task.task_queue_strategy.max_size,
                task_queue_strategy_udf_id=task_queue_udf_id,
                task_queue_strategy_udf_extra=task.task_queue_strategy.udf_extra,
                result_as_status=task.result_as_state,
                timeout=task.timeout,
                timepoints=[asdict(point) for point in task.timepoints],
                description=task.description,
                disabled=task.disabled
            ))

    async def add_func_parameter_schema(
            self,
            func_parameter_schema: entities.ParameterSchema,
            session: AsyncSession | None = None
    ):
        async with self._ensure_session(session) as session:
            session: AsyncSession
            session.add(model.ParameterSchema(
                uuid=func_parameter_schema.uuid
            ))

    async def update_task(self, task_uuid: entities.TaskUUID, value: Dict[str, Any], session: AsyncSession = None):
        async with self._ensure_session(session) as session:
            session: AsyncSession
            await session.execute(
                update(model.Task).where(model.Task.uuid == task_uuid).values(**value)
            )

    async def update_worker_last_heart_beat_time(
            self, worker_uuid: entities.WorkerUUID, t: datetime, session: AsyncSession | None = None):
        async with self._ensure_session(session) as session:
            session: AsyncSession
            res: Row[model.Worker] = (await session.execute(
                select(model.Worker).where(model.Worker.uuid == worker_uuid)
            )).first()
            if not res:
                raise interface.RecordNotFoundException(f'worker {worker_uuid} not found')
            worker: model.Worker = res[0]
            worker.last_heart_beat = t

    async def get_workers_from_tags(
            self,
            tags: List[entities.Tag],
            session: AsyncSession | None = None
    ) -> List[entities.Worker]:
        async with self._ensure_session(session) as session:
            session: AsyncSession

            value_tag: Type[model.Tag] = aliased(model.Tag)
            sub_query = select(model.Worker.id.label('worker_id'), fn.count(model.Worker.id).label('match_cnt')).join(
                model.TagRelation,
                model.Worker.uuid == model.TagRelation.related_uuid
            ).join(model.Tag, model.TagRelation.tag_id == model.Tag.id).join(
                value_tag,
                value_tag.parent_tag_id == model.Tag.id,
                isouter=True
            ).group_by(model.Worker.uuid).where(
                model.TagRelation.tag_type == 'worker',
                or_(
                    *[
                        and_(
                            model.Tag.tag_name == tag.key,
                            model.Tag.parent_tag_id == tag.value
                        )
                        for tag in tags
                    ]
                ),
            ).subquery()
            workers = await session.execute(select(model.Worker).join(
                sub_query,
                sub_query.worker_id == model.Worker.id
            ).where(sub_query.match_cnt == len(tags)))
            workers: List[Tuple[model.Worker]]
            return [worker[0].to_entity() for worker in workers]

    async def drop_model_schema(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(model.Base.metadata.drop_all)

    async def create_model_schema(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(model.Base.metadata.create_all)
