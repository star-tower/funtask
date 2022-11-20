from datetime import datetime
from typing import List, Dict, Any, AsyncIterator, Tuple, Type, TypeVar

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from contextlib import asynccontextmanager
from funtask.providers.db.sql import model

from funtask.core import interface_and_types as interface, entities
from funtask.providers.db.sql.model import EntityConvertable

_T = TypeVar('_T')


class Repository(interface.Repository):
    def __init__(
            self,
            uri: str,
            *args,
            **kwargs
    ):
        self.engine: AsyncEngine = create_async_engine(uri, *args, **kwargs)
        self.session = sessionmaker(
            self.engine, expire_on_commit=False, class_=AsyncSession
        )

    @asynccontextmanager
    async def session_ctx(self):
        async with self.session.begin() as session:
            session: AsyncSession
            try:
                yield session
                await session.commit()
            except interface.RollbackException:
                await session.rollback()
            finally:
                await session.close()

    @asynccontextmanager
    async def _ensure_session(self, session: AsyncSession | None) -> AsyncIterator[AsyncSession]:
        if session:
            yield session
        else:
            async with self.session_ctx() as session:
                yield session

    async def _get_entity_from_uuid(
            self,
            t: Type[EntityConvertable[_T]],
            uuid: str,
            session: AsyncSession | None = None
    ) -> _T:
        async with self._ensure_session(session) as session:
            result = (await session.execute(
                select(t).where(t.uuid == uuid)  # type: ignore
            )).first()
            if not result:
                raise interface.RecordNotFoundException(
                    f'{t.__name__}: {uuid} not found'
                )
            result: Tuple[EntityConvertable[_T]]
            return result[0].to_entity()

    async def get_function_from_uuid(
            self,
            func_uuid: entities.FuncUUID,
            session: AsyncSession | None = None
    ) -> entities.Func:
        return await self._get_entity_from_uuid(
            model.Function,
            func_uuid, session
        )

    async def get_task_from_uuid(self, task_uuid: entities.TaskUUID,
                                 session: AsyncSession | None = None) -> entities.Task:
        return await self._get_entity_from_uuid(
            model.Task,
            task_uuid, session
        )

    async def get_cron_task_from_uuid(
            self,
            task_uuid: entities.CronTaskUUID,
            session: AsyncSession | None = None
    ) -> entities.CronTask:
        return await self._get_entity_from_uuid(
            model.CronTask,
            task_uuid, session
        )

    async def get_all_cron_task(self, session: AsyncSession | None = None) -> List[entities.CronTask]:
        async with self._ensure_session(session) as session:
            result = (await session.execute(
                select(model.Task)
            )).all()
            return result

    async def add_task(self, task: entities.Task, session: AsyncSession | None = None):
        async with self._ensure_session(session) as session:
            session: AsyncSession
            session.add(model.Task(
                uuid=task.uuid,
                uuid_in_manager=task.uuid_in_manager,
                parent_task_uuid=task.parent_task_uuid
            ))

    async def change_task_status(
            self, task_uuid: entities.TaskUUID, status: entities.TaskStatus, session: AsyncSession | None = None):
        pass

    async def add_func(self, func: entities.Func, session: AsyncSession | None = None):
        async with self._ensure_session(session) as session:
            if func.parameter_schema is not None:
                schema_id = await session.execute(
                    select(model.ParameterSchema.id).where(
                        model.ParameterSchema.uuid == func.parameter_schema)
                )
            else:
                schema_id = None

            session.add(model.Function(
                uuid=func.uuid,
                description=func.description,
                dependencies=func.dependencies,
                parameter_schema_id=schema_id,
                function=func.func,
                name=func.name
            ))

    async def add_worker(self, worker: entities.Worker, session: AsyncSession | None = None):
        pass

    async def get_worker_from_uuid(self, task_uuid: entities.WorkerUUID,
                                   session: AsyncSession | None = None) -> entities.Task:
        pass

    async def change_worker_status(
            self, worker_uuid: entities.WorkerUUID, status: entities.WorkerStatus, session: AsyncSession | None = None):
        pass

    async def add_cron_task(self, task: entities.CronTask, session: AsyncSession | None = None):
        pass

    async def add_func_parameter_schema(
            self,
            func_parameter_schema: entities.ParameterSchema,
            session: AsyncSession | None = None
    ) -> entities.ParameterSchemaUUID:
        pass

    async def update_task_uuid_in_manager(
            self, task_uuid: entities.TaskUUID,
            task_uuid_in_manager: entities.TaskUUID,
            session: AsyncSession | None = None
    ):
        pass

    async def update_task(self, task_uuid: entities.TaskUUID, value: Dict[str, Any], session: AsyncSession = None):
        pass

    async def update_worker_last_heart_beat_time(
            self, worker_uuid: entities.WorkerUUID, t: datetime, session: AsyncSession | None = None):
        pass

    async def get_workers_from_tags(
            self,
            tags: List[str],
            session: AsyncSession | None = None
    ) -> List[entities.Worker]:
        pass

    async def drop_model_schema(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(model.Base.metadata.drop_all)

    async def create_model_schema(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(model.Base.metadata.create_all)
