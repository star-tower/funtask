from datetime import datetime
from typing import List, Dict, Any, AsyncIterator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from contextlib import asynccontextmanager
from funtask.providers.db.sql import model

from funtask.core import interface_and_types as interface, entities


class Repository(interface.Repository):
    def __init__(
            self,
            uri: str,
            *args,
            **kwargs
    ):
        self.engine = create_async_engine(uri, *args, **kwargs)
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

    async def get_task_from_uuid(self, task_uuid: entities.TaskUUID, session: AsyncSession | None = None) -> entities.Task:
        async with self._ensure_session(session) as session:
            result = (await session.execute(
                select(model.Task).where(model.Task.uuid == task_uuid)
            )).first()
            if not result:
                raise interface.RecordNotFoundException(
                    f'task {task_uuid} not found')
            return result[0]

    async def get_cron_task_from_uuid(self, task_uuid: entities.CronTaskUUID,
                                      session: AsyncSession | None = None) -> entities.CronTask:
        pass

    async def get_all_cron_task(self, session: AsyncSession | None = None) -> List[entities.CronTask]:
        pass

    async def add_task(self, task: entities.Task, session: AsyncSession | None = None) -> entities.TaskUUID:
        pass

    async def change_task_status(
            self, task_uuid: entities.TaskUUID, status: entities.TaskStatus, session: AsyncSession | None = None):
        pass

    async def add_func(self, func: entities.Func, session: AsyncSession | None = None) -> entities.FuncUUID:
        pass

    async def add_worker(self, worker: entities.Worker, session: AsyncSession | None = None) -> entities.WorkerUUID:
        pass

    async def get_worker_from_uuid(self, task_uuid: entities.WorkerUUID, session: AsyncSession | None = None) -> entities.Task:
        pass

    async def change_worker_status(
            self, worker_uuid: entities.WorkerUUID, status: entities.WorkerStatus, session: AsyncSession | None = None):
        pass

    async def add_cron_task(self, task: entities.CronTask, session: AsyncSession | None = None) -> entities.TaskUUID:
        pass

    async def add_func_parameter_schema(
            self,
            func_parameter_schema: entities.FuncParameterSchema,
            session: AsyncSession | None = None
    ) -> entities.FuncParameterSchemaUUID:
        pass

    async def update_task_uuid_in_manager(self, task_uuid: entities.TaskUUID, task_uuid_in_manager: entities.TaskUUID,
                                          session: AsyncSession | None = None):
        pass

    async def update_task(self, task_uuid: entities.TaskUUID, value: Dict[str, Any], session: AsyncSession = None):
        pass

    async def update_worker_last_heart_beat_time(
            self, worker_uuid: entities.WorkerUUID, t: datetime, session: AsyncSession | None = None):
        pass

    async def get_workers_from_tags(self, tags: List[str], session: AsyncSession | None = None) -> List[entities.Worker]:
        pass

    async def drop_model_schema(self):
        return await super().drop_model_schema()

    async def create_model_schema(self):
        return await super().create_model_schema()
