from typing import Tuple

from sqlalchemy import Column, BIGINT, String, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import declarative_base

from funtask.common.sql import BaseSQLRepository
from funtask.core import entities
from funtask.providers.loggers import rotate_file
from funtask.providers.loggers.rotate_file import Offset

Base = declarative_base()


class TaskLogMeta(Base):
    __tablename__ = 'task_log_meta'
    id = Column(BIGINT(), primary_key=True, autoincrement=True, nullable=False)
    task_uuid = Column(String(36), nullable=False)
    start_offset = Column(BIGINT(), nullable=False)
    start_log_filename = Column(String(256), nullable=False)
    end_offset = Column(BIGINT(), nullable=True)
    end_log_filename = Column(String(256), nullable=True)


class Repository(rotate_file.Repository, BaseSQLRepository):
    async def set_task_log_start(
            self,
            task_uuid: entities.TaskUUID,
            offset: Offset,
            session: AsyncSession | None = None
    ):
        filename, offset_in_file = offset
        task_log_meta = TaskLogMeta(
            start_offset=offset_in_file,
            start_log_filename=filename,
            task_uuid=task_uuid
        )
        async with self._ensure_session(session) as session:
            session: AsyncSession
            session.add(task_log_meta)

    async def set_task_log_end(
            self,
            task_uuid: entities.TaskUUID,
            offset: Offset,
            session: AsyncSession | None = None
    ):
        async with self._ensure_session(session) as session:
            session: AsyncSession
            filename, offset_in_file = offset
            update_query = update(TaskLogMeta).where(TaskLogMeta.task_uuid == task_uuid).values({
                'end_offset': offset_in_file,
                'end_log_filename': filename
            })
            await session.execute(update_query)

    async def get_task_log_offset_info(
            self,
            task_uuid: entities.TaskUUID,
            session: AsyncSession | None = None
    ) -> Tuple[Offset, Offset | None]:
        async with self._ensure_session(session) as session:
            session: AsyncSession
            log_meta = await self._get_model_from_column(
                TaskLogMeta,
                value=task_uuid,
                column='task_uuid',
                session=session
            )
            start_offset = (log_meta.start_log_filename, log_meta.start_offset)
            end_offset = None
            if log_meta.end_offset is not None:
                end_offset = (log_meta.end_log_filename, log_meta.end_offset)
            return start_offset, end_offset

    async def create_model_schema(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def drop_model_schema(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
