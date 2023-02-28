from contextlib import asynccontextmanager
from typing import AsyncIterator, Type, List, Any, TypeVar

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from funtask.core import interface_and_types as interface

from funtask.providers.db.sql.model import EntityConvertable

_T = TypeVar('_T')


class BaseSQLRepository:
    def __init__(
            self,
            uri: str,
            *args,
            **kwargs
    ):
        self.engine: AsyncEngine = create_async_engine(uri, *args, **kwargs)
        self.session = sessionmaker(
            self.engine, class_=AsyncSession
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

    async def _get_entity_from_column(
            self,
            t: Type[EntityConvertable[_T]],
            value: Any,
            column: str,
            options: List = None,
            session: AsyncSession | None = None
    ) -> _T:
        async with self._ensure_session(session) as session:
            result = await self._get_model_from_column(t, value, column, options=options, session=session)
            result: EntityConvertable[_T]
            return result.to_entity()

    async def _get_entity_from_uuid(
            self,
            t: Type[EntityConvertable[_T]],
            uuid: str,
            options: List = None,
            session: AsyncSession | None = None
    ) -> _T:
        async with self._ensure_session(session) as session:
            result = await self._get_model_from_uuid(t, uuid, options=options, session=session)
            result: EntityConvertable[_T]
            return result.to_entity()

    async def _get_model_from_column(
            self,
            t: Type[_T],
            value: str,
            column: str,
            options: List = None,
            session: AsyncSession | None = None
    ) -> _T:
        options = options or []
        async with self._ensure_session(session) as session:
            result = (await session.execute(
                select(t).where(getattr(t, column) == value).options(*options)  # type: ignore
            )).scalar()
            if not result:
                raise interface.RecordNotFoundException(
                    f'{t.__name__}: column {column} of value: `{value}` not found'
                )
            return result

    async def _get_model_from_uuid(
            self,
            t: Type[_T],
            uuid: str,
            options: List = None,
            session: AsyncSession | None = None
    ) -> _T:
        return await self._get_model_from_column(
            t,
            uuid,
            "uuid",
            options,
            session
        )

    async def _model_uuid2id(self, t: Type[_T], uuid: str, session: AsyncSession | None = None) -> int:
        async with self._ensure_session(session) as session:
            session: AsyncSession
            query = select(t.id).where(t.uuid == uuid)
            result = (await session.execute(query)).first()
            if not result:
                raise interface.RecordNotFoundException(f'record uuid: {uuid} of type: {t} not found')
            return result[0]

