from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from mireacrm_common.config import ServiceSettings
from mireacrm_common.db import create_engine, create_session_factory
from mireacrm_common.events import EventPublisher


class Closeable(Protocol):
    async def close(self) -> None: ...


@dataclass(slots=True)
class AppContext:
    """Ресурсы процесса. Живут столько же, сколько сервис."""

    settings: ServiceSettings
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    publisher: EventPublisher
    # gRPC-клиенты к соседям, если сервис куда-то ходит. Каркас про их
    # устройство ничего не знает, только закрывает на выходе.
    clients: Closeable | None = field(default=None)

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        async with self.sessions() as session:
            yield session


@asynccontextmanager
async def build_context(
    settings: ServiceSettings, clients: Closeable | None = None
) -> AsyncIterator[AppContext]:
    engine = create_engine(settings.postgres_dsn, echo=settings.debug)
    publisher = EventPublisher(settings.amqp_url, settings.service_name)
    await publisher.connect()

    try:
        yield AppContext(
            settings=settings,
            engine=engine,
            sessions=create_session_factory(engine),
            publisher=publisher,
            clients=clients,
        )
    finally:
        if clients is not None:
            await clients.close()
        await publisher.close()
        await engine.dispose()
