"""Каркас gRPC-клиента: трасса в метаданных и перевод чужих кодов в свои."""

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import grpc

from mireacrm_common import identity, tracing
from mireacrm_common.errors import (
    ConflictError,
    ForbiddenError,
    InvalidArgumentError,
    NotFoundError,
    UnavailableError,
)

_CODES: dict[grpc.StatusCode, Callable[[str], Exception]] = {
    grpc.StatusCode.INVALID_ARGUMENT: InvalidArgumentError,
    grpc.StatusCode.PERMISSION_DENIED: ForbiddenError,
    grpc.StatusCode.ABORTED: ConflictError,
    grpc.StatusCode.FAILED_PRECONDITION: ConflictError,
}


def channel(address: str) -> grpc.aio.Channel:
    return grpc.aio.insecure_channel(address)


def _metadata() -> list[tuple[str, str]]:
    traceparent = tracing.current()
    metadata = [(tracing.HEADER, traceparent)] if traceparent else []
    return metadata + identity.metadata()


@asynccontextmanager
async def call(dependency: str, what: str, key: object) -> AsyncIterator[list[tuple[str, str]]]:
    """Оборачивает вызов соседа: отдаёт метаданные, переводит ошибки.

    Чужой NOT_FOUND — это наш NotFoundError, а не пятисотка. Недоступность
    соседа — не наша поломка, поэтому отдельный тип и 503 наружу.
    """
    try:
        yield _metadata()
    except grpc.aio.AioRpcError as exc:
        code = exc.code()
        if code == grpc.StatusCode.NOT_FOUND:
            raise NotFoundError(what, key) from exc
        if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
            raise UnavailableError(dependency) from exc
        factory = _CODES.get(code)
        if factory is not None:
            raise factory(exc.details() or code.name) from exc
        raise
