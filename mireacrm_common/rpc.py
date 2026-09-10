"""Каркас gRPC-сервера: перехват ошибок, разбор аргументов, сборка сервера."""

import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from grpc_reflection.v1alpha import reflection
from sqlalchemy.exc import IntegrityError

from mireacrm_common import identity, observability, tracing
from mireacrm_common.errors import (
    ConflictError,
    ForbiddenError,
    InvalidArgumentError,
    NotFoundError,
    UnavailableError,
)


class ServerInterceptor(grpc.aio.ServerInterceptor):
    """Подхватывает контекст вызова, маппит доменные ошибки и считает вызовы."""

    def __init__(self, service: str = "") -> None:
        self._service = service

    _CODES: ClassVar[dict[type[Exception], grpc.StatusCode]] = {
        NotFoundError: grpc.StatusCode.NOT_FOUND,
        ForbiddenError: grpc.StatusCode.PERMISSION_DENIED,
        InvalidArgumentError: grpc.StatusCode.INVALID_ARGUMENT,
        ConflictError: grpc.StatusCode.ABORTED,
        UnavailableError: grpc.StatusCode.UNAVAILABLE,
    }

    async def intercept_service(self, continuation, handler_call_details):
        handler = await continuation(handler_call_details)
        if handler is None or handler.unary_unary is None:
            return handler

        inner: Callable[..., Awaitable] = handler.unary_unary
        metadata = dict(handler_call_details.invocation_metadata or ())

        method = handler_call_details.method

        async def wrapper(request, context):
            tracing.set_current(tracing.parse(metadata.get(tracing.HEADER)))
            identity.set_current(identity.parse(metadata))
            code = "OK"
            try:
                return await inner(request, context)
            except IntegrityError:
                code = grpc.StatusCode.ABORTED.name
                await context.abort(grpc.StatusCode.ABORTED, "конфликт при записи в базу")
            except tuple(self._CODES) as exc:
                code = self._CODES[type(exc)].name
                await context.abort(self._CODES[type(exc)], str(exc))
            finally:
                # finally, а не хвост try: context.abort бросает исключение,
                # и до строки после него управление не дойдёт.
                observability.RPC_CALLS.labels(self._service, method, code).inc()

        return grpc.unary_unary_rpc_method_handler(
            wrapper,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


@dataclass(frozen=True, slots=True)
class Registration:
    """Как подключить сервисер к серверу и под каким именем показать в рефлексии."""

    register: Callable[[grpc.aio.Server], None]
    full_name: str


def parse_uuid(value: str, field: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError as exc:
        raise InvalidArgumentError(f"{field}: невалидный UUID") from exc


def to_timestamp(value: datetime) -> Timestamp:
    out = Timestamp()
    out.FromDatetime(value)
    return out


async def build_server(
    port: int, registrations: Sequence[Registration], service: str = ""
) -> grpc.aio.Server:
    server = grpc.aio.server(interceptors=[ServerInterceptor(service)])
    for item in registrations:
        item.register(server)

    # Рефлексия нужна, чтобы grpcurl работал без .proto под рукой.
    reflection.enable_server_reflection(
        (*(item.full_name for item in registrations), reflection.SERVICE_NAME), server
    )
    server.add_insecure_port(f"0.0.0.0:{port}")
    return server
