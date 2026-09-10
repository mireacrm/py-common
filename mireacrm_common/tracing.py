"""Контекст трассировки W3C.

Полноценный OpenTelemetry придёт на ПР10. Пока — минимум, который не даёт
трассе рваться: заголовок подхватывается из входящего запроса и проставляется
в событиях, уходящих в брокер.
"""

import re
import secrets
from collections.abc import Callable
from contextvars import ContextVar

HEADER = "traceparent"

# version-traceid-spanid-flags, RFC W3C Trace Context
_FORMAT = re.compile(r"^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$")

_current: ContextVar[str] = ContextVar("traceparent", default="")

_provider: Callable[[], str] | None = None


def use_provider(source: Callable[[], str]) -> None:
    """Подменяет источник traceparent — например, активным спаном OpenTelemetry.

    Нужно, чтобы идентификатор в событиях и логах совпадал с тем, что видно
    в системе трассировки: два независимых идентификатора не свести.
    """
    global _provider
    _provider = source


def new_traceparent() -> str:
    return f"00-{secrets.token_hex(16)}-{secrets.token_hex(8)}-01"


def parse(raw: str | None) -> str:
    """Принимает валидный заголовок, иначе начинает новую трассу."""
    if raw and _FORMAT.match(raw):
        return raw
    return new_traceparent()


def set_current(value: str) -> None:
    _current.set(value)


def current() -> str:
    if _provider is not None:
        value = _provider()
        if value:
            return value
    return _current.get()


def trace_id(value: str | None = None) -> str:
    """Короткий идентификатор для логов."""
    parts = (value or current()).split("-")
    return parts[1] if len(parts) == 4 else ""
