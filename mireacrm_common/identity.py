"""Личность вызывающего, пришедшая от шлюза.

Токен проверяет шлюз, дальше по системе едет уже разобранный результат.
Без этого внутренняя сеть оказывается доверенной целиком: кто попал внутрь
периметра, действует от любого имени.

Кроме учётной записи передаётся сотрудник, которому она соответствует.
Проверять принадлежность объекта обязан сервис-владелец, и без этого
соответствия ему сравнивать не с чем.

Имя пользователя кодируется процентами: и HTTP-заголовок, и метаданные gRPC
допускают только ASCII, а имя может оказаться кириллицей.
"""

from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from urllib.parse import quote, unquote

HEADER_SUBJECT = "x-user-id"
HEADER_USERNAME = "x-user-name"
HEADER_ROLES = "x-user-roles"
HEADER_EMPLOYEE = "x-employee-id"


@dataclass(frozen=True, slots=True)
class Caller:
    subject: str = ""
    username: str = ""
    roles: frozenset[str] = field(default_factory=frozenset)
    employee_id: str = ""

    @property
    def known(self) -> bool:
        return bool(self.subject)

    def has_any(self, *roles: str) -> bool:
        return bool(self.roles & frozenset(roles))


ANONYMOUS = Caller()

_current: ContextVar[Caller] = ContextVar("caller", default=ANONYMOUS)


def parse(headers: Mapping[str, str]) -> Caller:
    subject = headers.get(HEADER_SUBJECT, "")
    if not subject:
        return ANONYMOUS
    roles = headers.get(HEADER_ROLES, "")
    return Caller(
        subject=subject,
        username=unquote(headers.get(HEADER_USERNAME, "")),
        roles=frozenset(role for role in roles.split(",") if role),
        employee_id=headers.get(HEADER_EMPLOYEE, ""),
    )


def set_current(caller: Caller) -> None:
    _current.set(caller)


def current() -> Caller:
    return _current.get()


def metadata() -> list[tuple[str, str]]:
    """Заголовки для исходящего вызова соседа."""
    caller = current()
    if not caller.known:
        return []
    return [
        (HEADER_SUBJECT, caller.subject),
        (HEADER_USERNAME, quote(caller.username)),
        (HEADER_ROLES, ",".join(sorted(caller.roles))),
        (HEADER_EMPLOYEE, caller.employee_id),
    ]
