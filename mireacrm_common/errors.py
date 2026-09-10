class DomainError(Exception):
    """База доменных ошибок. Каждый транспорт маппит их в свои коды."""


class NotFoundError(DomainError):
    def __init__(self, what: str, key: object) -> None:
        super().__init__(f"{what} {key} не найден")
        self.what = what
        self.key = key


class ConflictError(DomainError):
    """Состояние системы не позволяет выполнить операцию."""


class InvalidArgumentError(DomainError):
    """Аргумент не проходит проверку до обращения к состоянию."""


class ForbiddenError(DomainError):
    """Роли достаточно, но объект принадлежит другому сотруднику."""

    def __init__(self, what: str) -> None:
        super().__init__(f"{what}: доступ только к своим записям")
        self.what = what


class UnavailableError(DomainError):
    """Сосед недоступен. Не наша поломка, поэтому 503, а не 500."""

    def __init__(self, dependency: str) -> None:
        super().__init__(f"{dependency} недоступен")
        self.dependency = dependency
