"""Проверки живости и готовности.

Разделены намеренно: liveness отвечает «процесс не завис», readiness —
«зависимости доступны». Если их не разделить, оркестратор либо считает
сервис живым при лежащей базе, либо перезапускает его из-за чужого сбоя.
"""

from dataclasses import dataclass

from sqlalchemy import text

from mireacrm_common.lifespan import AppContext


@dataclass(frozen=True, slots=True)
class ReadinessReport:
    checks: dict[str, str]

    @property
    def ready(self) -> bool:
        return all(status == "ok" for status in self.checks.values())


async def check_readiness(context: AppContext) -> ReadinessReport:
    checks: dict[str, str] = {}

    try:
        async with context.session() as session:
            await session.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as exc:
        checks["database"] = f"error: {type(exc).__name__}"

    checks["broker"] = "ok" if await context.publisher.ping() else "unreachable"
    return ReadinessReport(checks)
