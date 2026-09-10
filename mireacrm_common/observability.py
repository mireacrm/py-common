"""Наблюдаемость: метрики Prometheus и трассировка OpenTelemetry.

Трассировка включается адресом коллектора: пустой адрес означает выключено.
В тестах и при локальном запуске без инфраструктуры экспортёр только мешал бы,
поэтому по умолчанию он не поднимается.

Метрики работают всегда — их отдача ничего не стоит и ни от чего не зависит.
"""

import logging
import time

from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from mireacrm_common import tracing

log = logging.getLogger(__name__)

# Метки должны быть перечислимыми. Путь берётся шаблоном маршрута
# (/branches/{branch_id}/employees), а не фактическим: иначе каждый UUID
# порождает свой временной ряд, и хранилище распухает на ровном месте.
_LABELS = ("service", "method", "route", "status")

REQUESTS = Counter(
    "http_requests_total", "Обработанные HTTP-запросы", _LABELS
)
LATENCY = Histogram(
    "http_request_duration_seconds", "Время обработки HTTP-запроса",
    ("service", "method", "route"),
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
EVENTS_PUBLISHED = Counter(
    "domain_events_published_total", "Опубликованные доменные события",
    ("service", "routing_key"),
)
EVENTS_CONSUMED = Counter(
    "domain_events_consumed_total", "Обработанные доменные события",
    ("service", "routing_key", "outcome"),
)
RPC_CALLS = Counter(
    "grpc_server_requests_total", "Обработанные вызовы gRPC",
    ("service", "method", "code"),
)

# Пробы и сама отдача метрик из статистики исключены: они дают постоянный фон
# в несколько запросов в секунду и смазывают картину нагрузки.
_SILENT = frozenset({"/healthz", "/readyz", "/metrics"})


def route_of(request: Request) -> str:
    """Шаблон маршрута для метки.

    Шлюз проксирует чужие пути и подставляет шаблон сам, поэтому
    request.state имеет приоритет над маршрутом FastAPI.
    """
    explicit = getattr(request.state, "metrics_route", None)
    if explicit:
        return explicit
    route = request.scope.get("route")
    return getattr(route, "path", None) or "unmatched"


def install(app: FastAPI, service: str) -> None:
    """Считает запросы и отдаёт /metrics."""

    @app.middleware("http")
    async def collect(request: Request, call_next):
        if request.url.path in _SILENT:
            return await call_next(request)

        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            REQUESTS.labels(service, request.method, route_of(request), "500").inc()
            raise
        elapsed = time.perf_counter() - started

        route = route_of(request)
        REQUESTS.labels(service, request.method, route, str(response.status_code)).inc()
        LATENCY.labels(service, request.method, route).observe(elapsed)
        return response

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def setup_tracing(service: str, endpoint: str) -> bool:
    """Экспорт трасс по OTLP. Пустой адрес — трассировка выключена."""
    if not endpoint:
        return False

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(resource=Resource.create({"service.name": service}))
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True))
    )
    trace.set_tracer_provider(provider)

    # Идентификатор трассы для событий и логов теперь берётся у активного спана,
    # иначе трасса в Jaeger и трасса в конверте события разъехались бы.
    tracing.use_provider(_active_traceparent)
    log.info("трассировка включена, коллектор %s", endpoint)
    return True


def _active_traceparent() -> str:
    from opentelemetry.propagate import inject

    carrier: dict[str, str] = {}
    inject(carrier)
    return carrier.get("traceparent", "")


def name_span(name: str) -> None:
    """Переименовывает активный спан.

    Шлюз проксирует чужие пути и по маршруту FastAPI выглядит одним
    эндпоинтом /{path:path}: без переименования все запросы системы
    сливаются в одну операцию и разобрать в трассировке нечего.
    """
    from opentelemetry import trace

    span = trace.get_current_span()
    if span.is_recording():
        span.update_name(name)


def instrument_app(app: FastAPI) -> None:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    # Спаны receive и send отражают устройство ASGI, а не работу сервиса:
    # в трассе они дают шум на каждый запрос и ничего не объясняют.
    FastAPIInstrumentor.instrument_app(
        app,
        excluded_urls="healthz,readyz,metrics",
        exclude_spans=["receive", "send"],
    )


def instrument_database(engine) -> None:
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

    SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)


def publish_span(routing_key: str):
    """Спан издателя события.

    Готовой инструментации для нашей версии aio-pika нет, да она и не нужна:
    контекст едет в самом конверте, а не в заголовках AMQP, — так же, как его
    видит потребитель на другом языке.
    """
    from opentelemetry import trace

    return trace.get_tracer(__name__).start_as_current_span(
        f"publish {routing_key}", kind=trace.SpanKind.PRODUCER
    )


def consume_span(routing_key: str, traceparent: str):
    """Спан потребителя, продолжающий трассу издателя."""
    from opentelemetry import trace
    from opentelemetry.propagate import extract

    parent = extract({"traceparent": traceparent}) if traceparent else None
    return trace.get_tracer(__name__).start_as_current_span(
        f"consume {routing_key}", context=parent, kind=trace.SpanKind.CONSUMER
    )


def instrument_grpc() -> None:
    from opentelemetry.instrumentation.grpc import (
        GrpcAioInstrumentorClient,
        GrpcAioInstrumentorServer,
    )

    GrpcAioInstrumentorServer().instrument()
    GrpcAioInstrumentorClient().instrument()


def instrument_httpx() -> None:
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    HTTPXClientInstrumentor().instrument()
