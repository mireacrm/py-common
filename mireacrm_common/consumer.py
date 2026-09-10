"""Каркас потребителя RabbitMQ.

Очередь и биндинги объявлены декларативно в deploy/rabbitmq/definitions.json —
потребитель их не создаёт, только подписывается.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable

import aio_pika
from google.protobuf.json_format import Parse, ParseError
from mirea.events.v1 import events_pb2

from mireacrm_common import observability, tracing
from mireacrm_common.errors import InvalidArgumentError

log = logging.getLogger(__name__)

Handler = Callable[[events_pb2.EventEnvelope], Awaitable[None]]

# Без ограничения брокер вывалит в потребителя всю очередь разом, и при
# падении процесса вся пачка вернётся необработанной.
PREFETCH = 16

# Ошибка обработчика чаще всего транзиентная: сосед перезапускается, база
# моргнула. Повторить безопасно — обработчики идемпотентны, отметка об
# обработке пишется той же транзакцией, что и эффект. Поэтому событие
# возвращается в очередь, а не уходит в dead-letter с первой же осечки:
# иначе перезапуск соседа навсегда терял бы счёт или списание материалов.
RETRY_LIMIT = 5

# Пауза растёт с попытками: без неё лимит сгорает за миллисекунды, пока
# сосед ещё поднимается, и повтор не успевает ничего исправить.
RETRY_DELAY = 0.5
RETRY_DELAY_MAX = 5.0

# Повторять нечего: payload не проходит проверку, и следующая попытка
# разобьётся о то же самое.
PERMANENT = (InvalidArgumentError,)


def _attempt(message: aio_pika.abc.AbstractIncomingMessage) -> int:
    """Какая это попытка по счёту. Счётчик ведёт брокер: quorum-очередь
    проставляет x-delivery-count при каждом возврате."""
    headers = message.headers or {}
    try:
        return int(headers.get("x-delivery-count", 0)) + 1
    except (TypeError, ValueError):
        return 1


class Consumer:
    def __init__(self, amqp_url: str, queue: str, service: str = "") -> None:
        self._service = service or queue
        self._url = amqp_url
        self._queue_name = queue
        self._routes: dict[str, Handler] = {}
        self._fallback: Handler | None = None
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._task: asyncio.Task | None = None

    def handle(self, routing_key: str, handler: Handler) -> None:
        self._routes[routing_key] = handler

    def handle_all(self, handler: Handler) -> None:
        """Один обработчик на весь поток — для подписчиков с биндингом `#`."""
        self._fallback = handler

    async def start(self) -> None:
        self._connection = await aio_pika.connect_robust(self._url)
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=PREFETCH)

        queue = await channel.get_queue(self._queue_name)
        self._task = asyncio.create_task(queue.consume(self._on_message, no_ack=False))
        log.info("слушаем очередь %s", self._queue_name)

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
        if self._connection is not None:
            await self._connection.close()

    async def _on_message(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        try:
            envelope = Parse(message.body.decode(), events_pb2.EventEnvelope())
        except (ParseError, UnicodeDecodeError):
            log.exception("не разобрали событие, message_id=%s", message.message_id)
            # Ключ маршрутизации берём из свойства сообщения: конверт не разобран,
            # а без метки такие отказы не видны в статистике вовсе.
            observability.EVENTS_CONSUMED.labels(
                self._service, message.type or "unknown", "unparsable").inc()
            await message.reject(requeue=False)
            return

        # Трасса продолжается: контекст пришёл вместе с событием.
        with observability.consume_span(envelope.routing_key, envelope.traceparent):
            await self._dispatch(message, envelope)

    async def _dispatch(
        self,
        message: aio_pika.abc.AbstractIncomingMessage,
        envelope: events_pb2.EventEnvelope,
    ) -> None:
        tracing.set_current(tracing.parse(envelope.traceparent))

        handler = self._routes.get(envelope.routing_key, self._fallback)
        if handler is None:
            # На ключ никто не подписан — подтверждаем, иначе очередь встанет.
            log.warning("обработчик не найден: %s", envelope.routing_key)
            observability.EVENTS_CONSUMED.labels(
                self._service, envelope.routing_key, "skipped").inc()
            await message.ack()
            return

        try:
            await handler(envelope)
        except Exception as exc:
            await self._retry_or_bury(message, envelope, exc)
            return

        observability.EVENTS_CONSUMED.labels(
            self._service, envelope.routing_key, "handled").inc()
        await message.ack()

    async def _retry_or_bury(
        self,
        message: aio_pika.abc.AbstractIncomingMessage,
        envelope: events_pb2.EventEnvelope,
        exc: BaseException,
    ) -> None:
        attempt = _attempt(message)
        permanent = isinstance(exc, PERMANENT)
        exhausted = attempt >= RETRY_LIMIT

        if permanent or exhausted:
            log.exception(
                "обработка не удалась окончательно: %s, event_id=%s, попытка %s, trace_id=%s",
                envelope.routing_key, envelope.event_id, attempt, tracing.trace_id(),
            )
            observability.EVENTS_CONSUMED.labels(
                self._service, envelope.routing_key, "failed").inc()
            await message.reject(requeue=False)
            return

        log.warning(
            "обработка не удалась, вернём в очередь: %s, event_id=%s, попытка %s, trace_id=%s: %s",
            envelope.routing_key, envelope.event_id, attempt, tracing.trace_id(), exc,
        )
        observability.EVENTS_CONSUMED.labels(
            self._service, envelope.routing_key, "retried").inc()
        await asyncio.sleep(min(RETRY_DELAY * attempt, RETRY_DELAY_MAX))
        await message.nack(requeue=True)
