import asyncio
import logging
import uuid
from datetime import UTC, datetime

import aio_pika
from google.protobuf.json_format import MessageToJson
from google.protobuf.message import Message
from mirea.events.v1 import events_pb2

from mireacrm_common import identity, observability, tracing

log = logging.getLogger(__name__)

EXCHANGE = "mirea.events"


class EventPublisher:
    def __init__(self, amqp_url: str, producer: str) -> None:
        self._url = amqp_url
        self._producer = producer
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._exchange: aio_pika.abc.AbstractExchange | None = None

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(self._url)
        channel = await self._connection.channel(publisher_confirms=True)
        self._exchange = await channel.declare_exchange(
            EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
        )
        log.info("подключён к RabbitMQ, обменник %s", EXCHANGE)

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()

    async def ping(self, timeout: float = 2.0) -> bool:
        """Активная проверка: robust-соединение не признаётся закрытым, пока
        переподключается, поэтому судить по его флагам нельзя."""
        connection = self._connection
        if connection is None or connection.is_closed:
            return False
        try:
            async with asyncio.timeout(timeout):
                channel = await connection.channel()
                await channel.declare_exchange(
                    EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True, passive=True
                )
                await channel.close()
        except Exception:
            return False
        return True

    async def publish(self, routing_key: str, **payload: Message) -> None:
        if self._exchange is None:
            raise RuntimeError("publisher не подключён")

        with observability.publish_span(routing_key):
            envelope = events_pb2.EventEnvelope(
                event_id=str(uuid.uuid4()),
                routing_key=routing_key,
                producer=self._producer,
                traceparent=tracing.current(),
                actor=identity.current().subject,
                **payload,
            )
            envelope.occurred_at.FromDatetime(datetime.now(UTC))

            await self._exchange.publish(
                aio_pika.Message(
                    body=MessageToJson(envelope, indent=0).encode(),
                    content_type="application/json",
                    message_id=envelope.event_id,
                    type=routing_key,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=routing_key,
            )
        observability.EVENTS_PUBLISHED.labels(self._producer, routing_key).inc()
        log.info("опубликовано %s (%s)", routing_key, envelope.event_id)

