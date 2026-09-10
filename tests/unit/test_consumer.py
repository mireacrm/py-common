"""Политика повторов потребителя.

Обработчик падает по двум разным причинам, и различать их обязательно.
Сосед перезапускается — повторить; payload не проходит проверку — повторять
нечего. До появления этих правил любая осечка уводила событие в dead-letter
с первой попытки, и перезапуск соседа навсегда терял счёт или списание.
"""

import pytest
from mirea.events.v1 import events_pb2

from mireacrm_common import consumer as consumer_module
from mireacrm_common.consumer import RETRY_LIMIT, Consumer
from mireacrm_common.errors import InvalidArgumentError, UnavailableError


class FakeMessage:
    """Ровно та часть сообщения aio_pika, которой пользуется потребитель."""

    def __init__(self, envelope: events_pb2.EventEnvelope, delivered: int = 0) -> None:
        from google.protobuf.json_format import MessageToJson

        self.body = MessageToJson(envelope, indent=0).encode()
        self.message_id = envelope.event_id
        self.type = envelope.routing_key
        self.headers = {"x-delivery-count": delivered} if delivered else {}
        self.acked = False
        self.nacked_requeue: bool | None = None
        self.rejected = False

    async def ack(self) -> None:
        self.acked = True

    async def nack(self, requeue: bool = True) -> None:
        self.nacked_requeue = requeue

    async def reject(self, requeue: bool = False) -> None:
        self.rejected = True


def envelope(routing_key: str = "appointment.completed") -> events_pb2.EventEnvelope:
    return events_pb2.EventEnvelope(
        event_id="7f4c1e2a-0000-4000-8000-000000000001",
        routing_key=routing_key,
        producer="tests",
    )


@pytest.fixture(autouse=True)
def _no_waiting(monkeypatch):
    """Пауза перед возвратом здесь не нужна: проверяется решение, а не сон."""
    monkeypatch.setattr(consumer_module, "RETRY_DELAY", 0.0)
    monkeypatch.setattr(consumer_module, "RETRY_DELAY_MAX", 0.0)


def consumer_with(handler) -> Consumer:
    consumer = Consumer("amqp://unused", "tests.events", "tests")
    consumer.handle("appointment.completed", handler)
    return consumer


class TestRetry:
    async def test_unavailable_neighbour_is_retried(self) -> None:
        """Сосед недоступен — событие возвращается в очередь, а не хоронится."""

        async def handler(_):
            raise UnavailableError("booking")

        message = FakeMessage(envelope())
        await consumer_with(handler)._dispatch(message, envelope())

        assert message.nacked_requeue is True
        assert message.rejected is False

    async def test_retries_are_bounded(self) -> None:
        """Повтор не бесконечен: исчерпав попытки, событие уходит в dead-letter."""

        async def handler(_):
            raise UnavailableError("booking")

        message = FakeMessage(envelope(), delivered=RETRY_LIMIT - 1)
        await consumer_with(handler)._dispatch(message, envelope())

        assert message.rejected is True
        assert message.nacked_requeue is None

    async def test_invalid_payload_is_not_retried(self) -> None:
        """Повторять нечего: следующая попытка разобьётся о то же самое."""

        async def handler(_):
            raise InvalidArgumentError("branch_id не разобран")

        message = FakeMessage(envelope())
        await consumer_with(handler)._dispatch(message, envelope())

        assert message.rejected is True
        assert message.nacked_requeue is None

    async def test_success_is_acknowledged(self) -> None:
        async def handler(_):
            return None

        message = FakeMessage(envelope())
        await consumer_with(handler)._dispatch(message, envelope())

        assert message.acked is True

    async def test_unsubscribed_key_is_acknowledged(self) -> None:
        """Иначе очередь встанет на событии, которое никому не нужно."""

        async def handler(_):
            raise AssertionError("не должен вызываться")

        message = FakeMessage(envelope("stock.low"))
        await consumer_with(handler)._dispatch(message, envelope("stock.low"))

        assert message.acked is True
