import os
from unittest.mock import AsyncMock
from uuid import uuid4

import aio_pika
import httpx
import pytest
import pytest_asyncio
from faststream.rabbit import RabbitBroker, RabbitExchange
from sqlalchemy import select

from app.models import Outbox
from app.outbox import publish_one
from app.payments import get_or_create_payment
from app.processing import process_payment
from app.schemas import PaymentCreate, PaymentEvent


@pytest_asyncio.fixture
async def rabbit_topology(monkeypatch):
    url = os.getenv("TEST_RABBITMQ_URL")
    if not url:
        pytest.skip("Set TEST_RABBITMQ_URL for real broker integration")
    name = f"payments.test.{uuid4().hex}"
    exchange = RabbitExchange(name, durable=True)
    monkeypatch.setattr("app.outbox.EXCHANGE", exchange)
    connection = await aio_pika.connect_robust(url)
    async with connection:
        channel = await connection.channel()
        actual_exchange = await channel.declare_exchange(name, durable=True)
        queue = await channel.declare_queue(name, durable=True)
        await queue.bind(actual_exchange, routing_key="payments.new")
        try:
            async with RabbitBroker(url, publisher_confirms=True, on_return_raises=True) as broker:
                await broker.declare_exchange(exchange)
                yield broker, queue, actual_exchange
        finally:
            await queue.delete()
            await actual_exchange.delete()


async def test_real_broker_confirm_and_persistent_message(factory, rabbit_topology):
    broker, queue, _ = rabbit_topology
    async with factory() as session, session.begin():
        session.add(Outbox(routing_key="payments.new", payload={"hello": "world"}))
    assert await publish_one(factory, broker)
    message = await queue.get(timeout=5)
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert b"world" in message.body
    await message.ack()
    async with factory() as session:
        assert (await session.scalar(select(Outbox))).published_at is not None


async def test_unroutable_publish_keeps_outbox_unpublished(factory, rabbit_topology):
    broker, queue, exchange = rabbit_topology
    await queue.unbind(exchange, routing_key="payments.new")
    async with factory() as session, session.begin():
        session.add(Outbox(routing_key="payments.new", payload={"hello": "world"}))
    with pytest.raises(aio_pika.exceptions.DeliveryError):
        await publish_one(factory, broker)
    async with factory() as session:
        assert (await session.scalar(select(Outbox))).published_at is None


async def test_crash_after_confirm_republishes_and_consumer_deduplicates(
    factory, body, rabbit_topology
):
    broker, queue, _ = rabbit_topology
    async with factory() as session:
        payment = await get_or_create_payment(session, PaymentCreate(**body), "crash-after-confirm")
    async with factory() as session:
        event_id = (await session.scalar(select(Outbox))).id

    async def publish_then_fail(*args, **kwargs):
        await broker.publish(*args, **kwargs)
        raise RuntimeError("Crash after broker confirmation")

    crashing_broker = AsyncMock()
    crashing_broker.publish.side_effect = publish_then_fail
    with pytest.raises(RuntimeError, match="Crash after broker confirmation"):
        await publish_one(factory, crashing_broker)
    async with factory() as session:
        assert (await session.get(Outbox, event_id)).published_at is None
    assert await publish_one(factory, broker)

    requests = []
    gateway = AsyncMock(return_value="succeeded")

    def receive(request):
        requests.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        for _ in range(2):
            message = await queue.get(timeout=5)
            assert message.message_id == str(event_id)
            event = PaymentEvent.model_validate_json(message.body)
            assert event.payment_id == payment.id
            await process_payment(event, factory, client, gateway)
            await message.ack()
    gateway.assert_awaited_once()
    assert len(requests) == 1
    async with factory() as session:
        assert (await session.get(Outbox, event_id)).published_at is not None
