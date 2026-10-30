import os
from uuid import uuid4

import aio_pika
import pytest
from faststream.rabbit import RabbitBroker, RabbitExchange
from sqlalchemy import select

from app.models import Outbox
from app.outbox import publish_one


async def test_real_broker_confirm_and_persistent_message(factory, monkeypatch):
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
            async with factory() as session, session.begin():
                session.add(Outbox(routing_key="payments.new", payload={"hello": "world"}))
            async with RabbitBroker(url, publisher_confirms=True, on_return_raises=True) as broker:
                await broker.declare_exchange(exchange)
                assert await publish_one(factory, broker)
            message = await queue.get(timeout=5)
            assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
            assert b"world" in message.body
            await message.ack()
            async with factory() as session:
                assert (await session.scalar(select(Outbox))).published_at is not None
        finally:
            await queue.delete()
            await actual_exchange.delete()


async def test_unroutable_publish_keeps_outbox_unpublished(factory, monkeypatch):
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
        try:
            async with factory() as session, session.begin():
                session.add(Outbox(routing_key="payments.new", payload={"hello": "world"}))
            async with RabbitBroker(url, publisher_confirms=True, on_return_raises=True) as broker:
                await broker.declare_exchange(exchange)
                with pytest.raises(aio_pika.exceptions.DeliveryError):
                    await publish_one(factory, broker)
            async with factory() as session:
                assert (await session.scalar(select(Outbox))).published_at is None
        finally:
            await actual_exchange.delete()
