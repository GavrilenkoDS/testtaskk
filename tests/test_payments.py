import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import func, select

from app.config import get_settings
from app.models import Outbox, Payment
from app.outbox import publish_one
from app.processing import process_payment, retry_processing_failure
from app.schemas import PaymentEvent


async def create(api_client, body, key="order-1"):
    response = await api_client.post(
        "/api/v1/payments", json=body, headers={"Idempotency-Key": key}
    )
    assert response.status_code == 202, response.text
    return PaymentEvent(payment_id=response.json()["payment_id"], attempt=1)


async def test_create_get_and_atomic_outbox(api_client, body, factory):
    event = await create(api_client, body)
    detail = (await api_client.get(f"/api/v1/payments/{event.payment_id}")).json()
    assert detail["amount"] == "100.50"
    assert detail["metadata"] == body["metadata"]
    assert detail["status"] == "pending"
    assert detail["processed_at"] is None
    async with factory() as session:
        outbox = (await session.scalars(select(Outbox))).one()
        assert outbox.payload == event.model_dump(mode="json")
        assert outbox.published_at is None


async def test_concurrent_idempotency(api_client, body, factory):
    responses = await asyncio.gather(
        *[
            api_client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": "same"})
            for _ in range(10)
        ]
    )
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_idempotency_conflict(api_client, body):
    await create(api_client, body)
    response = await api_client.post(
        "/api/v1/payments", json={**body, "amount": "101"}, headers={"Idempotency-Key": "order-1"}
    )
    assert response.status_code == 409


async def test_required_header_and_missing_payment(api_client, body):
    assert (await api_client.post("/api/v1/payments", json=body)).status_code == 422
    assert (
        await api_client.get("/api/v1/payments/00000000-0000-0000-0000-000000000000")
    ).status_code == 404


@pytest.mark.parametrize("status", ["succeeded", "failed"])
async def test_processing_and_duplicate_delivery(api_client, body, factory, status):
    event = await create(api_client, body)
    gateway = AsyncMock(return_value=status)
    requests = []

    def receive(request):
        requests.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        await asyncio.gather(*[process_payment(event, factory, client, gateway) for _ in range(3)])
    gateway.assert_awaited_once()
    assert len(requests) == 1
    assert requests[0].headers["Idempotency-Key"] == f"payment:{event.payment_id}:result"
    async with factory() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.status == status
        assert payment.webhook_sent_at is not None


async def test_three_attempts_backoff_dlq_and_no_second_charge(api_client, body, factory):
    event = await create(api_client, body)
    gateway = AsyncMock(return_value="succeeded")
    calls = []

    def receive(request):
        calls.append(request)
        return httpx.Response(503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        for attempt in range(1, 4):
            started = datetime.now(UTC)
            current = event.model_copy(update={"attempt": attempt})
            await process_payment(current, factory, client, gateway)
            await process_payment(current, factory, client, gateway)  # Duplicate is ignored.
            async with factory() as session:
                payment = await session.get(Payment, event.payment_id)
                assert payment.attempts_completed == attempt
                assert payment.status == "succeeded"
                assert payment.webhook_sent_at is None
                if attempt < 3:
                    retry = await session.scalar(
                        select(Outbox).where(Outbox.payload["attempt"].as_integer() == attempt + 1)
                    )
                    delay = get_settings().retry_base_seconds * 2 ** (attempt - 1)
                    assert (retry.available_at - started).total_seconds() >= delay
        async with factory() as session:
            dead = (
                await session.scalars(select(Outbox).where(Outbox.routing_key == "payments.dead"))
            ).one()
            assert dead.payload["attempt"] == 3
            assert "503" in dead.payload["error"]
    gateway.assert_awaited_once()
    assert len(calls) == 3


async def test_webhook_recovers_on_second_attempt(api_client, body, factory):
    event = await create(api_client, body)
    gateway = AsyncMock(return_value="failed")
    responses = iter([503, 200])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(next(responses)))
    ) as client:
        await process_payment(event, factory, client, gateway)
        await process_payment(event.model_copy(update={"attempt": 2}), factory, client, gateway)
    async with factory() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.status == "failed"
        assert payment.webhook_sent_at is not None
        assert payment.last_error is None
    gateway.assert_awaited_once()


async def test_outbox_failure_rolls_back_then_recovers(api_client, body, factory):
    await create(api_client, body)
    broker = AsyncMock()
    broker.publish.side_effect = ConnectionError("Broker unavailable")
    with pytest.raises(ConnectionError):
        await publish_one(factory, broker)
    async with factory() as session:
        assert (await session.scalar(select(Outbox))).published_at is None
    broker.publish.side_effect = None
    assert await publish_one(factory, broker)
    assert not await publish_one(factory, broker)
    assert broker.publish.call_args.kwargs["persist"] is True
    assert broker.publish.call_args.kwargs["mandatory"] is True


async def test_outbox_skips_future_events(factory):
    from datetime import timedelta

    async with factory() as session, session.begin():
        session.add(
            Outbox(
                routing_key="payments.new",
                payload={},
                available_at=datetime.now(UTC) + timedelta(hours=1),
            )
        )
    broker = AsyncMock()
    assert not await publish_one(factory, broker)
    broker.publish.assert_not_called()


async def test_outbox_parallel_relays_do_not_publish_same_row(api_client, body, factory):
    await create(api_client, body)
    broker = AsyncMock()
    results = await asyncio.gather(publish_one(factory, broker), publish_one(factory, broker))
    assert sorted(results) == [False, True]
    broker.publish.assert_awaited_once()


async def test_processing_failures_exhaust_to_dlq(api_client, body, factory):
    event = await create(api_client, body)
    for attempt in range(1, 4):
        await retry_processing_failure(
            event.model_copy(update={"attempt": attempt}),
            factory,
            "Processing RuntimeError",
        )
    async with factory() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.status == "failed"
        assert payment.processed_at is not None
        assert payment.attempts_completed == 3
        assert (
            await session.scalar(
                select(func.count())
                .select_from(Outbox)
                .where(Outbox.routing_key == "payments.dead")
            )
            == 1
        )


async def test_webhook_timeout_schedules_retry(api_client, body, factory):
    event = await create(api_client, body)

    def timeout(request):
        raise httpx.ReadTimeout("timed out", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(timeout)) as client:
        await process_payment(event, factory, client, AsyncMock(return_value="succeeded"))
    async with factory() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.attempts_completed == 1
        assert payment.last_error == "Webhook ReadTimeout"
        assert payment.webhook_sent_at is None


@pytest.mark.parametrize("invalid", ["\x00", "\ud800", "\udfff"])
@pytest.mark.parametrize("location", ["description", "value", "key", "array"])
async def test_invalid_text_returns_422_without_writes(
    api_client, body, factory, invalid, location
):
    changes = {
        "description": {"description": f"before{invalid}after"},
        "value": {"metadata": {"nested": {"note": invalid}}},
        "key": {"metadata": {"nested": {invalid: "note"}}},
        "array": {"metadata": {"nested": [None, 1, {"note": invalid}]}},
    }
    # Escape surrogates on the wire so the request reaches server-side validation.
    response = await api_client.post(
        "/api/v1/payments",
        content=json.dumps({**body, **changes[location]}).encode(),
        headers={"Idempotency-Key": "invalid-text", "Content-Type": "application/json"},
    )
    assert response.status_code == 422, response.text
    assert response.json()["detail"][0]["loc"] == [
        "body",
        "description" if location == "description" else "metadata",
    ]
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_valid_unicode_round_trips(api_client, body):
    body = {
        **body,
        "description": "Оплата заказа 😀\nСпасибо",
        "metadata": {"ключ😀": [{"текст": "Привет 🌍", "число": 1, "флаг": True}, None]},
    }
    event = await create(api_client, body)
    detail = (await api_client.get(f"/api/v1/payments/{event.payment_id}")).json()
    assert detail["description"] == body["description"]
    assert detail["metadata"] == body["metadata"]


async def test_webhook_total_deadline_retries_then_dlq(api_client, body, factory, monkeypatch):
    event = await create(api_client, body)
    gateway = AsyncMock(return_value="succeeded")
    monkeypatch.setattr(get_settings(), "webhook_timeout_seconds", 0.05)
    streams = []

    class SlowResponse(httpx.AsyncByteStream):
        closed = False
        chunks_sent = 0

        async def __aiter__(self):
            while True:
                await asyncio.sleep(0.005)
                self.chunks_sent += 1
                yield b"x"

        async def aclose(self):
            self.closed = True

    def receive(request):
        stream = SlowResponse()
        streams.append(stream)
        return httpx.Response(200, stream=stream)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        # The watchdog makes the old implementation fail instead of hanging the suite.
        async with asyncio.timeout(2):
            for attempt in range(1, 4):
                await process_payment(
                    event.model_copy(update={"attempt": attempt}), factory, client, gateway
                )
                async with factory() as session:
                    payment = await session.get(Payment, event.payment_id)
                    assert payment.status == "succeeded"
                    assert payment.attempts_completed == attempt
                    assert payment.webhook_sent_at is None
                    assert payment.last_error == "Webhook TimeoutError"
                    if attempt < 3:
                        retry = await session.scalar(
                            select(Outbox).where(
                                Outbox.payload["attempt"].as_integer() == attempt + 1
                            )
                        )
                        assert retry is not None
            # A timed-out delivery releases the lock and lets another payment finish.
            next_event = await create(api_client, body, key="after-slow-webhook")
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(lambda request: httpx.Response(200))
            ) as fast_client:
                await process_payment(
                    next_event, factory, fast_client, AsyncMock(return_value="succeeded")
                )

    gateway.assert_awaited_once()
    assert len(streams) == 3
    assert all(stream.closed and stream.chunks_sent > 0 for stream in streams)
    async with factory() as session:
        dead = (
            await session.scalars(select(Outbox).where(Outbox.routing_key == "payments.dead"))
        ).one()
        assert dead.payload["attempt"] == 3
        assert dead.payload["error"] == "Webhook TimeoutError"
        assert (await session.get(Payment, next_event.payment_id)).webhook_sent_at is not None
