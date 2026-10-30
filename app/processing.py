import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

import httpx
from faststream.exceptions import RejectMessage
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.models import Outbox, Payment
from app.schemas import PaymentEvent

logger = logging.getLogger(__name__)


async def emulate_gateway() -> str:
    await asyncio.sleep(random.uniform(2, 5))
    return "succeeded" if random.random() < 0.9 else "failed"


def already_handled(payment: Payment, event: PaymentEvent) -> bool:
    return payment.webhook_sent_at is not None or event.attempt <= payment.attempts_completed


async def locked_payment(session: AsyncSession, event: PaymentEvent) -> Payment:
    payment = await session.scalar(
        select(Payment).where(Payment.id == event.payment_id).with_for_update()
    )
    if payment is None:
        raise RejectMessage()
    if event.attempt > payment.attempts_completed + 1:
        raise RejectMessage()
    return payment


def record_failure(
    session: AsyncSession, payment: Payment, event: PaymentEvent, error: str
) -> None:
    payment.last_error = error
    payment.attempts_completed = event.attempt
    if event.attempt < 3:
        delay = get_settings().retry_base_seconds * 2 ** (event.attempt - 1)
        session.add(
            Outbox(
                routing_key="payments.new",
                payload={"payment_id": str(payment.id), "attempt": event.attempt + 1},
                available_at=datetime.now(UTC) + timedelta(seconds=delay),
            )
        )
    else:
        if payment.status == "pending":
            payment.status = "failed"
            payment.processed_at = datetime.now(UTC)
        session.add(
            Outbox(
                routing_key="payments.dead",
                payload={
                    **event.model_dump(mode="json"),
                    "error": error,
                    "event_id": f"payment:{payment.id}:result",
                },
            )
        )


async def retry_processing_failure(
    event: PaymentEvent,
    factory: async_sessionmaker[AsyncSession],
    error: str,
) -> None:
    async with factory() as session, session.begin():
        payment = await locked_payment(session, event)
        if not already_handled(payment, event):
            record_failure(session, payment, event, error)


async def process_payment(
    event: PaymentEvent,
    factory: async_sessionmaker[AsyncSession],
    client: httpx.AsyncClient,
    gateway: Callable[[], Awaitable[str]] = emulate_gateway,
) -> None:
    # Commit the gateway result separately, before any webhook I/O. A retry of a
    # failed webhook therefore cannot change the simulated payment result.
    async with factory() as session, session.begin():
        payment = await locked_payment(session, event)
        if already_handled(payment, event):
            return
        if payment.status == "pending":
            payment.status = await gateway()
            payment.processed_at = datetime.now(UTC)

    async with factory() as session, session.begin():
        payment = await locked_payment(session, event)
        if already_handled(payment, event):
            return
        # The stable event ID lets the recipient deduplicate at-least-once HTTP
        # delivery, including a crash between a successful POST and the DB commit.
        event_id = f"payment:{payment.id}:result"
        payload = {
            "event_id": event_id,
            "payment_id": str(payment.id),
            "status": payment.status,
            "amount": str(payment.amount),
            "currency": payment.currency,
            "metadata": payment.extra_metadata,
            "processed_at": payment.processed_at.isoformat(),
        }
        try:
            # HTTPX timeouts bound individual I/O operations. Also bound the full
            # request, including a response body that keeps arriving in small chunks.
            async with asyncio.timeout(get_settings().webhook_timeout_seconds):
                response = await client.post(
                    payment.webhook_url,
                    json=payload,
                    headers={"Idempotency-Key": event_id, "X-Event-ID": event_id},
                )
                # Redirects are deliberately not followed and are retryable failures too.
                response.raise_for_status()
        except (httpx.HTTPError, TimeoutError) as exc:
            # Do not persist exception URLs, credentials, or response bodies.
            code = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
            error = f"Webhook {type(exc).__name__}" + (f" ({code})" if code else "")
            record_failure(session, payment, event, error)
            logger.warning("Webhook failed payment_id=%s attempt=%s", payment.id, event.attempt)
        else:
            payment.webhook_sent_at = datetime.now(UTC)
            payment.last_error = None
        payment.attempts_completed = event.attempt
