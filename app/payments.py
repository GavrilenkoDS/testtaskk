from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.constants import PAYMENTS_ROUTING_KEY
from app.models import Outbox, Payment
from app.schemas import PaymentCreate


class IdempotencyConflict(Exception):
    pass


async def get_or_create_payment(
    session: AsyncSession, body: PaymentCreate, idempotency_key: str
) -> Payment:
    fingerprint = body.fingerprint()
    payment_id = uuid4()
    async with session.begin():
        inserted = await session.scalar(
            insert(Payment)
            .values(
                id=payment_id,
                amount=body.amount,
                currency=body.currency,
                description=body.description,
                extra_metadata=body.metadata,
                webhook_url=str(body.webhook_url),
                idempotency_key=idempotency_key,
                request_hash=fingerprint,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment.id)
        )
        payment = (
            await session.scalars(select(Payment).where(Payment.idempotency_key == idempotency_key))
        ).one()
        if payment.request_hash != fingerprint:
            raise IdempotencyConflict("Idempotency-Key was already used with another request")
        if inserted:
            session.add(
                Outbox(
                    routing_key=PAYMENTS_ROUTING_KEY,
                    payload={"payment_id": str(payment_id), "attempt": 1},
                )
            )
    return payment
