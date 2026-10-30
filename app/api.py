import json
import secrets
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.security import APIKeyHeader
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import engine, get_session
from app.models import Outbox, Payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetail

key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def authenticate(key: Annotated[str | None, Depends(key_header)]) -> None:
    expected = get_settings().api_key
    if key is None or not secrets.compare_digest(key.encode(), expected.encode()):
        raise HTTPException(401, "Invalid API key")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()


# OpenAPI is authenticated; a public Swagger UI is not exposed.
app = FastAPI(
    title="Async Payments",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=lifespan,
    dependencies=[Depends(authenticate)],
)


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError) -> Response:
    # Invalid Unicode can occur in error inputs too; escape it in the JSON response.
    return Response(
        content=json.dumps(
            jsonable_encoder({"detail": exc.errors()}), ensure_ascii=True, allow_nan=False
        ),
        status_code=422,
        media_type="application/json",
    )


@app.get("/openapi.json", include_in_schema=False)
async def openapi():
    return app.openapi()


@app.post("/api/v1/payments", response_model=PaymentAccepted, status_code=202)
async def create_payment(
    body: PaymentCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
):
    if not idempotency_key.strip():
        raise HTTPException(422, "Idempotency-Key must not be blank")
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
        payment = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if payment.request_hash != fingerprint:
            raise HTTPException(409, "Idempotency-Key was already used with another request")
        if inserted:
            session.add(
                Outbox(
                    routing_key="payments.new",
                    payload={
                        "payment_id": str(payment_id),
                        "attempt": 1,
                    },
                )
            )
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetail)
async def get_payment(payment_id: UUID, session: Annotated[AsyncSession, Depends(get_session)]):
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(404, "Payment not found")
    return PaymentDetail(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.extra_metadata,
        webhook_url=payment.webhook_url,
        processed_at=payment.processed_at,
        webhook_sent_at=payment.webhook_sent_at,
        attempts_completed=payment.attempts_completed,
        last_error=payment.last_error,
    )


@app.get("/health")
async def health(session: Annotated[AsyncSession, Depends(get_session)]):
    await session.execute(select(1))
    return {"status": "ok"}
