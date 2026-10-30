import json
import secrets
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.security import APIKeyHeader
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import engine, get_session
from app.models import Payment
from app.payments import IdempotencyConflict, get_or_create_payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetail

key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def authenticate(key: Annotated[str | None, Depends(key_header)]) -> None:
    expected = get_settings().api_key
    if key is None or not secrets.compare_digest(key.encode(), expected.encode()):
        raise HTTPException(401, "Invalid API key")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
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
    # Invalid inputs can contain infinity, invalid Unicode or excessive nesting.
    detail = [{key: error[key] for key in ("loc", "msg", "type")} for error in exc.errors()]
    return Response(
        content=json.dumps({"detail": detail}, ensure_ascii=True, allow_nan=False),
        status_code=422,
        media_type="application/json",
    )


@app.get("/openapi.json", include_in_schema=False)
async def openapi() -> dict[str, Any]:
    return app.openapi()


@app.post("/api/v1/payments", response_model=PaymentAccepted, status_code=202)
async def create_payment(
    body: PaymentCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
) -> Payment:
    if not idempotency_key.strip():
        raise HTTPException(422, "Idempotency-Key must not be blank")
    try:
        return await get_or_create_payment(session, body, idempotency_key)
    except IdempotencyConflict as exc:
        raise HTTPException(409, str(exc)) from exc


@app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetail)
async def get_payment(
    payment_id: UUID, session: Annotated[AsyncSession, Depends(get_session)]
) -> Payment:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(404, "Payment not found")
    return payment


@app.get("/health", response_model=None)
async def health(session: Annotated[AsyncSession, Depends(get_session)]) -> dict[str, str]:
    await session.execute(select(1))
    return {"status": "ok"}
