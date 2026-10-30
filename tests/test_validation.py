from decimal import Decimal
from unittest.mock import patch

import httpx
import pytest
from pydantic import ValidationError

from app.api import app
from app.processing import emulate_gateway
from app.schemas import PaymentCreate


@pytest.mark.parametrize("amount", ["0", "-1", "1.001", "NaN", "Infinity", "10000000000000000"])
def test_invalid_amount(body, amount):
    with pytest.raises(ValidationError):
        PaymentCreate(**{**body, "amount": amount})


@pytest.mark.parametrize(
    "changes",
    [
        {"currency": "GBP"},
        {"webhook_url": "ftp://example.com"},
        {"webhook_url": "http://user:pass@example.com"},
        {"description": ""},
        {"unexpected": True},
    ],
)
def test_invalid_request(body, changes):
    with pytest.raises(ValidationError):
        PaymentCreate(**{**body, **changes})


def test_fingerprint_canonicalizes_decimal_and_json_order(body):
    left = PaymentCreate(**{**body, "amount": "100.5", "metadata": {"b": 2, "a": 1}})
    right = PaymentCreate(**{**body, "amount": "100.50", "metadata": {"a": 1, "b": 2}})
    assert left.fingerprint() == right.fingerprint()
    assert left.amount == Decimal("100.50")


@pytest.mark.parametrize(
    "path", ["/health", "/openapi.json", "/api/v1/payments/00000000-0000-0000-0000-000000000000"]
)
async def test_all_get_endpoints_require_auth(path):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        assert (await c.get(path)).status_code == 401


async def test_create_requires_auth(body):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        assert (await c.post("/api/v1/payments", json=body)).status_code == 401


@pytest.mark.parametrize(("draw", "expected"), [(0.899, "succeeded"), (0.9, "failed")])
async def test_gateway_outcomes_and_delay(draw, expected):
    with (
        patch("app.processing.asyncio.sleep") as sleep,
        patch("app.processing.random.uniform", return_value=3.5) as uniform,
        patch("app.processing.random.random", return_value=draw),
    ):
        assert await emulate_gateway() == expected
        uniform.assert_called_once_with(2, 5)
        sleep.assert_awaited_once_with(3.5)
