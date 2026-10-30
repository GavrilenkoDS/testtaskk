"""End-to-end check of the running demo; configuration is loaded from .env."""

import json
import time
from base64 import b64encode
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

from app.config import SmokeSettings
from app.constants import DEAD_LETTER_QUEUE, MAX_ATTEMPTS

settings = SmokeSettings()
API = settings.smoke_api_url.rstrip("/")
WEBHOOK = settings.smoke_webhook_url.rstrip("/")
RABBIT = settings.smoke_rabbit_url.rstrip("/")
KEY = settings.api_key


def request(method, url, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = Request(
        url,
        data=data,
        method=method,
        headers={
            "Content-Type": "application/json",
            **(headers or {}),
        },
    )
    with urlopen(req, timeout=15) as response:
        return response.status, json.load(response)


def wait_for(payment_id, predicate, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        _, payment = request(
            "GET", f"{API}/api/v1/payments/{payment_id}", headers={"X-API-Key": KEY}
        )
        if predicate(payment):
            return payment
        time.sleep(0.5)
    raise AssertionError(f"Payment did not reach expected state: {payment}")


def main():
    try:
        request("GET", f"{API}/health")
    except HTTPError as exc:
        assert exc.code == 401
    else:
        raise AssertionError("API must require authentication")

    payments = {}
    for path in ("webhook", "fail"):
        headers = {"X-API-Key": KEY, "Idempotency-Key": f"smoke-{uuid4()}"}
        body = {
            "amount": "42.10",
            "currency": "RUB",
            "description": "Smoke check",
            "metadata": {"smoke": True},
            "webhook_url": f"{settings.smoke_callback_url.rstrip('/')}/{path}",
        }
        status, created = request("POST", f"{API}/api/v1/payments", body, headers)
        assert status == 202
        _, duplicate = request("POST", f"{API}/api/v1/payments", body, headers)
        assert duplicate["payment_id"] == created["payment_id"]
        payments[path] = created["payment_id"]

    delivered = wait_for(payments["webhook"], lambda p: p["webhook_sent_at"] is not None)
    exhausted = wait_for(payments["fail"], lambda p: p["attempts_completed"] == MAX_ATTEMPTS)
    assert delivered["status"] in ("succeeded", "failed")
    assert delivered["attempts_completed"] == 1
    assert exhausted["status"] in ("succeeded", "failed")
    assert exhausted["webhook_sent_at"] is None
    _, events = request("GET", f"{WEBHOOK}/events")
    assert any(e["payment_id"] == payments["webhook"] for e in events)

    # Read-and-requeue keeps the DLQ event available for manual inspection.
    credentials = b64encode(
        f"{settings.rabbitmq_default_user}:{settings.rabbitmq_default_pass}".encode()
    ).decode()
    deadline = time.monotonic() + 10
    found = False
    while time.monotonic() < deadline:
        _, messages = request(
            "POST",
            f"{RABBIT}/api/queues/%2F/{DEAD_LETTER_QUEUE}/get",
            {
                "count": 100,
                "ackmode": "ack_requeue_true",
                "encoding": "auto",
            },
            {"Authorization": f"Basic {credentials}"},
        )

        def matches_payment(message):
            try:
                payload = json.loads(message["payload"])
            except (ValueError, TypeError):
                return False
            return isinstance(payload, dict) and payload.get("payment_id") == payments["fail"]

        if any(matches_payment(m) for m in messages):
            found = True
            break
        time.sleep(0.5)
    assert found, "Exhausted webhook must be published to the real DLQ"
    print(
        json.dumps(
            {"result": "passed", "delivered": delivered, "exhausted": exhausted},
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
