"""Exercise the running demo through HTTP, RabbitMQ and controlled Docker outages."""

import json
import subprocess
import time
from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from uuid import uuid4

from app.config import get_settings
from scripts.smoke import API, KEY, RABBIT, WEBHOOK, request, settings, wait_for

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "e2e-results.json"
AUTH = {"X-API-Key": KEY}
RABBIT_AUTH = {
    "Authorization": "Basic "
    + b64encode(
        f"{settings.rabbitmq_default_user}:{settings.rabbitmq_default_pass}".encode()
    ).decode()
}
RESULTS = []


def docker(*args):
    result = subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout


def eventually(check, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (HTTPError, URLError, ConnectionError):
            pass
        time.sleep(0.5)
    raise AssertionError("Timed out waiting for the expected end-to-end state")


def expect_status(expected, method, url, body=None, headers=None):
    try:
        status, response = request(method, url, body, headers)
    except HTTPError as exc:
        assert exc.code == expected, (expected, exc.code)
        return json.load(exc)
    assert status == expected, (expected, status)
    return response


def payload(path="webhook"):
    return {
        "amount": "73.25",
        "currency": "EUR",
        "description": "E2E verification",
        "metadata": {"e2e_run": uuid4().hex},
        "webhook_url": f"{settings.smoke_callback_url.rstrip('/')}/{path}",
    }


def create(body=None):
    return expect_status(
        202,
        "POST",
        f"{API}/api/v1/payments",
        body or payload(),
        {
            **AUTH,
            "Idempotency-Key": f"e2e-{uuid4()}",
        },
    )["payment_id"]


def detail(payment_id):
    return request("GET", f"{API}/api/v1/payments/{payment_id}", headers=AUTH)[1]


def outbox_rows(payment_id):
    code = """
import asyncio, json, sys
from sqlalchemy import select
from app.db import sessions, engine
from app.models import Outbox
async def main():
    async with sessions() as session:
        rows = await session.scalars(select(Outbox).where(
            Outbox.payload['payment_id'].as_string() == sys.argv[1]
        ).order_by(Outbox.created_at))
        print(json.dumps([{
            'routing_key': row.routing_key, 'payload': row.payload,
            'created_at': row.created_at, 'available_at': row.available_at,
            'published_at': row.published_at,
        } for row in rows], default=str))
    await engine.dispose()
asyncio.run(main())
"""
    return json.loads(docker("exec", "-T", "api", "python", "-c", code, payment_id))


def rabbit(method, path, body=None):
    return request(method, f"{RABBIT}/api/{path}", body, RABBIT_AUTH)[1]


def publish(body):
    encoded = json.dumps(body) if isinstance(body, dict) else body
    result = rabbit(
        "POST",
        "exchanges/%2F/payments/publish",
        {
            "properties": {"content_type": "application/json", "delivery_mode": 2},
            "routing_key": "payments.new",
            "payload": encoded,
            "payload_encoding": "string",
        },
    )
    assert result["routed"]


def dlq_contains(predicate):
    messages = rabbit(
        "POST",
        "queues/%2F/payments.dlq/get",
        {
            "count": 1000,
            "ackmode": "ack_requeue_true",
            "encoding": "auto",
        },
    )
    return any(predicate(message["payload"]) for message in messages)


def delivery_case():
    payment_id = create()
    payment = wait_for(payment_id, lambda p: p["webhook_sent_at"] is not None)
    assert payment["status"] in ("succeeded", "failed")
    assert payment["attempts_completed"] == 1
    event = next(e for e in request("GET", f"{WEBHOOK}/events")[1] if e["payment_id"] == payment_id)
    assert event["event_id"] == f"payment:{payment_id}:result"
    assert event["status"] == payment["status"]
    assert event["amount"] == "73.25"
    return {"payment_id": payment_id, "payment_status": payment["status"], "attempts": 1}


def auth_and_validation_case():
    for path in ("/health", "/openapi.json", f"/api/v1/payments/{uuid4()}"):
        expect_status(401, "GET", f"{API}{path}")
        expect_status(401, "GET", f"{API}{path}", headers={"X-API-Key": uuid4().hex})
    expect_status(401, "POST", f"{API}/api/v1/payments", payload())
    expect_status(422, "POST", f"{API}/api/v1/payments", payload(), AUTH)
    expect_status(
        422,
        "POST",
        f"{API}/api/v1/payments",
        {**payload(), "amount": "-1"},
        {
            **AUTH,
            "Idempotency-Key": f"e2e-{uuid4()}",
        },
    )
    expect_status(404, "GET", f"{API}/api/v1/payments/{uuid4()}", headers=AUTH)
    return {"unauthorized": 401, "invalid_request": 422, "missing_payment": 404}


def concurrent_idempotency_case():
    body = payload()
    headers = {**AUTH, "Idempotency-Key": f"e2e-concurrent-{uuid4()}"}

    def post(_):
        return expect_status(202, "POST", f"{API}/api/v1/payments", body, headers)

    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(post, range(10)))
    identifiers = {response["payment_id"] for response in responses}
    assert len(identifiers) == 1
    payment_id = identifiers.pop()
    expect_status(409, "POST", f"{API}/api/v1/payments", {**body, "amount": "99"}, headers)
    wait_for(payment_id, lambda p: p["webhook_sent_at"] is not None)
    rows = outbox_rows(payment_id)
    assert len(rows) == 1 and rows[0]["payload"]["attempt"] == 1
    assert rows[0]["published_at"] is not None
    return {
        "concurrent_requests": 10,
        "unique_payments": 1,
        "outbox_events": 1,
        "conflicting_request": 409,
        "payment_id": payment_id,
    }


def retry_dlq_case():
    payment_id = create(payload("fail"))
    payment = wait_for(payment_id, lambda p: p["attempts_completed"] == 3)
    assert payment["webhook_sent_at"] is None
    eventually(lambda: dlq_contains(lambda raw: payment_id in raw))
    rows = outbox_rows(payment_id)
    retries = sorted(
        (row for row in rows if row["routing_key"] == "payments.new"),
        key=lambda row: row["payload"]["attempt"],
    )
    assert [row["payload"]["attempt"] for row in retries] == [1, 2, 3]
    delays = []
    for row in retries[1:]:
        delay = (
            datetime.fromisoformat(row["available_at"]) - datetime.fromisoformat(row["created_at"])
        ).total_seconds()
        expected = get_settings().retry_base_seconds * 2 ** (row["payload"]["attempt"] - 2)
        assert delay >= expected - 0.1, (delay, expected)
        assert datetime.fromisoformat(row["published_at"]) >= datetime.fromisoformat(
            row["available_at"]
        )
        delays.append(round(delay, 3))
    assert len([row for row in rows if row["routing_key"] == "payments.dead"]) == 1
    return {
        "payment_id": payment_id,
        "attempts": 3,
        "retry_delay_seconds": delays,
        "dlq": True,
        "payment_status": payment["status"],
    }


def consumer_outage_case():
    docker("stop", "consumer")
    try:
        payment_id = create()
        assert detail(payment_id)["status"] == "pending"
        rows = outbox_rows(payment_id)
        assert len(rows) == 1 and rows[0]["published_at"] is None
    finally:
        docker("start", "consumer")
    wait_for(payment_id, lambda p: p["webhook_sent_at"] is not None, timeout=60)
    assert outbox_rows(payment_id)[0]["published_at"] is not None
    return {
        "payment_id": payment_id,
        "pending_during_outage": True,
        "delivered_after_restart": True,
    }


def broker_outage_case():
    docker("stop", "rabbitmq")
    try:
        payment_id = create()
        assert detail(payment_id)["status"] == "pending"
        assert outbox_rows(payment_id)[0]["published_at"] is None
    finally:
        docker("start", "rabbitmq")
    wait_for(payment_id, lambda p: p["webhook_sent_at"] is not None, timeout=90)
    assert outbox_rows(payment_id)[0]["published_at"] is not None
    return {
        "payment_id": payment_id,
        "accepted_during_broker_outage": True,
        "delivered_after_reconnect": True,
    }


def webhook_recovery_case():
    docker("stop", "webhook")
    try:
        payment_id = create()
        before = wait_for(payment_id, lambda p: p["attempts_completed"] >= 1)
        assert before["webhook_sent_at"] is None
        assert before["attempts_completed"] < 3
    finally:
        docker("start", "webhook")
    after = wait_for(payment_id, lambda p: p["webhook_sent_at"] is not None)
    assert after["attempts_completed"] in (2, 3)
    assert after["processed_at"] == before["processed_at"]
    assert after["status"] == before["status"]
    assert after["last_error"] is None
    return {
        "payment_id": payment_id,
        "attempts": after["attempts_completed"],
        "gateway_result_preserved": True,
    }


def duplicate_event_case():
    payment_id = create()
    before = wait_for(payment_id, lambda p: p["webhook_sent_at"] is not None)
    log_count = docker("logs", "--no-color", "webhook").count("POST /webhook")
    queue = rabbit("GET", "queues/%2F/payments.new")
    ack_before = queue.get("message_stats", {}).get("ack", 0)
    publish({"payment_id": payment_id, "attempt": 1})
    eventually(
        lambda: rabbit("GET", "queues/%2F/payments.new").get("message_stats", {}).get("ack", 0)
        > ack_before,
        timeout=20,
    )
    after = detail(payment_id)
    assert after == before
    assert docker("logs", "--no-color", "webhook").count("POST /webhook") == log_count
    return {
        "payment_id": payment_id,
        "duplicate_acknowledged": True,
        "additional_webhook_requests": 0,
    }


def poison_event_case():
    poison = "{invalid-e2e-" + uuid4().hex
    publish(poison)
    eventually(lambda: dlq_contains(lambda raw: raw == poison))
    return {"malformed_json": "rejected", "dlq": True}


def run_case(name, action):
    started = time.monotonic()
    try:
        details = action()
    except Exception as exc:
        RESULTS.append({"case": name, "status": "failed", "error": f"{type(exc).__name__}: {exc}"})
        raise
    else:
        RESULTS.append(
            {
                "case": name,
                "status": "passed",
                "seconds": round(time.monotonic() - started, 2),
                "details": details,
            }
        )
        print(f"PASS {name}", flush=True)


def main():
    cases = [
        ("authorization_and_validation", auth_and_validation_case),
        ("payment_and_webhook", delivery_case),
        ("concurrent_idempotency_and_conflict", concurrent_idempotency_case),
        ("three_attempts_exponential_backoff_and_dlq", retry_dlq_case),
        ("outbox_survives_consumer_outage", consumer_outage_case),
        ("outbox_survives_broker_outage", broker_outage_case),
        ("webhook_recovers_without_reprocessing_payment", webhook_recovery_case),
        ("duplicate_broker_event_is_ignored", duplicate_event_case),
        ("malformed_json_goes_to_dlq", poison_event_case),
    ]
    try:
        eventually(lambda: request("GET", f"{API}/health", headers=AUTH)[0] == 200)
        for name, action in cases:
            run_case(name, action)
    finally:
        REPORT.parent.mkdir(exist_ok=True)
        REPORT.write_text(
            json.dumps(
                {
                    "finished_at": datetime.now(UTC).isoformat(),
                    "passed": sum(result["status"] == "passed" for result in RESULTS),
                    "total": len(cases),
                    "cases": RESULTS,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    print(f"{len(RESULTS)}/{len(cases)} E2E cases passed; report: {REPORT}")


if __name__ == "__main__":
    main()
