import asyncio
import logging

import httpx
from faststream import FastStream
from faststream.exceptions import RejectMessage
from pydantic import ValidationError

from app.config import get_settings
from app.db import engine, sessions
from app.messaging import EXCHANGE, QUEUE, declare_topology, make_broker
from app.outbox import relay
from app.processing import process_payment, retry_processing_failure
from app.schemas import PaymentEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
broker = make_broker()
app = FastStream(broker)
client: httpx.AsyncClient
relay_task: asyncio.Task | None = None
logger = logging.getLogger(__name__)


@broker.subscriber(QUEUE, EXCHANGE, retry=True, decoder=lambda message: message.body)
async def handle_payment(message: bytes) -> None:
    try:
        event = PaymentEvent.model_validate_json(message)
    except ValidationError as exc:
        raise RejectMessage() from exc
    try:
        await process_payment(event, sessions, client)
    except RejectMessage:
        raise
    except Exception as exc:
        logger.exception(
            "Processing failed payment_id=%s attempt=%s", event.payment_id, event.attempt
        )

        try:
            await retry_processing_failure(event, sessions, f"Processing {type(exc).__name__}")
        except Exception:
            # Avoid a hot requeue loop during a database outage.
            await asyncio.sleep(get_settings().database_requeue_delay_seconds)
            raise


@app.on_startup
async def prepare() -> None:
    global client
    await broker.connect()
    await declare_topology(broker)
    client = httpx.AsyncClient(
        timeout=get_settings().webhook_timeout_seconds,
        follow_redirects=False,
        trust_env=False,
    )


@app.after_startup
async def startup() -> None:
    global relay_task
    relay_task = asyncio.create_task(relay(sessions, broker))


@app.on_shutdown
async def shutdown() -> None:
    if relay_task:
        relay_task.cancel()
        await asyncio.gather(relay_task, return_exceptions=True)


@app.after_shutdown
async def cleanup() -> None:
    if "client" in globals():
        await client.aclose()
    await engine.dispose()
