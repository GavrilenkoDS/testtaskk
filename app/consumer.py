import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Annotated

import httpx
from faststream import Context, ContextRepo, FastStream
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
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(context: ContextRepo) -> AsyncGenerator[None, None]:
    async with AsyncExitStack() as stack:
        stack.push_async_callback(engine.dispose)
        client = await stack.enter_async_context(
            httpx.AsyncClient(
                timeout=get_settings().webhook_timeout_seconds,
                follow_redirects=False,
                trust_env=False,
            )
        )
        stack.enter_context(context.scope("http_client", client))
        await stack.enter_async_context(broker)
        await declare_topology(broker)
        relay_task = asyncio.create_task(relay(sessions, broker))
        try:
            yield
        finally:
            relay_task.cancel()
            await asyncio.gather(relay_task, return_exceptions=True)


app = FastStream(broker, lifespan=lifespan)


@broker.subscriber(QUEUE, EXCHANGE, retry=True, decoder=lambda message: message.body)
async def handle_payment(
    message: bytes, client: Annotated[httpx.AsyncClient, Context("http_client")]
) -> None:
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
