import asyncio
import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.messaging import DLX, EXCHANGE
from app.models import Outbox

logger = logging.getLogger(__name__)


async def publish_one(factory: async_sessionmaker[AsyncSession], broker: RabbitBroker) -> bool:
    async with factory() as session, session.begin():
        event = await session.scalar(
            select(Outbox)
            .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
            .order_by(Outbox.available_at, Outbox.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if event is None:
            return False

        await broker.publish(
            event.payload,
            exchange=DLX if event.routing_key == "payments.dead" else EXCHANGE,
            routing_key=event.routing_key,
            persist=True,
            mandatory=True,
            message_id=str(event.id),
            timeout=get_settings().publish_timeout_seconds,
        )
        event.published_at = datetime.now(UTC)
    return True


async def relay(factory: async_sessionmaker[AsyncSession], broker: RabbitBroker) -> None:
    while True:
        try:
            if await publish_one(factory, broker):
                continue
        except Exception:
            # Rollback leaves the event unpublished. Broker outages must not lose events.
            logger.exception("Outbox publish failed; retrying")
        await asyncio.sleep(get_settings().outbox_poll_seconds)
