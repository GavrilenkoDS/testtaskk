from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue

from app.config import get_settings
from app.constants import (
    DEAD_LETTER_EXCHANGE,
    DEAD_LETTER_QUEUE,
    DEAD_LETTER_ROUTING_KEY,
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    PAYMENTS_ROUTING_KEY,
)

EXCHANGE = RabbitExchange(PAYMENTS_EXCHANGE, durable=True)
DLX = RabbitExchange(DEAD_LETTER_EXCHANGE, durable=True)
QUEUE = RabbitQueue(
    PAYMENTS_QUEUE,
    durable=True,
    routing_key=PAYMENTS_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
        "x-dead-letter-routing-key": DEAD_LETTER_ROUTING_KEY,
    },
)
DLQ = RabbitQueue(DEAD_LETTER_QUEUE, durable=True, routing_key=DEAD_LETTER_ROUTING_KEY)


def make_broker() -> RabbitBroker:
    return RabbitBroker(
        get_settings().rabbitmq_url,
        max_consumers=1,
        publisher_confirms=True,
        on_return_raises=True,
    )


async def declare_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(EXCHANGE)
    dlx = await broker.declare_exchange(DLX)
    queue = await broker.declare_queue(QUEUE)
    dlq = await broker.declare_queue(DLQ)
    await queue.bind(exchange, routing_key=PAYMENTS_ROUTING_KEY)
    await dlq.bind(dlx, routing_key=DEAD_LETTER_ROUTING_KEY)
