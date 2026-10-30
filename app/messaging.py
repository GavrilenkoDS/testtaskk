from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue

from app.config import get_settings

EXCHANGE = RabbitExchange("payments", durable=True)
DLX = RabbitExchange("payments.dlx", durable=True)
QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key="payments.new",
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.dead",
    },
)
DLQ = RabbitQueue("payments.dlq", durable=True, routing_key="payments.dead")


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
    await queue.bind(exchange, routing_key="payments.new")
    await dlq.bind(dlx, routing_key="payments.dead")
