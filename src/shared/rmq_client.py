"""RabbitMQ client for message queue operations."""

import asyncio
import json
from typing import Any, Callable, Coroutine

import aio_pika
from aio_pika import Channel, Connection, ExchangeType, Message, Queue
from aio_pika.abc import AbstractIncomingMessage
import structlog

from src.config import RabbitMQSettings, get_settings

logger = structlog.get_logger()


# Queue and Exchange configuration
EXCHANGES = {
    "parser.direct": {
        "type": ExchangeType.DIRECT,
        "durable": True,
    },
    "parser.dlq": {
        "type": ExchangeType.DIRECT,
        "durable": True,
    },
}

QUEUES = {
    "tasks.http": {
        "durable": True,
        "arguments": {
            "x-max-priority": 10,
            "x-dead-letter-exchange": "parser.dlq",
            "x-dead-letter-routing-key": "dlq.tasks",
        },
    },
    "tasks.browser": {
        "durable": True,
        "arguments": {
            "x-max-priority": 10,
            "x-dead-letter-exchange": "parser.dlq",
            "x-dead-letter-routing-key": "dlq.tasks",
        },
    },
    "results": {
        "durable": True,
        "arguments": {},
    },
    "dlq.tasks": {
        "durable": True,
        "arguments": {
            "x-message-ttl": 604800000,  # 7 days
        },
    },
}

BINDINGS = [
    {"queue": "tasks.http", "exchange": "parser.direct", "routing_key": "task.http"},
    {"queue": "tasks.browser", "exchange": "parser.direct", "routing_key": "task.browser"},
    {"queue": "results", "exchange": "parser.direct", "routing_key": "result"},
    {"queue": "dlq.tasks", "exchange": "parser.dlq", "routing_key": "dlq.tasks"},
]


class RabbitMQClient:
    """Async RabbitMQ client for Universal Parser."""

    def __init__(self, settings: RabbitMQSettings | None = None):
        self.settings = settings or get_settings().rmq
        self._connection: Connection | None = None
        self._channel: Channel | None = None
        self._queues: dict[str, Queue] = {}
        self._exchanges: dict[str, aio_pika.Exchange] = {}

    async def connect(self) -> None:
        """Establish connection to RabbitMQ."""
        if self._connection and not self._connection.is_closed:
            return

        logger.info("Connecting to RabbitMQ", url=self.settings.url)

        self._connection = await aio_pika.connect_robust(
            self.settings.url,
            timeout=30,
        )
        self._channel = await self._connection.channel()

        # Setup exchanges, queues, and bindings
        await self._setup_topology()

        logger.info("Connected to RabbitMQ")

    async def _setup_topology(self) -> None:
        """Setup exchanges, queues, and bindings."""
        if not self._channel:
            raise RuntimeError("Channel not initialized")

        # Declare exchanges
        for name, config in EXCHANGES.items():
            exchange = await self._channel.declare_exchange(
                name,
                type=config["type"],
                durable=config["durable"],
            )
            self._exchanges[name] = exchange
            logger.debug("Declared exchange", name=name)

        # Declare queues
        for name, config in QUEUES.items():
            queue = await self._channel.declare_queue(
                name,
                durable=config["durable"],
                arguments=config.get("arguments", {}),
            )
            self._queues[name] = queue
            logger.debug("Declared queue", name=name)

        # Setup bindings
        for binding in BINDINGS:
            queue = self._queues[binding["queue"]]
            exchange = self._exchanges[binding["exchange"]]
            await queue.bind(exchange, routing_key=binding["routing_key"])
            logger.debug(
                "Bound queue to exchange",
                queue=binding["queue"],
                exchange=binding["exchange"],
                routing_key=binding["routing_key"],
            )

    async def close(self) -> None:
        """Close connection."""
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
            logger.info("Disconnected from RabbitMQ")

    async def publish(
        self,
        exchange: str,
        routing_key: str,
        message: dict[str, Any],
        priority: int = 5,
        expiration: int | None = None,
    ) -> None:
        """Publish a message to an exchange."""
        if not self._channel:
            await self.connect()

        body = json.dumps(message, default=str).encode()

        msg = Message(
            body=body,
            content_type="application/json",
            priority=priority,
            expiration=expiration,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

        exchange_obj = self._exchanges.get(exchange)
        if not exchange_obj:
            raise ValueError(f"Exchange '{exchange}' not found")

        await exchange_obj.publish(msg, routing_key=routing_key)

        logger.debug(
            "Published message",
            exchange=exchange,
            routing_key=routing_key,
            priority=priority,
        )

    async def publish_task(self, task: dict[str, Any], mode: str = "http") -> None:
        """Publish a parsing task."""
        routing_key = f"task.{mode}"
        priority = task.get("priority", 5)
        ttl = task.get("ttl_seconds", 3600) * 1000  # Convert to ms

        await self.publish(
            exchange="parser.direct",
            routing_key=routing_key,
            message=task,
            priority=priority,
            expiration=ttl,
        )

    async def publish_result(self, result: dict[str, Any]) -> None:
        """Publish a task result."""
        await self.publish(
            exchange="parser.direct",
            routing_key="result",
            message=result,
        )

    async def consume(
        self,
        queue_name: str,
        callback: Callable[[AbstractIncomingMessage], Coroutine[Any, Any, None]],
        prefetch_count: int = 10,
    ) -> None:
        """Start consuming messages from a queue."""
        if not self._channel:
            await self.connect()

        await self._channel.set_qos(prefetch_count=prefetch_count)

        queue = self._queues.get(queue_name)
        if not queue:
            raise ValueError(f"Queue '{queue_name}' not found")

        await queue.consume(callback)
        logger.info("Started consuming", queue=queue_name, prefetch=prefetch_count)

    async def get_queue_stats(self, queue_name: str) -> dict[str, int]:
        """Get queue statistics."""
        if not self._channel:
            await self.connect()

        queue = self._queues.get(queue_name)
        if not queue:
            return {"message_count": 0, "consumer_count": 0}

        await queue.declare()
        return {
            "message_count": queue.declaration_result.message_count,
            "consumer_count": queue.declaration_result.consumer_count,
        }


# Global client instance
_rmq_client: RabbitMQClient | None = None


async def get_rmq_client() -> RabbitMQClient:
    """Get or create RabbitMQ client instance."""
    global _rmq_client
    if _rmq_client is None:
        _rmq_client = RabbitMQClient()
        await _rmq_client.connect()
    return _rmq_client


async def close_rmq_client() -> None:
    """Close global RabbitMQ client."""
    global _rmq_client
    if _rmq_client:
        try:
            await _rmq_client.close()
        except Exception as e:
            logger.warning("Error closing RabbitMQ client", error=str(e))
        finally:
            _rmq_client = None
