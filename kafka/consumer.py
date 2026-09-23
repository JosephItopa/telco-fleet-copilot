"""Kafka consumer-group helpers: rebalancing, manual offsets, graceful stop."""

from __future__ import annotations

import json

from aiokafka import AIOKafkaConsumer
from aiokafka.abc import ConsumerRebalanceListener

from config.logging_utils import get_logger
from config.settings import settings

logger = get_logger("kafka.consumer")


class LoggingRebalanceListener(ConsumerRebalanceListener):
    """Observes partition assignment so scaling instances is visible in logs."""

    def __init__(self, instance_id: str) -> None:
        self.instance_id = instance_id

    async def on_partitions_revoked(self, revoked):
        logger.info(
            "partitions revoked",
            extra={"instance": self.instance_id, "partitions": [tp.partition for tp in revoked]},
        )

    async def on_partitions_assigned(self, assigned):
        logger.info(
            "partitions assigned",
            extra={"instance": self.instance_id, "partitions": [tp.partition for tp in assigned]},
        )


def build_rebalance_listener(instance_id: str) -> LoggingRebalanceListener:
    return LoggingRebalanceListener(instance_id)


def build_consumer(instance_id: str) -> AIOKafkaConsumer:
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=settings.kafka_max_poll_records,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
    )
    consumer.subscribe([settings.kafka_topic], listener=build_rebalance_listener(instance_id))
    return consumer
