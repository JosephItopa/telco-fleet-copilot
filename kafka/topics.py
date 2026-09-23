"""Topic provisioning. The producer is not a partition: it publishes to a
partitioned topic, and the partition is chosen from the record key (app_id)."""

from __future__ import annotations

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from config.logging_utils import get_logger
from config.settings import settings

logger = get_logger("kafka.topics")


async def ensure_topics() -> dict[str, bool]:
    """Create the telemetry topic (partitioned) and the dead-letter topic."""
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    created: dict[str, bool] = {}
    await admin.start()
    try:
        topics = [
            NewTopic(
                name=settings.kafka_topic,
                num_partitions=settings.kafka_partitions,
                replication_factor=settings.kafka_replication_factor,
            ),
            NewTopic(
                name=settings.kafka_dlq_topic,
                num_partitions=max(1, settings.kafka_partitions // 2),
                replication_factor=settings.kafka_replication_factor,
            ),
        ]
        for topic in topics:
            try:
                await admin.create_topics([topic])
                created[topic.name] = True
                logger.info("topic created", extra={"topic": topic.name, "partitions": topic.num_partitions})
            except TopicAlreadyExistsError:
                created[topic.name] = False
                logger.info("topic exists", extra={"topic": topic.name})
    finally:
        await admin.close()
    return created
