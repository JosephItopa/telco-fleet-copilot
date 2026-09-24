"""Reliable, idempotent, batched, compressed Kafka producer."""

from __future__ import annotations

import json
from typing import Any

from aiokafka import AIOKafkaProducer

from config.logging_utils import get_logger
from config.metrics import EVENTS_PRODUCED
from config.settings import settings

logger = get_logger("kafka.producer")


def build_producer() -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
        # Idempotent delivery. aiokafka enforces max in-flight requests itself,
        # there is no max_in_flight_requests_per_connection parameter.
        enable_idempotence=True,
        retry_backoff_ms=500,
        linger_ms=settings.kafka_linger_ms,
        max_batch_size=settings.kafka_max_batch_size,
        compression_type=settings.kafka_compression,
        request_timeout_ms=30_000,
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
    )


async def publish_batch(
    producer: AIOKafkaProducer,
    topic: str,
    records: list[dict[str, Any]],
    service: str = "collector",
) -> int:
    """Publish records keyed deterministically by app_id.

    Using app_id as the key routes every metric for an application to the same
    partition, which keeps per-application ordering and lets consumers aggregate
    without cross-partition coordination.
    """
    published = 0
    for record in records:
        key = record.get("app_id") or record.get("cluster_id") or "unknown"
        await producer.send_and_wait(topic, value=record, key=key)
        published += 1
    if published:
        EVENTS_PRODUCED.labels(service, topic).inc(published)
    return published
