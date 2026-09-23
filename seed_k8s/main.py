"""Standalone seed publisher.

Optional: publishes the seed Kubernetes telemetry straight to Kafka, using the
same normalized schema and producer settings as the collector. The collector's
``COLLECTOR_MODE=seed`` already emits this data, so run this only if you want a
separate publisher process.

    python -m seed_k8s.main
"""

from __future__ import annotations

import asyncio
from contextlib import suppress

from config.logging_utils import configure_logging, get_logger
from config.settings import settings
from kafka.producer import build_producer, publish_batch
from kafka.topics import ensure_topics
from seed_k8s.dataset import SeedSource

logger = get_logger("seed_k8s.main")


async def run() -> None:
    source = SeedSource(
        apps=settings.seed_apps,
        cluster_id=settings.seed_cluster,
        namespace=settings.seed_namespace,
        unhealthy_ratio=settings.seed_unhealthy_ratio,
    )
    producer = build_producer()
    await producer.start()
    await ensure_topics()
    logger.info("seed publisher started", extra={"apps": len(source.apps), "cluster": settings.seed_cluster})
    try:
        while True:
            events = source.collect(settings.seed_collector_id)
            published = await publish_batch(producer, settings.kafka_topic, events, "seed-k8s")
            logger.info("seed cycle", extra={"events": len(events), "published": published})
            await asyncio.sleep(settings.seed_interval_seconds)
    finally:
        with suppress(Exception):
            await producer.stop()


def main() -> None:
    configure_logging("seed-k8s")
    asyncio.run(run())


if __name__ == "__main__":
    main()
