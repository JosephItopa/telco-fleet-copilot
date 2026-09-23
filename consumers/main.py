"""Partition consumer service.

A Kafka consumer group member: Kafka assigns partitions dynamically, this
service batches records from its assigned partitions and forwards them to the
detector fleet. Offsets are committed only after the detector acknowledges, with
bounded retries and a dead-letter topic for poison records.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from config.logging_utils import configure_logging, get_logger
from config.metrics import EVENTS_CONSUMED, EVENTS_FAILED, INGEST_LATENCY, mark_up, metrics_response
from config.settings import settings
from kafka.consumer import build_consumer
from kafka.producer import build_producer
from models.schema import utcnow_iso

configure_logging("consumer")
logger = get_logger("consumers.main")

INSTANCE_ID = f"consumer-{settings.hostname}"


class ConsumerState:
    def __init__(self) -> None:
        self.started_at = utcnow_iso()
        self.connected = False
        self.running = True
        self.assigned_partitions: list[int] = []
        self.records_consumed = 0
        self.records_forwarded = 0
        self.records_dlq = 0
        self.detector_failures = 0
        self.last_batch_at: str | None = None
        self.last_error: str | None = None


state = ConsumerState()


async def forward_to_detector(client: httpx.AsyncClient, events: list[dict[str, Any]], partition: int) -> bool:
    """Send a batch to the detector fleet; retry, then give up for DLQ."""
    payload = {"events": events, "kafka_partition": partition, "consumer_id": INSTANCE_ID}
    for attempt in range(1, settings.consumer_max_retries + 1):
        try:
            response = await client.post(f"{settings.detector_url}/evaluate", json=payload, timeout=30.0)
            response.raise_for_status()
            return True
        except Exception as exc:  # noqa: BLE001
            state.last_error = str(exc)
            if attempt < settings.consumer_max_retries:
                await asyncio.sleep(0.5 * attempt)
    state.detector_failures += 1
    return False


async def consume_loop() -> None:
    consumer = build_consumer(INSTANCE_ID)
    dlq = build_producer()
    await dlq.start()
    try:
        await consumer.start()
        state.connected = True
        logger.info("consumer started", extra={"group": settings.kafka_consumer_group, "instance": INSTANCE_ID})
        async with httpx.AsyncClient() as client:
            while state.running:
                batches = await consumer.getmany(timeout_ms=1000, max_records=settings.kafka_max_poll_records)
                if not batches:
                    continue
                for topic_partition, messages in batches.items():
                    if not messages:
                        continue
                    events = [message.value for message in messages]
                    started = time.perf_counter()
                    ok = await forward_to_detector(client, events, topic_partition.partition)
                    INGEST_LATENCY.labels("consumer").observe(time.perf_counter() - started)
                    state.records_consumed += len(events)
                    EVENTS_CONSUMED.labels("consumer", settings.kafka_topic).inc(len(events))
                    if ok:
                        state.records_forwarded += len(events)
                    else:
                        for message in messages:
                            await dlq.send_and_wait(settings.kafka_dlq_topic, value=message.value, key=(message.key or b"poison").decode("utf-8", "ignore"))
                        state.records_dlq += len(messages)
                        EVENTS_FAILED.labels("consumer", "detector_unavailable").inc(len(messages))
                    # Commit after success or DLQ routing so a poison batch cannot block the partition.
                    await consumer.commit()
                    state.last_batch_at = utcnow_iso()
                state.assigned_partitions = sorted(tp.partition for tp in consumer.assignment())
    except asyncio.CancelledError:
        logger.info("consumer loop cancelled")
        raise
    except Exception as exc:  # noqa: BLE001
        state.connected = False
        state.last_error = str(exc)
        logger.error("consumer loop failed", extra={"error": str(exc)})
    finally:
        state.connected = False
        try:
            await consumer.stop()
        except Exception:  # noqa: BLE001
            pass
        try:
            await dlq.stop()
        except Exception:  # noqa: BLE001
            pass


@asynccontextmanager
async def lifespan(_: FastAPI):
    mark_up("consumer")
    task = asyncio.create_task(consume_loop())
    yield
    state.running = False
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="AIOps Partition Consumers", version="1.0.0", lifespan=lifespan)


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "consumer",
        "instance": INSTANCE_ID,
        "group": settings.kafka_consumer_group,
        "topic": settings.kafka_topic,
    }


@app.get("/health")
def health() -> JSONResponse:
    healthy = state.connected
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"service": "consumer", "status": "healthy" if healthy else "degraded", "connected": healthy},
    )


@app.get("/status")
def status() -> dict[str, Any]:
    return {
        "service": "consumer",
        "instance": INSTANCE_ID,
        "group": settings.kafka_consumer_group,
        "started_at": state.started_at,
        "connected": state.connected,
        "assigned_partitions": state.assigned_partitions,
        "records_consumed": state.records_consumed,
        "records_forwarded": state.records_forwarded,
        "records_dlq": state.records_dlq,
        "detector_failures": state.detector_failures,
        "last_batch_at": state.last_batch_at,
        "last_error": state.last_error,
    }


@app.get("/metrics")
def metrics():
    return metrics_response()
