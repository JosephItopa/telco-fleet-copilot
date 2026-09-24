"""Collector service: discovers Kubernetes workloads and streams normalized
telemetry to the partitioned Kafka topic. Horizontally scalable: run N replicas
with distinct COLLECTOR_ID values."""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any, Callable

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from config.logging_utils import configure_logging, get_logger
from config.metrics import INGEST_LATENCY, mark_up, metrics_response
from config.settings import settings
from kafka.producer import build_producer, publish_batch
from kafka.topics import ensure_topics
from models.schema import utcnow_iso

configure_logging("collector")
logger = get_logger("collectors.main")


class CollectorState:
    def __init__(self) -> None:
        self.started_at = utcnow_iso()
        self.kafka_connected = False
        self.topics_ready = False
        self.cycles = 0
        self.events_collected = 0
        self.events_published = 0
        self.failures = 0
        self.last_cycle_at: str | None = None
        self.last_error: str | None = None
        self.apps_discovered = 0
        self.source_mode: str | None = None
        self.fallback_reason: str | None = None


state = CollectorState()
source: Any = None
producer: Any = None


def _default_k8s_factory() -> Any:
    from collectors.k8s_source import KubernetesSource

    return KubernetesSource(settings.k8s_contexts, settings.collector_id, settings.kubeconfig)


def _default_seed_factory() -> Any:
    from seed_k8s.dataset import SeedSource

    return SeedSource(
        apps=settings.seed_apps,
        cluster_id=settings.seed_cluster,
        namespace=settings.seed_namespace,
        unhealthy_ratio=settings.seed_unhealthy_ratio,
    )


def build_source(
    k8s_factory: Callable[[], Any] | None = None,
    seed_factory: Callable[[], Any] | None = None,
) -> tuple[Any, str]:
    """Return (source, mode).

    In ``k8s`` mode the Kubernetes source is probed first; if the cluster is
    unreachable the collector transparently falls back to the seed-k8s dataset so
    the platform always has telemetry.
    """
    k8s_factory = k8s_factory or _default_k8s_factory
    seed_factory = seed_factory or _default_seed_factory

    if settings.collector_mode == "k8s":
        try:
            candidate = k8s_factory()
            verify = getattr(candidate, "verify", None)
            if callable(verify):
                verify()
            state.fallback_reason = None
            return candidate, "k8s"
        except Exception as exc:  # noqa: BLE001 - fall back rather than go dark
            state.fallback_reason = str(exc)
            logger.warning("kubernetes unavailable; using seed-k8s fallback", extra={"error": str(exc)})

    return seed_factory(), "seed"


def use_seed_fallback(reason: str, seed_factory: Callable[[], Any] | None = None) -> None:
    global source
    source = (seed_factory or _default_seed_factory)()
    state.source_mode = "seed"
    state.fallback_reason = reason
    logger.warning("switched to seed-k8s fallback", extra={"reason": reason})


async def connect_kafka() -> None:
    """Connect the producer and ensure topics, retrying without crashing."""
    global producer
    while True:
        try:
            if producer is None:
                producer = build_producer()
                await producer.start()
            if not state.topics_ready:
                await ensure_topics()
                state.topics_ready = True
            state.kafka_connected = True
            state.last_error = None
            logger.info("kafka connected", extra={"bootstrap": settings.kafka_bootstrap_servers})
            return
        except Exception as exc:  # noqa: BLE001 - keep retrying, collector must not die
            state.kafka_connected = False
            state.last_error = str(exc)
            logger.warning("kafka connect failed; retrying", extra={"error": str(exc)})
            await asyncio.sleep(5)


def effective_interval_seconds() -> int:
    """Seed data publishes on its own fixed cadence; a live cluster uses the
    collector interval."""
    return settings.seed_interval_seconds if state.source_mode == "seed" else settings.collect_interval_seconds


async def collect_loop() -> None:
    global source
    source, state.source_mode = build_source()
    logger.info(
        "collector source selected",
        extra={
            "mode": state.source_mode,
            "interval_seconds": effective_interval_seconds(),
            "fallback_reason": state.fallback_reason,
        },
    )
    while True:
        cycle_start = time.perf_counter()
        try:
            if not state.kafka_connected:
                await connect_kafka()
            events = source.collect(settings.collector_id)
            state.events_collected += len(events)
        except Exception as exc:  # noqa: BLE001 - collection failure
            state.failures += 1
            state.last_error = str(exc)
            logger.error("collection failed", extra={"mode": state.source_mode, "error": str(exc)})
            if state.source_mode == "k8s":
                use_seed_fallback(str(exc))
            await asyncio.sleep(effective_interval_seconds())
            continue

        try:
            published = await publish_batch(producer, settings.kafka_topic, events, "collector")
            state.events_published += published
            state.cycles += 1
            state.last_cycle_at = utcnow_iso()
            INGEST_LATENCY.labels("collector").observe(time.perf_counter() - cycle_start)
            logger.info(
                "collection cycle",
                extra={"cycle": state.cycles, "mode": state.source_mode, "events": len(events), "published": published},
            )
        except Exception as exc:  # noqa: BLE001 - kafka failure, reconnect next cycle
            state.failures += 1
            state.kafka_connected = False
            state.last_error = str(exc)
            logger.error("publish failed", extra={"error": str(exc)})
        await asyncio.sleep(effective_interval_seconds())


@asynccontextmanager
async def lifespan(_: FastAPI):
    mark_up("collector")
    task = asyncio.create_task(collect_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    if producer is not None:
        try:
            await producer.stop()
        except Exception:  # noqa: BLE001
            pass


app = FastAPI(title="AIOps Collector", version="1.0.0", lifespan=lifespan)


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "collector",
        "collector_id": settings.collector_id,
        "configured_mode": settings.collector_mode,
        "active_source": state.source_mode,
        "fallback_reason": state.fallback_reason,
        "topic": settings.kafka_topic,
    }


@app.get("/health")
def health() -> JSONResponse:
    healthy = state.kafka_connected
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={
            "service": "collector",
            "status": "healthy" if healthy else "degraded",
            "kafka_connected": state.kafka_connected,
        },
    )


@app.get("/status")
def status() -> dict[str, Any]:
    return {
        "service": "collector",
        "collector_id": settings.collector_id,
        "configured_mode": settings.collector_mode,
        "active_source": state.source_mode,
        "fallback_reason": state.fallback_reason,
        "started_at": state.started_at,
        "kafka_connected": state.kafka_connected,
        "topics_ready": state.topics_ready,
        "cycles": state.cycles,
        "events_collected": state.events_collected,
        "events_published": state.events_published,
        "failures": state.failures,
        "last_cycle_at": state.last_cycle_at,
        "last_error": state.last_error,
        "interval_seconds": effective_interval_seconds(),
        "collect_interval_seconds": settings.collect_interval_seconds,
        "seed_interval_seconds": settings.seed_interval_seconds,
    }


@app.get("/discovery")
def discovery() -> dict[str, Any]:
    items = source.discovery_snapshot() if source is not None else []
    state.apps_discovered = len(items)
    return {"count": len(items), "items": items[:1000]}


@app.get("/metrics")
def metrics():
    return metrics_response()
