"""Detector fleet service.

Receives batches of normalized telemetry from the partition consumers, runs the
configured detection rules, deduplicates by incident fingerprint, and persists
incidents with NEW/ONGOING/RESOLVED lifecycle transitions. Horizontally scalable:
each instance owns the applications on its assigned Kafka partitions.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel, Field

from config.logging_utils import configure_logging, get_logger
from config.metrics import mark_up, metrics_response
from config.settings import settings
from database.repository import resolve_stale
from database.session import init_db
from detectors.engine import DetectorEngine
from models.schema import utcnow_iso

configure_logging("detector")
logger = get_logger("detectors.main")

engine = DetectorEngine(settings.detector_id)


class EvaluateRequest(BaseModel):
    events: list[dict[str, Any]] = Field(default_factory=list)
    kafka_partition: int | None = None
    consumer_id: str | None = None


class DetectorState:
    def __init__(self) -> None:
        self.started_at = utcnow_iso()
        self.batches = 0
        self.events = 0
        self.detections = 0
        self.incidents_new = 0
        self.incidents_ongoing = 0
        self.resolved_sweeps = 0
        self.last_batch_at: str | None = None
        self.last_error: str | None = None


state = DetectorState()


async def resolve_loop() -> None:
    while True:
        try:
            resolved = await asyncio.to_thread(resolve_stale)
            if resolved:
                state.resolved_sweeps += resolved
                logger.info("incidents resolved by stale sweep", extra={"count": resolved})
        except Exception as exc:  # noqa: BLE001
            state.last_error = str(exc)
            logger.error("resolve sweep failed", extra={"error": str(exc)})
        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(_: FastAPI):
    mark_up("detector")
    init_db()
    task = asyncio.create_task(resolve_loop())
    logger.info("detector started", extra={"detector_id": settings.detector_id})
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="AIOps Detector Fleet", version="1.0.0", lifespan=lifespan)


@app.post("/evaluate")
def evaluate(request: EvaluateRequest) -> dict[str, Any]:
    try:
        result = engine.evaluate(request.events, request.kafka_partition)
        state.batches += 1
        state.events += result["events_evaluated"]
        state.detections += result["detections"]
        state.incidents_new += result["transitions"].get("NEW", 0)
        state.incidents_ongoing += result["transitions"].get("ONGOING", 0)
        state.last_batch_at = utcnow_iso()
        return result
    except Exception as exc:  # noqa: BLE001
        state.last_error = str(exc)
        logger.error("evaluate failed", extra={"error": str(exc)})
        raise


@app.get("/")
def root() -> dict[str, Any]:
    return {"service": "detector", "detector_id": settings.detector_id}


@app.get("/health")
def health() -> dict[str, Any]:
    return {"service": "detector", "status": "healthy", "detector_id": settings.detector_id}


@app.get("/status")
def status() -> dict[str, Any]:
    return {
        "service": "detector",
        "detector_id": settings.detector_id,
        "started_at": state.started_at,
        "batches": state.batches,
        "events_evaluated": state.events,
        "detections": state.detections,
        "incidents_new": state.incidents_new,
        "incidents_ongoing": state.incidents_ongoing,
        "resolved": state.resolved_sweeps,
        "last_batch_at": state.last_batch_at,
        "last_error": state.last_error,
    }


@app.post("/resolve-stale")
def resolve_stale_now() -> dict[str, Any]:
    resolved = resolve_stale()
    return {"resolved": resolved}


@app.get("/metrics")
def metrics():
    return metrics_response()
