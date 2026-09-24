"""API service.

Serves processed incidents, application/cluster health and platform status to the
dashboard. Deliberately NOT in the raw telemetry path: Kafka and the detector
fleet handle streaming, this service only reads/writes processed data.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query

from config.logging_utils import configure_logging, get_logger
from config.metrics import mark_up, metrics_response
from config.settings import settings
from database.repository import (
    application_health,
    attach_ai,
    cluster_health,
    counts_by,
    detector_activity,
    get_incident,
    list_incidents,
    summary,
)
from database.session import init_db

configure_logging("api")
logger = get_logger("api.main")

app = FastAPI(title="AIOps API", version="1.0.0")


@app.on_event("startup")
def startup() -> None:
    mark_up("api")
    init_db()
    logger.info("api started")


def _since(value: str | None) -> datetime | None:
    if not value:
        return None
    if value.isdigit():
        return datetime.now(timezone.utc) - timedelta(seconds=int(value))
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


async def _fetch_status(url: str) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{url.rstrip('/')}/status")
            response.raise_for_status()
            payload = response.json()
            return {"reachable": True, **payload}
    except Exception as exc:  # noqa: BLE001
        return {"reachable": False, "error": str(exc)}


@app.get("/")
def root() -> dict[str, Any]:
    return {"service": "api", "status": "running"}


@app.get("/health")
def health() -> dict[str, Any]:
    return {"service": "api", "status": "healthy"}


@app.get("/metrics")
def metrics():
    return metrics_response()


@app.get("/summary")
def get_summary() -> dict[str, Any]:
    data = summary()
    data["incidents_by_severity"] = counts_by("severity")
    data["incidents_by_status"] = counts_by("status")
    data["incidents_by_anomaly"] = counts_by("anomaly_type")
    return data


@app.get("/incidents")
def get_incidents(
    status: str | None = Query(default=None, description="NEW,ONGOING,RESOLVED"),
    severity: str | None = Query(default=None),
    app_id: str | None = None,
    cluster_id: str | None = None,
    anomaly_type: str | None = None,
    since: str | None = Query(default=None, description="ISO timestamp or seconds ago"),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    items = list_incidents(
        status=status,
        severity=severity,
        app_id=app_id,
        cluster_id=cluster_id,
        anomaly_type=anomaly_type,
        since=_since(since),
        limit=limit,
        offset=offset,
    )
    return {"count": len(items), "items": items}


@app.get("/anomalies/current")
def current_anomalies(severity: str | None = None, limit: int = Query(default=200, ge=1, le=1000)) -> dict[str, Any]:
    items = list_incidents(status="NEW,ONGOING", severity=severity, limit=limit)
    return {"count": len(items), "items": items}


@app.get("/incidents/history")
def incident_history(
    app_id: str | None = None,
    cluster_id: str | None = None,
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    items = list_incidents(status="RESOLVED", app_id=app_id, cluster_id=cluster_id, limit=limit, offset=offset)
    return {"count": len(items), "items": items}


@app.get("/incidents/{incident_id}")
def incident_detail(incident_id: str) -> dict[str, Any]:
    incident = get_incident(incident_id)
    if incident is None:
        raise HTTPException(status_code=404, detail="incident not found")
    return incident


@app.get("/incidents/{incident_id}/timeline")
def incident_timeline(incident_id: str, limit: int = Query(default=200, ge=1, le=1000)) -> dict[str, Any]:
    incident = get_incident(incident_id)
    if incident is None:
        raise HTTPException(status_code=404, detail="incident not found")
    related = list_incidents(app_id=incident["app_id"], limit=limit)
    return {"incident_id": incident_id, "app_id": incident["app_id"], "items": related}


@app.get("/applications/health")
def applications(limit: int = Query(default=1000, ge=1, le=5000)) -> dict[str, Any]:
    items = application_health(limit=limit)
    healthy = sum(1 for item in items if item["status"] == "healthy")
    return {"count": len(items), "healthy": healthy, "anomalous": len(items) - healthy, "items": items}


@app.get("/clusters/health")
def clusters() -> dict[str, Any]:
    items = cluster_health()
    return {"count": len(items), "items": items}


@app.get("/detectors/status")
async def detectors_status() -> dict[str, Any]:
    remote = await _fetch_status(settings.detector_url)
    return {"detectors": detector_activity(), "detector_service": remote}


@app.get("/kafka/health")
async def kafka_health() -> dict[str, Any]:
    consumer = await _fetch_status(settings.consumer_url)
    return {
        "topic": settings.kafka_topic,
        "dlq_topic": settings.kafka_dlq_topic,
        "partitions": settings.kafka_partitions,
        "group": settings.kafka_consumer_group,
        "consumer": consumer,
    }


@app.get("/collectors/health")
async def collectors_health() -> dict[str, Any]:
    collector = await _fetch_status(settings.collector_url)
    return {"collector": collector, "mode": settings.collector_mode}


@app.post("/incidents/{incident_id}/analyze")
async def analyze_incident(incident_id: str, force: bool = False) -> dict[str, Any]:
    """Ask the AI inference service to analyse an incident, then persist it."""
    incident = get_incident(incident_id)
    if incident is None:
        raise HTTPException(status_code=404, detail="incident not found")
    if incident.get("ai") and not force:
        return {"incident_id": incident_id, "ai": incident["ai"], "cached": True}

    history = list_incidents(app_id=incident["app_id"], limit=20)
    payload = {"incident": incident, "history": history}
    try:
        async with httpx.AsyncClient(timeout=settings.inference_timeout_seconds) as client:
            response = await client.post(f"{settings.inference_url.rstrip('/')}/analyze", json=payload)
            response.raise_for_status()
            analysis = response.json()
    except Exception as exc:  # noqa: BLE001
        logger.error("ai inference failed", extra={"incident_id": incident_id, "error": str(exc)})
        raise HTTPException(status_code=502, detail=f"inference unavailable: {exc}") from exc

    remediation = "; ".join(analysis.get("remediation", [])[:3]) or None
    updated = attach_ai(incident_id, analysis, remediation)
    return {"incident_id": incident_id, "ai": updated.get("ai") if updated else analysis, "cached": False}
