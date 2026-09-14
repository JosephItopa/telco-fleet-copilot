"""Detector microservice.

Owns telemetry collection and anomaly detection. It polls Prometheus every
``POLL_INTERVAL_SECONDS`` (default 180s = 3 minutes), falls back to bundled
sample records when Prometheus is unreachable, and publishes findings to the
async reasoning worker.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from common.models import Finding, utcnow

from . import collector, config, publisher, rules

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("detector")


class DetectorState:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.last_run_at: str | None = None
        self.last_source: str | None = None
        self.last_error: str | None = None
        self.last_series_count: int = 0
        self.runs: int = 0
        self.analysis_seconds: float = 0.0
        self.findings: list[Finding] = []
        self.published: list[dict[str, Any]] = []
        self.cooldown: dict[tuple[str, str | None, str], float] = {}
        self.last_snapshot: dict[str, Any] = {"apps": [], "clusters": []}


state = DetectorState()


def _build_finding(snapshot: dict[str, Any], detected: dict[str, Any], source: str) -> Finding:
    evidence = {
        "target": snapshot.get("target"),
        "kind": snapshot.get("kind"),
        "cluster": snapshot.get("cluster"),
        "namespace": snapshot.get("namespace"),
        "pod": snapshot.get("pod"),
        "instance": snapshot.get("instance"),
        "metrics": snapshot.get("metrics", {}),
    }
    return Finding(
        incident_id=f"INC-{uuid.uuid4().hex[:8].upper()}",
        created_at=utcnow(),
        target=snapshot["target"],
        kind=snapshot["kind"],
        cluster=snapshot.get("cluster"),
        namespace=snapshot.get("namespace"),
        instance=snapshot.get("instance"),
        anomaly=detected["anomaly"],
        severity=detected["severity"],
        confidence=detected["confidence"],
        reason=detected["reason"],
        evidence=evidence,
        source=source,
        status="DETECTED",
    )


async def run_analysis() -> dict[str, Any]:
    started = time.perf_counter()
    collected = await collector.collect()
    snapshots = collected.apps + collected.clusters

    new_findings: list[Finding] = []
    published: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=10.0) as client:
        for snapshot in snapshots:
            for detected in rules.detect(snapshot):
                key = (snapshot["target"], snapshot.get("cluster"), detected["anomaly"])
                last_seen = state.cooldown.get(key)
                now = time.monotonic()
                if last_seen is not None and (now - last_seen) < config.INCIDENT_COOLDOWN_SECONDS:
                    continue
                state.cooldown[key] = now

                finding = _build_finding(snapshot, detected, collected.source)
                new_findings.append(finding)
                result = await publisher.publish(finding, client)
                published.append({"incident_id": finding.incident_id, **result})

    async with state.lock:
        state.findings = (new_findings + state.findings)[:500]
        state.published = (published + state.published)[:200]
        state.last_run_at = utcnow()
        state.last_source = collected.source
        state.last_error = collected.error
        state.last_series_count = collected.series_count
        state.runs += 1
        state.analysis_seconds = round(time.perf_counter() - started, 3)
        state.last_snapshot = {"apps": collected.apps, "clusters": collected.clusters}

    logger.info(
        "analysis #%s source=%s series=%s findings=%s published=%s",
        state.runs,
        collected.source,
        collected.series_count,
        len(new_findings),
        sum(1 for item in published if item.get("ok")),
    )
    return {
        "analyzed_at": state.last_run_at,
        "source": collected.source,
        "fallback_reason": collected.error,
        "series_count": collected.series_count,
        "findings": len(new_findings),
        "new_incidents": [f.model_dump(mode="json") for f in new_findings],
        "published": published,
    }


async def _scheduler() -> None:
    await asyncio.sleep(2)  # let the app finish starting
    while True:
        try:
            await run_analysis()
        except Exception:  # noqa: BLE001 - scheduler must never die
            logger.exception("scheduled analysis failed")
        await asyncio.sleep(config.POLL_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(_scheduler())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="AIOps Detector", version="0.1.0", lifespan=lifespan)


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "aiops-detector",
        "status": "running",
        "poll_interval_seconds": config.POLL_INTERVAL_SECONDS,
        "prometheus_url": config.PROMETHEUS_URL,
        "worker_url": config.WORKER_URL,
    }


@app.get("/health")
def health() -> dict[str, Any]:
    return {"service": "aiops-detector", "status": "healthy"}


@app.get("/status")
def status() -> dict[str, Any]:
    return {
        "service": "aiops-detector",
        "runs": state.runs,
        "last_run_at": state.last_run_at,
        "last_source": state.last_source,
        "last_error": state.last_error,
        "last_series_count": state.last_series_count,
        "analysis_seconds": state.analysis_seconds,
        "poll_interval_seconds": config.POLL_INTERVAL_SECONDS,
        "fallback_enabled": config.FALLBACK_ENABLED,
        "prometheus_url": config.PROMETHEUS_URL,
        "worker_url": config.WORKER_URL,
        "findings_buffered": len(state.findings),
    }


@app.post("/analyze")
async def analyze() -> dict[str, Any]:
    return await run_analysis()


@app.get("/findings")
def list_findings() -> dict[str, Any]:
    return {
        "count": len(state.findings),
        "items": [f.model_dump(mode="json") for f in state.findings],
    }


@app.get("/snapshot")
def snapshot() -> dict[str, Any]:
    return {
        "source": state.last_source,
        "collected_at": state.last_run_at,
        "error": state.last_error,
        **state.last_snapshot,
    }


@app.get("/cooldowns")
def cooldowns() -> dict[str, Any]:
    return {
        "cooldown_seconds": config.INCIDENT_COOLDOWN_SECONDS,
        "entries": [
            {"target": key[0], "cluster": key[1], "anomaly": key[2], "age_seconds": round(time.monotonic() - seen, 1)}
            for key, seen in state.cooldown.items()
        ],
    }
