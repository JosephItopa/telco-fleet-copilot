"""Async reasoning worker.

Receives findings from the detector, enqueues them for LLM reasoning in a
background consumer (no Kafka for this prototype), and serves the enriched
findings that the Streamlit dashboard reads.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from common.models import AIAnalysis, Finding, utcnow

from . import catalog, config, reasoner
from .observations import ObservationStore, build_reports, observations_from_snapshot
from .store import FindingStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("reasoning-worker")

SEVERITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}

store = FindingStore()
queue: asyncio.Queue[str] = asyncio.Queue()
consumer_task: asyncio.Task[None] | None = None


def _rank(severity: str) -> int:
    return SEVERITY_ORDER.get(severity, 9)


def _eligible(severity: str) -> bool:
    return _rank(severity) <= _rank(config.REASON_MIN_SEVERITY)


async def _consume() -> None:
    while True:
        incident_id = await queue.get()
        try:
            finding = await store.get(incident_id)
            if finding is None:
                continue
            finding.status = "REASONING"
            await store.upsert(finding)

            analysis = await reasoner.analyze(finding)
            finding.ai = analysis
            if finding.status == "RESOLVED":
                logger.info("reasoned incident=%s but it is resolved; keeping status", incident_id)
            else:
                finding.status = "RECOMMENDED" if analysis.status == "completed" else "FAILED"
            await store.upsert(finding)
            logger.info(
                "reasoned incident=%s status=%s provider=%s",
                incident_id,
                finding.status,
                analysis.provider,
            )
        except Exception:  # noqa: BLE001 - one bad finding must not kill the consumer
            logger.exception("reasoning failed for incident=%s", incident_id)
            finding = await store.get(incident_id)
            if finding is not None:
                finding.status = "FAILED"
                finding.ai.status = "failed"
                await store.upsert(finding)
        finally:
            queue.task_done()


@asynccontextmanager
async def lifespan(_: FastAPI):
    global consumer_task
    consumer_task = asyncio.create_task(_consume())
    yield
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="AIOps Reasoning Worker", version="0.1.0", lifespan=lifespan)


def _llm_status() -> dict[str, Any]:
    configured = bool(config.NVIDIA_API_KEY) and not config.NVIDIA_API_KEY.startswith("your_")
    return {
        "provider": "nvidia",
        "configured": configured,
        "model": config.NVIDIA_MODEL,
        "base_url": config.NVIDIA_BASE_URL,
        "reason_min_severity": config.REASON_MIN_SEVERITY,
    }


@app.get("/")
def root() -> dict[str, Any]:
    return {"service": "aiops-reasoning-worker", "status": "running", "llm": _llm_status()}


@app.get("/health")
def health() -> dict[str, Any]:
    return {"service": "aiops-reasoning-worker", "status": "healthy"}


@app.get("/status")
async def status() -> dict[str, Any]:
    return {
        "service": "aiops-reasoning-worker",
        "queue_size": queue.qsize(),
        "findings": await store.count(),
        "llm": _llm_status(),
    }


@app.get("/summary")
async def summary() -> dict[str, Any]:
    items = await store.list(limit=1000)
    by_severity: dict[str, int] = {}
    by_kind: dict[str, int] = {}
    by_status: dict[str, int] = {}
    by_source: dict[str, int] = {}
    by_anomaly: dict[str, int] = {}
    for item in items:
        by_severity[item.severity] = by_severity.get(item.severity, 0) + 1
        by_kind[item.kind] = by_kind.get(item.kind, 0) + 1
        by_status[item.status] = by_status.get(item.status, 0) + 1
        by_source[item.source] = by_source.get(item.source, 0) + 1
        by_anomaly[item.anomaly] = by_anomaly.get(item.anomaly, 0) + 1
    return {
        "total": len(items),
        "queue_size": queue.qsize(),
        "by_severity": by_severity,
        "by_kind": by_kind,
        "by_status": by_status,
        "by_source": by_source,
        "by_anomaly": by_anomaly,
        "llm": _llm_status(),
    }


@app.post("/findings", status_code=202)
async def ingest(finding: Finding) -> dict[str, Any]:
    existing = await store.get(finding.incident_id)
    if existing is not None:
        return {"accepted": False, "reason": "duplicate", "incident_id": finding.incident_id}

    signature_match = await store.find_active_signature(finding, config.DEDUP_WINDOW_SECONDS)
    if signature_match is not None:
        return {
            "accepted": False,
            "reason": "duplicate_signature",
            "incident_id": signature_match.incident_id,
        }

    if _eligible(finding.severity):
        finding.ai = AIAnalysis(status="pending", provider="nvidia", model=config.NVIDIA_MODEL)
        finding.status = "REASONING"
        await store.upsert(finding)
        await queue.put(finding.incident_id)
        return {"accepted": True, "queued_for_reasoning": True, "incident_id": finding.incident_id}

    if config.ENRICH_MEDIUM_WITH_RULES:
        analysis = catalog.deterministic(finding)
        analysis.status = "skipped"
        analysis.provider = "rules"
        analysis.model = "rules-v1"
        analysis.error = "Below reasoning severity threshold; deterministic recommendation attached."
    else:
        analysis = AIAnalysis(
            status="skipped",
            provider="rules",
            model="rules-v1",
            summary="Below reasoning severity threshold; no LLM analysis requested.",
        )
    finding.ai = analysis
    finding.status = "RECOMMENDED"
    await store.upsert(finding)
    return {"accepted": True, "queued_for_reasoning": False, "incident_id": finding.incident_id}


@app.get("/findings")
async def list_findings(
    severity: str | None = Query(default=None, description="Comma-separated severities"),
    kind: str | None = Query(default=None, description="app or cluster"),
    status_filter: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict[str, Any]:
    items = await store.list(severity=severity, kind=kind, status=status_filter, limit=limit)
    return {"count": len(items), "items": [item.model_dump(mode="json") for item in items]}


@app.get("/findings/{incident_id}")
async def get_finding(incident_id: str) -> dict[str, Any]:
    finding = await store.get(incident_id)
    if finding is None:
        raise HTTPException(status_code=404, detail="finding not found")
    return finding.model_dump(mode="json")


@app.post("/findings/{incident_id}/reanalyze", status_code=202)
async def reanalyze(incident_id: str) -> dict[str, Any]:
    finding = await store.get(incident_id)
    if finding is None:
        raise HTTPException(status_code=404, detail="finding not found")
    if finding.status == "RESOLVED":
        raise HTTPException(status_code=409, detail="finding is flagged as fixed; reopen it first")
    finding.ai = AIAnalysis(status="pending", provider="nvidia", model=config.NVIDIA_MODEL)
    finding.status = "REASONING"
    await store.upsert(finding)
    await queue.put(incident_id)
    return {"accepted": True, "queued_for_reasoning": True, "incident_id": incident_id}


@app.post("/findings/{incident_id}/resolve")
async def resolve(incident_id: str, note: str | None = Query(default=None)) -> dict[str, Any]:
    """Flag a finding as fixed."""
    finding = await store.get(incident_id)
    if finding is None:
        raise HTTPException(status_code=404, detail="finding not found")
    finding.status = "RESOLVED"
    finding.resolved_at = utcnow()
    finding.resolved_note = note
    await store.upsert(finding)
    logger.info("finding flagged as fixed incident=%s", incident_id)
    return finding.model_dump(mode="json")


@app.post("/findings/{incident_id}/reopen")
async def reopen(incident_id: str) -> dict[str, Any]:
    """Undo the fixed flag so the finding returns to the active list."""
    finding = await store.get(incident_id)
    if finding is None:
        raise HTTPException(status_code=404, detail="finding not found")
    finding.status = "RECOMMENDED" if finding.ai.status in {"completed", "failed", "skipped"} else "DETECTED"
    finding.resolved_at = None
    finding.resolved_note = None
    await store.upsert(finding)
    logger.info("finding reopened incident=%s status=%s", incident_id, finding.status)
    return finding.model_dump(mode="json")
