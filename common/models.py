from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field

Severity = Literal["critical", "high", "medium", "low", "info"]
TargetKind = Literal["app", "cluster"]
DataSource = Literal["prometheus", "fallback"]
FindingStatus = Literal["DETECTED", "REASONING", "RECOMMENDED", "FAILED", "RESOLVED"]
AIStatus = Literal["pending", "completed", "failed", "skipped"]


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class AIAnalysis(BaseModel):
    """LLM output attached to a finding by the reasoning worker."""

    status: AIStatus = "pending"
    provider: str = "nvidia"
    model: str = ""
    summary: str = ""
    root_cause: str = ""
    remediation_steps: list[str] = Field(default_factory=list)
    risk: str = ""
    confidence: float = 0.0
    latency_seconds: float = 0.0
    generated_at: str | None = None
    error: str | None = None


class Finding(BaseModel):
    """A detected anomaly. Produced by the detector, enriched by the worker."""

    incident_id: str
    created_at: str = Field(default_factory=utcnow)
    target: str
    kind: TargetKind
    cluster: str | None = None
    namespace: str | None = None
    instance: str | None = None
    anomaly: str
    severity: Severity
    confidence: float = 0.5
    reason: str = ""
    evidence: dict[str, Any] = Field(default_factory=dict)
    source: DataSource = "fallback"
    status: FindingStatus = "DETECTED"
    resolved_at: str | None = None
    resolved_note: str | None = None
    detected_by: str = "detector/threshold-rules-v1"
    ai: AIAnalysis = Field(default_factory=AIAnalysis)
