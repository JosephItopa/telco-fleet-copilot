"""Shared domain schema for the AIOps platform.

These models are the frozen contract between every service:
collectors -> Kafka -> consumers -> detectors -> database -> api -> dashboard.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class MetricType(str, Enum):
    POD_CPU = "pod_cpu"
    POD_MEMORY = "pod_memory"
    NODE_CPU = "node_cpu"
    NODE_MEMORY = "node_memory"
    POD_STATUS = "pod_status"
    CONTAINER_RESTARTS = "container_restarts"
    DEPLOYMENT_REPLICAS = "deployment_replicas"
    RESOURCE_LIMITS = "resource_limits"
    NODE_CONDITION = "node_condition"
    K8S_EVENT = "k8s_event"


class AnomalyType(str, Enum):
    CPU_SATURATION = "cpu_saturation"
    MEMORY_SATURATION = "memory_saturation"
    CONTAINER_RESTART_STORM = "container_restart_storm"
    DEPLOYMENT_DEGRADATION = "deployment_degradation"
    UNAVAILABLE_REPLICAS = "unavailable_replicas"
    NODE_RESOURCE_EXHAUSTION = "node_resource_exhaustion"
    ABNORMAL_RESOURCE_GROWTH = "abnormal_resource_growth"
    UNHEALTHY_POD_STATE = "unhealthy_pod_state"


class Severity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class IncidentStatus(str, Enum):
    NEW = "NEW"
    ONGOING = "ONGOING"
    RESOLVED = "RESOLVED"


class AIStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


SEVERITY_RANK = {
    Severity.CRITICAL.value: 0,
    Severity.HIGH.value: 1,
    Severity.MEDIUM.value: 2,
    Severity.LOW.value: 3,
    Severity.INFO.value: 4,
}


class MetricEvent(BaseModel):
    """Normalized telemetry event published to Kafka, keyed by app_id."""

    cluster_id: str
    collector_id: str
    app_id: str
    namespace: str | None = None
    pod: str | None = None
    timestamp: str = Field(default_factory=utcnow_iso)
    metric_type: MetricType
    metrics: dict[str, Any] = Field(default_factory=dict)
    labels: dict[str, Any] = Field(default_factory=dict)


class AIAnalysis(BaseModel):
    """Grounded AI output. The model may only use supplied evidence."""

    status: AIStatus = AIStatus.PENDING
    provider: str = "nvidia"
    model: str = ""
    explanation: str = ""
    evidence: list[str] = Field(default_factory=list)
    root_causes: list[str] = Field(default_factory=list)
    remediation: list[str] = Field(default_factory=list)
    confidence: float = 0.0
    next_action: str = ""
    latency_seconds: float = 0.0
    generated_at: str | None = None
    error: str | None = None
    attempts: list[dict[str, Any]] = Field(default_factory=list)


class Incident(BaseModel):
    incident_id: str
    fingerprint: str
    app_id: str
    cluster_id: str | None = None
    namespace: str | None = None
    detector_id: str | None = None
    kafka_partition: int | None = None
    anomaly_type: str
    metric: str | None = None
    severity: Severity = Severity.MEDIUM
    status: IncidentStatus = IncidentStatus.NEW
    evidence: list[dict[str, Any]] = Field(default_factory=list)
    observation_count: int = 1
    first_detected: str = Field(default_factory=utcnow_iso)
    last_detected: str = Field(default_factory=utcnow_iso)
    resolved_at: str | None = None
    ai: AIAnalysis | None = None
    remediation: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class HealthStatus(BaseModel):
    service: str
    status: str = "healthy"
    detail: dict[str, Any] = Field(default_factory=dict)
