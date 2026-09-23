"""Incident and application persistence with idempotent, lifecycle-aware writes."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from config.settings import settings
from database.models import ApplicationRow, IncidentRow
from database.session import session_scope
from models.schema import SEVERITY_RANK, AnomalyType, IncidentStatus


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _new_incident_id() -> str:
    return f"INC-{uuid.uuid4().hex[:10].upper()}"


def row_to_dict(row: IncidentRow) -> dict[str, Any]:
    return {
        "incident_id": row.incident_id,
        "fingerprint": row.fingerprint,
        "app_id": row.app_id,
        "cluster_id": row.cluster_id,
        "namespace": row.namespace,
        "detector_id": row.detector_id,
        "kafka_partition": row.kafka_partition,
        "anomaly_type": row.anomaly_type,
        "metric": row.metric,
        "severity": row.severity,
        "status": row.status,
        "evidence": row.evidence or [],
        "observation_count": row.observation_count,
        "first_detected": _iso(row.first_detected),
        "last_detected": _iso(row.last_detected),
        "resolved_at": _iso(row.resolved_at),
        "ai": row.ai_analysis,
        "remediation": row.remediation,
        "created_at": _iso(row.created_at),
        "updated_at": _iso(row.updated_at),
    }


def _apply_observation(row: IncidentRow, item: dict[str, Any], now: datetime) -> str:
    """Mutate an existing active incident; return the lifecycle transition."""
    row.status = IncidentStatus.ONGOING.value
    row.last_detected = now
    row.updated_at = now
    row.observation_count = (row.observation_count or 0) + 1
    if SEVERITY_RANK.get(item["severity"], 9) < SEVERITY_RANK.get(row.severity, 9):
        row.severity = item["severity"]
    if item.get("detector_id"):
        row.detector_id = item["detector_id"]
    if item.get("kafka_partition") is not None:
        row.kafka_partition = item["kafka_partition"]
    evidence = list(row.evidence or [])
    evidence.extend(item.get("evidence", []))
    row.evidence = evidence[-settings.evidence_max_items :]
    return IncidentStatus.ONGOING.value


def record_observations(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Idempotently upsert a batch of anomaly observations.

    A repeated observation of the same active fingerprint updates the existing
    incident (NEW -> ONGOING) instead of creating a duplicate. Incident IDs are
    stable across detector restarts and Kafka redelivery because the active row
    is looked up by fingerprint.
    """
    results: list[dict[str, Any]] = []
    if not items:
        return results

    with session_scope() as session:
        for item in items:
            now = _now()
            row = session.execute(
                select(IncidentRow).where(
                    IncidentRow.fingerprint == item["fingerprint"],
                    IncidentRow.status != IncidentStatus.RESOLVED.value,
                )
            ).scalars().first()

            if row is None:
                row = IncidentRow(
                    incident_id=_new_incident_id(),
                    fingerprint=item["fingerprint"],
                    app_id=item["app_id"],
                    cluster_id=item.get("cluster_id"),
                    namespace=item.get("namespace"),
                    detector_id=item.get("detector_id"),
                    kafka_partition=item.get("kafka_partition"),
                    anomaly_type=item["anomaly_type"],
                    metric=item.get("metric"),
                    severity=item["severity"],
                    status=IncidentStatus.NEW.value,
                    evidence=list(item.get("evidence", []))[-settings.evidence_max_items :],
                    observation_count=1,
                    first_detected=now,
                    last_detected=now,
                    created_at=now,
                    updated_at=now,
                )
                session.add(row)
                transition = IncidentStatus.NEW.value
            else:
                transition = _apply_observation(row, item, now)

            try:
                session.flush()
            except IntegrityError:
                session.rollback()
                row = session.execute(
                    select(IncidentRow).where(
                        IncidentRow.fingerprint == item["fingerprint"],
                        IncidentRow.status != IncidentStatus.RESOLVED.value,
                    )
                ).scalars().first()
                if row is None:
                    continue
                transition = _apply_observation(row, item, now)
                session.flush()

            results.append({"incident_id": row.incident_id, "transition": transition, "fingerprint": row.fingerprint})

    return results


def touch_applications(items: list[dict[str, Any]]) -> int:
    """Upsert the set of monitored applications seen in telemetry."""
    if not items:
        return 0
    now = _now()
    with session_scope() as session:
        for item in items:
            app_id = item["app_id"]
            row = session.get(ApplicationRow, app_id)
            if row is None:
                session.add(
                    ApplicationRow(
                        app_id=app_id,
                        cluster_id=item.get("cluster_id"),
                        namespace=item.get("namespace"),
                        labels=item.get("labels", {}),
                        first_seen=now,
                        last_seen=now,
                    )
                )
            else:
                row.last_seen = now
                if item.get("cluster_id"):
                    row.cluster_id = item["cluster_id"]
                if item.get("namespace"):
                    row.namespace = item["namespace"]
                if item.get("labels"):
                    row.labels = {**(row.labels or {}), **item["labels"]}
        session.flush()
    return len(items)


def resolve_stale(older_than_seconds: int | None = None) -> int:
    """Mark incidents RESOLVED when no observation arrived within the window."""
    seconds = settings.incident_resolve_after_seconds if older_than_seconds is None else older_than_seconds
    cutoff = _now() - timedelta(seconds=seconds)
    with session_scope() as session:
        rows = session.execute(
            select(IncidentRow).where(
                IncidentRow.status != IncidentStatus.RESOLVED.value,
                IncidentRow.last_detected <= cutoff,
            )
        ).scalars().all()
        now = _now()
        for row in rows:
            row.status = IncidentStatus.RESOLVED.value
            row.resolved_at = now
            row.updated_at = now
        session.flush()
        return len(rows)


def list_incidents(
    status: str | None = None,
    severity: str | None = None,
    app_id: str | None = None,
    cluster_id: str | None = None,
    anomaly_type: str | None = None,
    since: datetime | None = None,
    limit: int = 100,
    offset: int = 0,
    order: str = "last_detected",
) -> list[dict[str, Any]]:
    with session_scope() as session:
        stmt = select(IncidentRow)
        if status:
            stmt = stmt.where(IncidentRow.status.in_([s.strip().upper() for s in status.split(",")]))
        if severity:
            stmt = stmt.where(IncidentRow.severity.in_([s.strip().lower() for s in severity.split(",")]))
        if app_id:
            stmt = stmt.where(IncidentRow.app_id == app_id)
        if cluster_id:
            stmt = stmt.where(IncidentRow.cluster_id == cluster_id)
        if anomaly_type:
            stmt = stmt.where(IncidentRow.anomaly_type == anomaly_type)
        if since is not None:
            stmt = stmt.where(IncidentRow.last_detected >= since)
        column = getattr(IncidentRow, order, IncidentRow.last_detected)
        stmt = stmt.order_by(column.desc()).limit(limit).offset(offset)
        return [row_to_dict(row) for row in session.execute(stmt).scalars().all()]


def get_incident(incident_id: str) -> dict[str, Any] | None:
    with session_scope() as session:
        row = session.execute(
            select(IncidentRow).where(IncidentRow.incident_id == incident_id)
        ).scalars().first()
        return row_to_dict(row) if row else None


def attach_ai(incident_id: str, analysis: dict[str, Any], remediation: str | None = None) -> dict[str, Any] | None:
    with session_scope() as session:
        row = session.execute(
            select(IncidentRow).where(IncidentRow.incident_id == incident_id)
        ).scalars().first()
        if row is None:
            return None
        row.ai_analysis = analysis
        if remediation:
            row.remediation = remediation
        row.updated_at = _now()
        session.flush()
        return row_to_dict(row)


def counts_by(column: str, status: str | None = None) -> dict[str, int]:
    field = getattr(IncidentRow, column)
    with session_scope() as session:
        stmt = select(field, func.count()).group_by(field)
        if status:
            stmt = stmt.where(IncidentRow.status.in_([s.strip().upper() for s in status.split(",")]))
        return {str(key): value for key, value in session.execute(stmt).all()}


def summary() -> dict[str, Any]:
    with session_scope() as session:
        total = session.execute(select(func.count()).select_from(IncidentRow)).scalar_one()
        active = session.execute(
            select(func.count()).select_from(IncidentRow).where(IncidentRow.status != IncidentStatus.RESOLVED.value)
        ).scalar_one()
        resolved = session.execute(
            select(func.count()).select_from(IncidentRow).where(IncidentRow.status == IncidentStatus.RESOLVED.value)
        ).scalar_one()
        apps = session.execute(select(func.count()).select_from(ApplicationRow)).scalar_one()
        anomalous_apps = session.execute(
            select(func.count(func.distinct(IncidentRow.app_id))).where(
                IncidentRow.status != IncidentStatus.RESOLVED.value
            )
        ).scalar_one()
    return {
        "applications_monitored": apps,
        "applications_with_anomalies": anomalous_apps,
        "applications_healthy": max(0, apps - anomalous_apps),
        "incidents_total": total,
        "incidents_active": active,
        "incidents_resolved": resolved,
    }


def application_health(limit: int = 500) -> list[dict[str, Any]]:
    """Per-application health derived from active incidents."""
    with session_scope() as session:
        apps = session.execute(select(ApplicationRow).order_by(ApplicationRow.app_id)).scalars().all()
        active = session.execute(
            select(IncidentRow).where(IncidentRow.status != IncidentStatus.RESOLVED.value)
        ).scalars().all()

    by_app: dict[str, list[IncidentRow]] = {}
    for row in active:
        by_app.setdefault(row.app_id, []).append(row)

    results: list[dict[str, Any]] = []
    for app in apps:
        rows = by_app.get(app.app_id, [])
        worst = min(rows, key=lambda r: SEVERITY_RANK.get(r.severity, 9)) if rows else None
        results.append(
            {
                "app_id": app.app_id,
                "cluster_id": app.cluster_id,
                "namespace": app.namespace,
                "labels": app.labels or {},
                "last_seen": _iso(app.last_seen),
                "active_incidents": len(rows),
                "status": "anomalous" if rows else "healthy",
                "worst_severity": worst.severity if worst else None,
                "last_detected": _iso(max((r.last_detected for r in rows), default=None)),
            }
        )
    results.sort(key=lambda item: (item["status"] != "anomalous", -(item["active_incidents"] or 0)))
    return results[:limit]


def cluster_health() -> list[dict[str, Any]]:
    with session_scope() as session:
        apps = session.execute(select(ApplicationRow)).scalars().all()
        active = session.execute(
            select(IncidentRow).where(IncidentRow.status != IncidentStatus.RESOLVED.value)
        ).scalars().all()

    app_counts: dict[str, int] = {}
    for app in apps:
        if app.cluster_id:
            app_counts[app.cluster_id] = app_counts.get(app.cluster_id, 0) + 1
    incident_counts: dict[str, int] = {}
    for row in active:
        if row.cluster_id:
            incident_counts[row.cluster_id] = incident_counts.get(row.cluster_id, 0) + 1

    clusters = sorted(set(app_counts) | set(incident_counts))
    return [
        {
            "cluster_id": cluster,
            "applications": app_counts.get(cluster, 0),
            "active_incidents": incident_counts.get(cluster, 0),
            "status": "anomalous" if incident_counts.get(cluster, 0) else "healthy",
        }
        for cluster in clusters
    ]


def detector_activity() -> list[dict[str, Any]]:
    with session_scope() as session:
        rows = session.execute(
            select(IncidentRow.detector_id, func.count(), func.max(IncidentRow.last_detected))
            .group_by(IncidentRow.detector_id)
        ).all()
    return [
        {"detector_id": detector_id, "incidents": count, "last_detected": _iso(last)}
        for detector_id, count, last in rows
    ]
