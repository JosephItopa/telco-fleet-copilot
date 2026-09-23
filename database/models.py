"""Persistence models for incidents, their lifecycle, and monitored applications."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime, Index, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

JSONType = JSON().with_variant(JSONB, "postgresql")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class IncidentRow(Base):
    __tablename__ = "incidents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    fingerprint: Mapped[str] = mapped_column(String(128), index=True)
    app_id: Mapped[str] = mapped_column(String(256), index=True)
    cluster_id: Mapped[str | None] = mapped_column(String(128), index=True, nullable=True)
    namespace: Mapped[str | None] = mapped_column(String(256), nullable=True)
    detector_id: Mapped[str | None] = mapped_column(String(128), index=True, nullable=True)
    kafka_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    anomaly_type: Mapped[str] = mapped_column(String(64), index=True)
    metric: Mapped[str | None] = mapped_column(String(64), nullable=True)
    severity: Mapped[str] = mapped_column(String(16), index=True)
    status: Mapped[str] = mapped_column(String(16), index=True, default="NEW")
    evidence: Mapped[list] = mapped_column(JSONType, default=list)
    observation_count: Mapped[int] = mapped_column(Integer, default=1)
    first_detected: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, index=True)
    last_detected: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, index=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ai_analysis: Mapped[dict | None] = mapped_column(JSONType, nullable=True)
    remediation: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)

    __table_args__ = (
        # One active (non-resolved) incident per fingerprint => idempotent upserts.
        Index(
            "ux_incidents_active_fingerprint",
            "fingerprint",
            unique=True,
            postgresql_where=text("status <> 'RESOLVED'"),
            sqlite_where=text("status <> 'RESOLVED'"),
        ),
    )


class ApplicationRow(Base):
    __tablename__ = "applications"

    app_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    cluster_id: Mapped[str | None] = mapped_column(String(128), index=True, nullable=True)
    namespace: Mapped[str | None] = mapped_column(String(256), nullable=True)
    labels: Mapped[dict] = mapped_column(JSONType, default=dict)
    first_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, index=True)
