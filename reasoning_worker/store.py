"""In-memory findings store for the prototype reasoning worker."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from common.models import Finding

_SEVERITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}


def _age_seconds(created_at: str) -> float | None:
    try:
        created = datetime.fromisoformat(created_at)
    except ValueError:
        return None
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - created).total_seconds()


class FindingStore:
    def __init__(self, max_items: int = 500) -> None:
        self._items: dict[str, Finding] = {}
        self._lock = asyncio.Lock()
        self._max_items = max_items

    async def upsert(self, finding: Finding) -> Finding:
        async with self._lock:
            self._items[finding.incident_id] = finding
            if len(self._items) > self._max_items:
                oldest = sorted(self._items.values(), key=lambda f: f.created_at)[: len(self._items) - self._max_items]
                for item in oldest:
                    self._items.pop(item.incident_id, None)
            return self._items[finding.incident_id]

    async def get(self, incident_id: str) -> Finding | None:
        async with self._lock:
            return self._items.get(incident_id)

    async def find_active_signature(self, finding: Finding, window_seconds: int) -> Finding | None:
        """Return a recent finding with the same target/cluster/anomaly, if any.

        Makes ingestion idempotent across detector restarts, which reset the
        detector's in-memory cooldown.
        """
        signature = (finding.target, finding.kind, finding.cluster, finding.anomaly)
        async with self._lock:
            candidates = list(self._items.values())
        for item in candidates:
            if (item.target, item.kind, item.cluster, item.anomaly) != signature:
                continue
            age = _age_seconds(item.created_at)
            if age is not None and age < window_seconds:
                return item
        return None

    async def list(
        self,
        severity: str | None = None,
        kind: str | None = None,
        status: str | None = None,
        limit: int = 200,
    ) -> list[Finding]:
        async with self._lock:
            items = list(self._items.values())
        if severity:
            wanted = {s.strip().lower() for s in severity.split(",")}
            items = [f for f in items if f.severity in wanted]
        if kind:
            items = [f for f in items if f.kind == kind]
        if status:
            items = [f for f in items if f.status == status]
        items.sort(key=lambda f: f.created_at, reverse=True)
        items.sort(key=lambda f: _SEVERITY_ORDER.get(f.severity, 9))
        return items[:limit]

    async def count(self) -> int:
        async with self._lock:
            return len(self._items)
