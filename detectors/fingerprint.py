"""Incident fingerprinting.

The fingerprint is the stable identity of an anomaly: application + anomaly type
+ relevant metric. The detection window governs when an incident goes stale
(see ``incident_resolve_after_seconds``), it is deliberately not part of the
fingerprint so repeated observations update one incident instead of creating a
new one every window.
"""

from __future__ import annotations

import hashlib


def compute_fingerprint(app_id: str, anomaly_type: str, metric: str | None) -> str:
    raw = f"{app_id}|{anomaly_type}|{metric or ''}".lower()
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()
