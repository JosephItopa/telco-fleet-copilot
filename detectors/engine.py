"""Detector engine: applies rules, deduplicates via fingerprints, persists
incidents with lifecycle transitions."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from config.logging_utils import get_logger
from config.metrics import DB_WRITES, INCIDENTS
from database.repository import record_observations, touch_applications
from detectors.fingerprint import compute_fingerprint
from detectors.rules import RuleEngine

logger = get_logger("detectors.engine")


class DetectorEngine:
    def __init__(self, detector_id: str) -> None:
        self.detector_id = detector_id
        self.rules = RuleEngine()

    def evaluate(self, events: list[dict[str, Any]], kafka_partition: int | None = None) -> dict[str, Any]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for event in events:
            grouped[event.get("app_id") or "unknown"].append(event)

        applications = []
        for app_id, app_events in grouped.items():
            sample = app_events[0]
            applications.append(
                {
                    "app_id": app_id,
                    "cluster_id": sample.get("cluster_id"),
                    "namespace": sample.get("namespace"),
                    "labels": sample.get("labels", {}),
                }
            )
        touched = touch_applications(applications)

        observations: list[dict[str, Any]] = []
        for app_id, app_events in grouped.items():
            sample = app_events[0]
            for detection in self.rules.evaluate_app(app_id, app_events):
                observations.append(
                    {
                        "fingerprint": compute_fingerprint(app_id, detection.anomaly_type, detection.metric),
                        "app_id": app_id,
                        "cluster_id": sample.get("cluster_id"),
                        "namespace": sample.get("namespace"),
                        "detector_id": self.detector_id,
                        "kafka_partition": kafka_partition,
                        "anomaly_type": detection.anomaly_type,
                        "metric": detection.metric,
                        "severity": detection.severity,
                        "evidence": detection.evidence,
                    }
                )

        results = record_observations(observations)
        transitions: dict[str, int] = {"NEW": 0, "ONGOING": 0}
        for result in results:
            transitions[result["transition"]] = transitions.get(result["transition"], 0) + 1
            INCIDENTS.labels("detector", result["transition"]).inc()
        if results:
            DB_WRITES.labels("detector", "incident_upsert").inc(len(results))

        if results:
            logger.info(
                "detector evaluated batch",
                extra={
                    "events": len(events),
                    "apps": len(grouped),
                    "incidents": len(results),
                    "new": transitions.get("NEW", 0),
                    "ongoing": transitions.get("ONGOING", 0),
                },
            )

        return {
            "detector_id": self.detector_id,
            "events_evaluated": len(events),
            "applications_touched": touched,
            "detections": len(observations),
            "incidents": results,
            "transitions": transitions,
        }
