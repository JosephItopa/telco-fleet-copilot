"""Configurable anomaly detection rules over normalized telemetry."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any

from config.settings import settings
from models.schema import AnomalyType, MetricType, Severity


@dataclass
class Detection:
    anomaly_type: str
    metric: str
    severity: str
    description: str
    evidence: list[dict[str, Any]] = field(default_factory=list)


def _latest_by_type(events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[event["metric_type"]].append(event)
    return grouped


class RuleEngine:
    """Stateful rules engine.

    A detector instance owns a fixed set of partitions (by Kafka assignment), so
    per-application history kept in process is consistent for that application.
    """

    def __init__(self, history_size: int = 30) -> None:
        self.memory_history: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=history_size))
        self.restart_history: dict[str, float] = {}

    def evaluate_app(self, app_id: str, events: list[dict[str, Any]]) -> list[Detection]:
        grouped = _latest_by_type(events)
        detections: list[Detection] = []

        cpu_events = grouped.get(MetricType.POD_CPU.value, [])
        if cpu_events:
            worst = max(cpu_events, key=lambda event: event["metrics"].get("ratio") or 0)
            ratio = worst["metrics"].get("ratio")
            if ratio is not None and ratio >= settings.thresh_cpu_ratio:
                detections.append(
                    Detection(
                        AnomalyType.CPU_SATURATION.value,
                        "pod_cpu",
                        Severity.HIGH.value,
                        f"Pod CPU usage is {ratio:.0%} of its limit (threshold {settings.thresh_cpu_ratio:.0%}).",
                        [worst],
                    )
                )

        memory_events = grouped.get(MetricType.POD_MEMORY.value, [])
        if memory_events:
            worst = max(memory_events, key=lambda event: event["metrics"].get("ratio") or 0)
            metrics = worst["metrics"]
            ratio = metrics.get("ratio")
            if ratio is not None and ratio >= settings.thresh_memory_ratio:
                detections.append(
                    Detection(
                        AnomalyType.MEMORY_SATURATION.value,
                        "pod_memory",
                        Severity.HIGH.value,
                        f"Pod memory usage is {ratio:.0%} of its limit (threshold {settings.thresh_memory_ratio:.0%}).",
                        [worst],
                    )
                )
            usage = metrics.get("usage_mb")
            if usage is not None:
                history = self.memory_history[app_id]
                baseline = min(history) if history else usage
                history.append(float(usage))
                if baseline > 0 and (float(usage) - baseline) / baseline >= settings.thresh_growth_ratio:
                    detections.append(
                        Detection(
                            AnomalyType.ABNORMAL_RESOURCE_GROWTH.value,
                            "pod_memory_growth",
                            Severity.MEDIUM.value,
                            f"Pod memory grew {(float(usage) - baseline) / baseline:.0%} over the detection window.",
                            [worst],
                        )
                    )

        restart_events = grouped.get(MetricType.CONTAINER_RESTARTS.value, [])
        if restart_events:
            worst = max(restart_events, key=lambda event: event["metrics"].get("restarts") or 0)
            restarts = float(worst["metrics"].get("restarts") or 0)
            previous = self.restart_history.get(app_id)
            self.restart_history[app_id] = restarts
            delta = restarts - previous if previous is not None else restarts
            if restarts >= settings.thresh_restarts or delta >= settings.thresh_restarts:
                detections.append(
                    Detection(
                        AnomalyType.CONTAINER_RESTART_STORM.value,
                        "container_restarts",
                        Severity.HIGH.value,
                        f"Container restarted {int(delta)} time(s) in the window (total {int(restarts)}).",
                        [worst],
                    )
                )

        replica_events = grouped.get(MetricType.DEPLOYMENT_REPLICAS.value, [])
        if replica_events:
            worst = max(replica_events, key=lambda event: event["metrics"].get("unavailable") or 0)
            metrics = worst["metrics"]
            desired = metrics.get("desired") or 0
            available = metrics.get("available") or 0
            unavailable = metrics.get("unavailable") or 0
            if desired and available == 0:
                detections.append(
                    Detection(
                        AnomalyType.UNAVAILABLE_REPLICAS.value,
                        "deployment_replicas",
                        Severity.CRITICAL.value,
                        f"All {desired} desired replicas are unavailable.",
                        [worst],
                    )
                )
            elif unavailable:
                detections.append(
                    Detection(
                        AnomalyType.DEPLOYMENT_DEGRADATION.value,
                        "deployment_replicas",
                        Severity.MEDIUM.value,
                        f"{available}/{desired} replicas available ({unavailable} unavailable).",
                        [worst],
                    )
                )

        status_events = grouped.get(MetricType.POD_STATUS.value, [])
        unhealthy = [
            event
            for event in status_events
            if not event["metrics"].get("ready")
            or event["metrics"].get("waiting_reason") in {"CrashLoopBackOff", "ImagePullBackOff", "Error"}
        ]
        if unhealthy:
            worst = unhealthy[0]
            detections.append(
                Detection(
                    AnomalyType.UNHEALTHY_POD_STATE.value,
                    "pod_status",
                    Severity.HIGH.value,
                    f"Pod is not ready (reason: {worst['metrics'].get('waiting_reason') or 'NotReady'}).",
                    unhealthy,
                )
            )

        node_cpu = grouped.get(MetricType.NODE_CPU.value, [])
        if node_cpu:
            worst = max(node_cpu, key=lambda event: event["metrics"].get("ratio") or 0)
            ratio = worst["metrics"].get("ratio")
            if ratio is not None and ratio >= settings.thresh_node_cpu_ratio:
                detections.append(
                    Detection(
                        AnomalyType.NODE_RESOURCE_EXHAUSTION.value,
                        "node_cpu",
                        Severity.CRITICAL.value,
                        f"Node CPU is {ratio:.0%} (threshold {settings.thresh_node_cpu_ratio:.0%}).",
                        [worst],
                    )
                )

        node_memory = grouped.get(MetricType.NODE_MEMORY.value, [])
        if node_memory:
            worst = max(node_memory, key=lambda event: event["metrics"].get("ratio") or 0)
            ratio = worst["metrics"].get("ratio")
            if ratio is not None and ratio >= settings.thresh_node_memory_ratio:
                detections.append(
                    Detection(
                        AnomalyType.NODE_RESOURCE_EXHAUSTION.value,
                        "node_memory",
                        Severity.CRITICAL.value,
                        f"Node memory is {ratio:.0%} (threshold {settings.thresh_node_memory_ratio:.0%}).",
                        [worst],
                    )
                )

        node_conditions = grouped.get(MetricType.NODE_CONDITION.value, [])
        pressure = [
            event
            for event in node_conditions
            if not event["metrics"].get("ready") or event["metrics"].get("memory_pressure")
        ]
        if pressure:
            detections.append(
                Detection(
                    AnomalyType.NODE_RESOURCE_EXHAUSTION.value,
                    "node_condition",
                    Severity.CRITICAL.value,
                    "Node reports NotReady or memory pressure.",
                    pressure,
                )
            )

        return detections
