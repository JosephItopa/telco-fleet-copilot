"""Threshold-based anomaly detection over normalized health snapshots."""

from __future__ import annotations

from typing import Any

from . import config


def _finding(anomaly: str, severity: str, confidence: float, reason: str) -> dict[str, Any]:
    return {
        "anomaly": anomaly,
        "severity": severity,
        "confidence": round(confidence, 2),
        "reason": reason,
    }


def detect_app(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    metrics = snapshot.get("metrics", {})
    findings: list[dict[str, Any]] = []

    health = metrics.get("health")
    if health is not None and health < 1:
        findings.append(
            _finding("service_down", "critical", 0.99, "Health gauge reports the app unhealthy (health=0).")
        )

    requests = metrics.get("requests_by_status", {})
    total_requests = sum(requests.values())
    error_requests = sum(value for status, value in requests.items() if str(status).startswith("5"))
    error_ratio = (error_requests / total_requests) if total_requests else 0.0
    if error_ratio >= config.THRESH_ERROR_RATIO:
        findings.append(
            _finding(
                "error_spike",
                "high",
                min(0.99, 0.5 + error_ratio / 2),
                f"{error_requests:.0f}/{total_requests:.0f} requests failed "
                f"(5xx ratio {error_ratio:.1%}).",
            )
        )

    latency_count = metrics.get("latency_count", 0.0)
    latency_sum = metrics.get("latency_sum", 0.0)
    avg_latency = (latency_sum / latency_count) if latency_count else 0.0
    if avg_latency >= config.THRESH_LATENCY_SECONDS:
        findings.append(
            _finding(
                "latency_spike",
                "high",
                0.9,
                f"Average latency {avg_latency:.2f}s exceeds the {config.THRESH_LATENCY_SECONDS}s threshold.",
            )
        )

    dep_failures = metrics.get("dependency_failures_total", 0.0)
    if dep_failures >= config.THRESH_DEP_FAILURES:
        findings.append(
            _finding(
                "dependency_failure",
                "medium",
                0.7,
                f"Observed {dep_failures:.0f} downstream dependency failures.",
            )
        )

    failures = metrics.get("failures_total", 0.0)
    if failures >= config.THRESH_FAILURES and error_ratio < config.THRESH_ERROR_RATIO:
        findings.append(
            _finding(
                "payment_failures",
                "medium",
                0.65,
                f"{failures:.0f} failures recorded without a matching 5xx ratio.",
            )
        )

    return findings


def detect_cluster(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    metrics = snapshot.get("metrics", {})
    findings: list[dict[str, Any]] = []

    if metrics.get("health") is not None and metrics["health"] < 1:
        findings.append(
            _finding("cluster_unhealthy", "critical", 0.99, "Cluster health probe reports unhealthy.")
        )

    total_nodes = metrics.get("nodes_total")
    ready_nodes = metrics.get("nodes_ready")
    nodes_down = metrics.get("nodes_down", 0)
    down = 0
    if total_nodes and ready_nodes is not None and ready_nodes < total_nodes:
        down = int(total_nodes - ready_nodes)
    down = max(down, int(nodes_down))
    if down > 0:
        findings.append(
            _finding(
                "node_down",
                "critical",
                0.95,
                f"{down} node(s) are not ready or reporting down in this cluster.",
            )
        )

    cpu = metrics.get("cpu_usage_ratio", 0.0)
    if cpu >= config.THRESH_CLUSTER_CPU:
        findings.append(
            _finding("high_cpu", "high", 0.85, f"Cluster CPU usage is {cpu:.0%}, above the {config.THRESH_CLUSTER_CPU:.0%} threshold.")
        )

    memory = metrics.get("memory_usage_ratio", 0.0)
    if memory >= config.THRESH_CLUSTER_MEMORY:
        findings.append(
            _finding(
                "high_memory",
                "high",
                0.85,
                f"Cluster memory usage is {memory:.0%}, above the {config.THRESH_CLUSTER_MEMORY:.0%} threshold.",
            )
        )

    pending = metrics.get("pods_pending", 0.0)
    if pending >= config.THRESH_PODS_PENDING:
        findings.append(
            _finding("scheduling_pressure", "medium", 0.7, f"{pending:.0f} pods are pending scheduling.")
        )

    unavailable = metrics.get("workloads_unavailable_total", 0.0)
    if unavailable > 0:
        findings.append(
            _finding(
                "workloads_unavailable",
                "high",
                0.8,
                f"{unavailable:.0f} workload(s) have unavailable replicas.",
            )
        )

    return findings


def detect(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    if snapshot.get("kind") == "cluster":
        return detect_cluster(snapshot)
    return detect_app(snapshot)
