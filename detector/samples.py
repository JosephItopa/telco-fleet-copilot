"""Fallback sample telemetry, shaped like a Prometheus `/api/v1/query` vector.

Used when Prometheus is unreachable so the demo dashboard always has data.
The samples mirror app health (api-service, payment-service) and cluster health
(cluster-a, cluster-b, cluster-c) across two clusters.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _series(ts: int) -> list[dict[str, Any]]:
    def s(name: str, labels: dict[str, Any], value: float) -> dict[str, Any]:
        return {"metric": {"__name__": name, **labels}, "value": [ts, str(value)]}

    out: list[dict[str, Any]] = []

    # --- api-service, cluster-a: healthy -----------------------------------
    api_healthy = {
        "app": "api-service",
        "cluster": "cluster-a",
        "namespace": "aiops-demo",
        "pod": "api-service-7d9f4c8b5-x2k9p",
        "instance": "10.244.1.15:8000",
    }
    out += [
        s("api_health", api_healthy, 1.0),
        s("api_requests_total", {**api_healthy, "endpoint": "/call-payment", "status": "200"}, 18432),
        s("api_request_latency_seconds_sum", {**api_healthy, "endpoint": "/call-payment"}, 412.77),
        s("api_request_latency_seconds_count", {**api_healthy, "endpoint": "/call-payment"}, 18432),
        s("api_dependency_failures_total", {**api_healthy, "dependency": "payment-service"}, 3),
    ]

    # --- payment-service, cluster-a: injected high latency ------------------
    payment_latency = {
        "app": "payment-service",
        "cluster": "cluster-a",
        "namespace": "aiops-demo",
        "pod": "payment-service-6c8b9f7d4-q8w2r",
        "instance": "10.244.1.22:8001",
    }
    out += [
        s("payment_health", payment_latency, 1.0),
        s("payment_failure_switch", payment_latency, 1.0),
        s("payment_requests_total", {**payment_latency, "status": "200"}, 9640),
        s("payment_request_latency_seconds_sum", payment_latency, 24118.6),
        s("payment_request_latency_seconds_count", payment_latency, 9640),
        s("payment_failures_total", {**payment_latency, "failure_mode": "high_latency"}, 0),
    ]

    # --- payment-service, cluster-b: service down + error spike -------------
    payment_down = {
        "app": "payment-service",
        "cluster": "cluster-b",
        "namespace": "aiops-demo",
        "pod": "payment-service-6c8b9f7d4-q8w2r",
        "instance": "10.244.2.31:8001",
    }
    out += [
        s("payment_health", payment_down, 0.0),
        s("payment_failure_switch", payment_down, 3.0),
        s("payment_requests_total", {**payment_down, "status": "500"}, 742),
        s("payment_requests_total", {**payment_down, "status": "503"}, 2198),
        s("payment_request_latency_seconds_sum", payment_down, 3.11),
        s("payment_request_latency_seconds_count", payment_down, 2940),
        s("payment_failures_total", {**payment_down, "failure_mode": "error_spike"}, 517),
        s("payment_failures_total", {**payment_down, "failure_mode": "service_down"}, 2198),
    ]

    # --- cluster-a: healthy ------------------------------------------------
    cluster_a = {"cluster": "cluster-a", "provider": "openshift", "region": "lagos-1"}
    out += [
        s("cluster_health", cluster_a, 1.0),
        s("cluster_nodes_total", cluster_a, 12),
        s("cluster_nodes_ready", cluster_a, 12),
        s("cluster_cpu_usage_ratio", cluster_a, 0.41),
        s("cluster_memory_usage_ratio", cluster_a, 0.58),
        s("cluster_pods_total", cluster_a, 214),
        s("cluster_pods_pending", cluster_a, 2),
        s("cluster_workloads_unavailable_total", cluster_a, 0),
    ]

    # --- cluster-b: CPU and memory pressure --------------------------------
    cluster_b = {"cluster": "cluster-b", "provider": "openshift", "region": "lagos-2"}
    out += [
        s("cluster_health", cluster_b, 1.0),
        s("cluster_nodes_total", cluster_b, 8),
        s("cluster_nodes_ready", cluster_b, 8),
        s("cluster_cpu_usage_ratio", cluster_b, 0.93),
        s("cluster_memory_usage_ratio", cluster_b, 0.88),
        s("cluster_pods_total", cluster_b, 176),
        s("cluster_pods_pending", cluster_b, 41),
        s("cluster_workloads_unavailable_total", cluster_b, 6),
    ]

    # --- cluster-c: node down ----------------------------------------------
    cluster_c = {"cluster": "cluster-c", "provider": "openshift", "region": "accra-1"}
    out += [
        s("cluster_health", cluster_c, 0.0),
        s("cluster_nodes_total", cluster_c, 10),
        s("cluster_nodes_ready", cluster_c, 7),
        s("cluster_cpu_usage_ratio", cluster_c, 0.71),
        s("cluster_memory_usage_ratio", cluster_c, 0.69),
        s("cluster_pods_total", cluster_c, 189),
        s("cluster_pods_pending", cluster_c, 63),
        s("cluster_workloads_unavailable_total", cluster_c, 14),
        s("cluster_node_up", {**cluster_c, "node": "worker-07", "instance": "10.244.3.7:9100"}, 0),
        s("cluster_node_up", {**cluster_c, "node": "worker-08", "instance": "10.244.3.8:9100"}, 0),
    ]

    return out


def fallback_series() -> list[dict[str, Any]]:
    return _series(_now_ms())
