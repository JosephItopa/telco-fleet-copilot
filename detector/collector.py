"""Telemetry collection: query Prometheus, or fall back to sample records."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx

from . import config, samples

APP_QUERY = '{__name__=~"api_.*|payment_.*", app=~".+"}'
CLUSTER_QUERY = '{__name__=~"cluster_.*", cluster=~".+"}'


@dataclass
class CollectionResult:
    source: str
    apps: list[dict[str, Any]] = field(default_factory=list)
    clusters: list[dict[str, Any]] = field(default_factory=list)
    series_count: int = 0
    error: str | None = None
    collected_at: str = ""


def _value(series: dict[str, Any]) -> float | None:
    value = series.get("value")
    if not isinstance(value, (list, tuple)) or len(value) < 2:
        return None
    try:
        return float(value[1])
    except (TypeError, ValueError):
        return None


def _prom_query_sync(query: str) -> list[dict[str, Any]]:
    with httpx.Client(timeout=config.COLLECT_TIMEOUT_SECONDS) as client:
        response = client.get(f"{config.PROMETHEUS_URL}/api/v1/query", params={"query": query})
        response.raise_for_status()
        payload = response.json()
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {payload.get('error', 'unknown error')}")
    return payload.get("data", {}).get("result", [])


async def _query_with_retries(query: str) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(1, config.COLLECT_RETRIES + 1):
        try:
            return await asyncio.to_thread(_prom_query_sync, query)
        except Exception as exc:  # noqa: BLE001 - surfaced to caller
            last_error = exc
            if attempt < config.COLLECT_RETRIES:
                await asyncio.sleep(1.0 * attempt)
    raise RuntimeError(f"collection failed after {config.COLLECT_RETRIES} attempts: {last_error}")


def normalize_apps(series_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str | None], dict[str, Any]] = {}
    for item in series_list:
        metric = item.get("metric", {})
        app = metric.get("app")
        if not app:
            continue
        cluster = metric.get("cluster")
        snapshot = grouped.setdefault(
            (app, cluster),
            {
                "target": app,
                "kind": "app",
                "cluster": cluster,
                "namespace": metric.get("namespace"),
                "pod": metric.get("pod"),
                "instance": metric.get("instance"),
                "metrics": {"requests_by_status": {}},
            },
        )
        name = metric.get("__name__")
        value = _value(item)
        if name is None or value is None:
            continue
        metrics = snapshot["metrics"]
        if name in {"api_requests_total", "payment_requests_total"}:
            metrics["requests_by_status"][str(metric.get("status", "unknown"))] = value
        elif name in {"api_health", "payment_health"}:
            metrics["health"] = value
        elif name in {"api_request_latency_seconds_sum", "payment_request_latency_seconds_sum"}:
            metrics["latency_sum"] = value
        elif name in {"api_request_latency_seconds_count", "payment_request_latency_seconds_count"}:
            metrics["latency_count"] = value
        elif name == "api_dependency_failures_total":
            metrics["dependency_failures_total"] = value
        elif name == "payment_failures_total":
            metrics["failures_total"] = metrics.get("failures_total", 0.0) + value
        elif name == "payment_failure_switch":
            metrics["failure_switch"] = value

    apps = []
    for snapshot in grouped.values():
        if snapshot["metrics"].get("health") is not None or snapshot["metrics"]["requests_by_status"]:
            apps.append(snapshot)
    return apps


def normalize_clusters(series_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    simple = {
        "cluster_health": "health",
        "cluster_nodes_total": "nodes_total",
        "cluster_nodes_ready": "nodes_ready",
        "cluster_cpu_usage_ratio": "cpu_usage_ratio",
        "cluster_memory_usage_ratio": "memory_usage_ratio",
        "cluster_pods_total": "pods_total",
        "cluster_pods_pending": "pods_pending",
    }
    for item in series_list:
        metric = item.get("metric", {})
        cluster = metric.get("cluster")
        if not cluster:
            continue
        snapshot = grouped.setdefault(
            cluster,
            {
                "target": cluster,
                "kind": "cluster",
                "cluster": cluster,
                "namespace": metric.get("namespace"),
                "instance": None,
                "metrics": {},
            },
        )
        name = metric.get("__name__")
        value = _value(item)
        if name is None or value is None:
            continue
        metrics = snapshot["metrics"]
        if name in simple:
            metrics[simple[name]] = value
        elif name == "cluster_workloads_unavailable_total":
            metrics["workloads_unavailable_total"] = metrics.get("workloads_unavailable_total", 0.0) + value
        elif name == "cluster_node_up":
            metrics["nodes_down"] = metrics.get("nodes_down", 0) + (1 if value == 0 else 0)
    return list(grouped.values())


async def collect() -> CollectionResult:
    try:
        app_series = await _query_with_retries(APP_QUERY)
        cluster_series = await _query_with_retries(CLUSTER_QUERY)
        if not app_series and not cluster_series:
            raise RuntimeError("Prometheus returned no series")
        source, error = "prometheus", None
    except Exception as exc:  # noqa: BLE001 - fall back so the demo never goes dark
        if not config.FALLBACK_ENABLED:
            raise
        app_series = []
        cluster_series = []
        source, error = "fallback", str(exc)

    if source == "fallback":
        fallback = samples.fallback_series()
        app_series = [s for s in fallback if not str(s["metric"]["__name__"]).startswith("cluster_")]
        cluster_series = [s for s in fallback if str(s["metric"]["__name__"]).startswith("cluster_")]

    return CollectionResult(
        source=source,
        apps=normalize_apps(app_series),
        clusters=normalize_clusters(cluster_series),
        series_count=len(app_series) + len(cluster_series),
        error=error,
        collected_at=datetime.now(timezone.utc).isoformat(),
    )
