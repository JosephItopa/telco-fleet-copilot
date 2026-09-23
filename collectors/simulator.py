"""Simulated Kubernetes telemetry source.

Used when COLLECTOR_MODE=simulated so the whole platform runs without a live
cluster. It produces the same normalized MetricEvent schema as the real
Kubernetes collector.
"""

from __future__ import annotations

import random
from typing import Any

from models.schema import MetricType, utcnow_iso

FAULTS = [
    "cpu_saturation",
    "memory_saturation",
    "container_restart_storm",
    "unavailable_replicas",
    "abnormal_resource_growth",
    "unhealthy_pod_state",
    "node_resource_exhaustion",
]


class SimulatedSource:
    def __init__(
        self,
        clusters: list[str],
        apps_per_cluster: int,
        unhealthy_ratio: float,
        seed: int = 42,
    ) -> None:
        self.random = random.Random(seed)
        self.clusters = clusters or ["cluster-a"]
        self.apps: list[dict[str, Any]] = []
        for cluster in self.clusters:
            for index in range(apps_per_cluster):
                app_id = f"app-{cluster}-{index:03d}"
                self.apps.append(
                    {
                        "app_id": app_id,
                        "cluster_id": cluster,
                        "namespace": f"ns-{cluster}",
                        "pod": f"{app_id}-{index % 7}",
                        "labels": {
                            "team": f"team-{index % 5}",
                            "tier": self.random.choice(["web", "api", "worker"]),
                            "kind": "workload",
                        },
                    }
                )
        self.nodes = [
            {
                "app_id": f"node-{cluster}-{n}",
                "cluster_id": cluster,
                "namespace": None,
                "pod": None,
                "labels": {"kind": "node", "node": f"worker-{n:02d}"},
            }
            for cluster in self.clusters
            for n in range(3)
        ]
        self.faults: dict[str, str] = {}
        for app in self.apps:
            if self.random.random() < unhealthy_ratio:
                self.faults[app["app_id"]] = self.random.choice(FAULTS)
        for node in self.nodes:
            if self.random.random() < 0.15:
                self.faults[node["app_id"]] = "node_resource_exhaustion"
        self.state: dict[str, dict[str, float]] = {}

    def _state(self, app_id: str) -> dict[str, float]:
        return self.state.setdefault(
            {"app_id": app_id}.get("app_id", app_id),
            {"restarts": 0.0, "memory_mb": 128.0, "cpu_cores": 0.25},
        )

    def _workload_events(self, app: dict[str, Any], collector_id: str) -> list[dict[str, Any]]:
        state = self._state(app["app_id"])
        fault = self.faults.get(app["app_id"])
        ts = utcnow_iso()
        cpu_limit = 1.0
        memory_limit_mb = 512.0

        cpu = state["cpu_cores"] + self.random.uniform(-0.05, 0.05)
        memory = state["memory_mb"] + self.random.uniform(-6, 6)
        if fault == "cpu_saturation":
            cpu = self.random.uniform(0.92, 0.99)
        if fault == "memory_saturation":
            memory = self.random.uniform(470, 505)
        if fault == "abnormal_resource_growth":
            memory = min(memory_limit_mb * 1.1, state["memory_mb"] + 60)
        if fault in {"container_restart_storm"}:
            state["restarts"] += 3
        if fault in {"unhealthy_pod_state"}:
            state["restarts"] += 1
        cpu = max(0.0, cpu)
        memory = max(32.0, memory)
        state["cpu_cores"], state["memory_mb"] = cpu, memory

        ready = fault not in {"unhealthy_pod_state"}
        desired = 4
        available = 4
        if fault == "unavailable_replicas":
            available = 0
        if fault == "container_restart_storm":
            available = 2

        base = {
            "cluster_id": app["cluster_id"],
            "collector_id": collector_id,
            "app_id": app["app_id"],
            "namespace": app["namespace"],
            "pod": app["pod"],
            "timestamp": ts,
            "labels": app["labels"],
        }
        return [
            {**base, "metric_type": MetricType.POD_CPU.value, "metrics": {"usage_cores": round(cpu, 3), "limit_cores": cpu_limit, "ratio": round(cpu / cpu_limit, 3)}},
            {**base, "metric_type": MetricType.POD_MEMORY.value, "metrics": {"usage_mb": round(memory, 1), "limit_mb": memory_limit_mb, "ratio": round(memory / memory_limit_mb, 3)}},
            {**base, "metric_type": MetricType.CONTAINER_RESTARTS.value, "metrics": {"restarts": int(state["restarts"]), "window_seconds": 300}},
            {**base, "metric_type": MetricType.DEPLOYMENT_REPLICAS.value, "metrics": {"desired": desired, "available": available, "unavailable": desired - available}},
            {**base, "metric_type": MetricType.POD_STATUS.value, "metrics": {"phase": "Running", "ready": ready, "container_ready": ready, "waiting_reason": None if ready else "CrashLoopBackOff"}},
            {**base, "metric_type": MetricType.RESOURCE_LIMITS.value, "metrics": {"cpu_limit_cores": cpu_limit, "memory_limit_mb": memory_limit_mb, "has_limits": True}},
        ]

    def _node_events(self, node: dict[str, Any], collector_id: str) -> list[dict[str, Any]]:
        fault = self.faults.get(node["app_id"])
        cpu_ratio = self.random.uniform(0.3, 0.6)
        memory_ratio = self.random.uniform(0.4, 0.65)
        if fault == "node_resource_exhaustion":
            cpu_ratio = self.random.uniform(0.93, 0.99)
            memory_ratio = self.random.uniform(0.9, 0.97)
        base = {
            "cluster_id": node["cluster_id"],
            "collector_id": collector_id,
            "app_id": node["app_id"],
            "namespace": node["namespace"],
            "pod": node["pod"],
            "timestamp": utcnow_iso(),
            "labels": node["labels"],
        }
        return [
            {**base, "metric_type": MetricType.NODE_CPU.value, "metrics": {"ratio": round(cpu_ratio, 3), "cores_used": round(cpu_ratio * 8, 2)}},
            {**base, "metric_type": MetricType.NODE_MEMORY.value, "metrics": {"ratio": round(memory_ratio, 3), "used_mb": round(memory_ratio * 16384, 1)}},
            {**base, "metric_type": MetricType.NODE_CONDITION.value, "metrics": {"ready": fault != "node_resource_exhaustion", "memory_pressure": memory_ratio > 0.9, "disk_pressure": False}},
        ]

    def collect(self, collector_id: str) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for app in self.apps:
            events.extend(self._workload_events(app, collector_id))
        for node in self.nodes:
            events.extend(self._node_events(node, collector_id))
        return events

    def discovery_snapshot(self) -> list[dict[str, Any]]:
        return [
            {
                "app_id": item["app_id"],
                "cluster_id": item["cluster_id"],
                "namespace": item["namespace"],
                "labels": item["labels"],
            }
            for item in self.apps + self.nodes
        ]
