"""Seed Kubernetes dataset.

Stand-in for a live cluster: a demo cluster of ``demo-app-XXX`` workloads, a
small percentage of which are unhealthy. It emits the platform's normalized
``MetricEvent`` schema, so the collector can use it as the fallback source when
no Kubernetes cluster is reachable.
"""

from __future__ import annotations

import random
from typing import Any

from models.schema import MetricType, utcnow_iso

DEFAULT_CLUSTER = "demo-cluster"
DEFAULT_NAMESPACE = "demo"
DEFAULT_APPS = 50
DEFAULT_UNHEALTHY_RATIO = 0.08

FAULTS = [
    "cpu_saturation",
    "memory_saturation",
    "container_restart_storm",
    "unavailable_replicas",
    "abnormal_resource_growth",
    "unhealthy_pod_state",
    "node_resource_exhaustion",
]

CPU_LIMIT = 1.0
MEMORY_LIMIT_MB = 512.0


class SeedSource:
    """Deterministic Kubernetes-workload fallback source."""

    def __init__(
        self,
        apps: int = DEFAULT_APPS,
        cluster_id: str = DEFAULT_CLUSTER,
        namespace: str = DEFAULT_NAMESPACE,
        unhealthy_ratio: float = DEFAULT_UNHEALTHY_RATIO,
        seed: int = 1337,
    ) -> None:
        self.random = random.Random(seed)
        self.cluster_id = cluster_id
        self.namespace = namespace
        self.apps = [f"demo-app-{index:03d}" for index in range(1, apps + 1)]
        self.nodes = [f"demo-node-{index}" for index in range(1, 4)]
        self.faults: dict[str, str] = {}
        for app in self.apps:
            if self.random.random() < unhealthy_ratio:
                self.faults[app] = self.random.choice(FAULTS)
        for node in self.nodes:
            if self.random.random() < 0.15:
                self.faults[f"{cluster_id}/{node}"] = "node_resource_exhaustion"
        self._state: dict[str, dict[str, float]] = {}

    def _app_state(self, app_id: str) -> dict[str, float]:
        return self._state.setdefault(app_id, {"restarts": 0.0, "memory_mb": 128.0, "cpu_cores": 0.25})

    def _base(self, app_id: str, collector_id: str, pod: str | None) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "collector_id": collector_id,
            "app_id": app_id,
            "namespace": self.namespace,
            "pod": pod,
            "timestamp": utcnow_iso(),
            "labels": {"app.kubernetes.io/name": app_id, "kind": "workload", "source": "seed-k8s"},
        }

    def _workload_events(self, app_id: str, collector_id: str) -> list[dict[str, Any]]:
        state = self._app_state(app_id)
        fault = self.faults.get(app_id)
        pod = f"{app_id}-pod"

        cpu = max(0.0, state["cpu_cores"] + self.random.uniform(-0.05, 0.05))
        memory = max(32.0, state["memory_mb"] + self.random.uniform(-6, 6))
        if fault == "cpu_saturation":
            cpu = self.random.uniform(0.88, 0.99)
        if fault == "memory_saturation":
            memory = self.random.uniform(0.86, 0.98) * MEMORY_LIMIT_MB
        if fault == "abnormal_resource_growth":
            memory = min(MEMORY_LIMIT_MB * 1.1, state["memory_mb"] + 60)
        if fault == "container_restart_storm":
            state["restarts"] += 3
        elif fault == "unhealthy_pod_state":
            state["restarts"] += 1
        state["cpu_cores"], state["memory_mb"] = cpu, memory

        ready = fault != "unhealthy_pod_state"
        desired = 4
        available = 4
        if fault == "unavailable_replicas":
            available = 0
        elif fault == "container_restart_storm":
            available = 2

        base = self._base(app_id, collector_id, pod)
        return [
            {
                **base,
                "metric_type": MetricType.POD_CPU.value,
                "metrics": {"usage_cores": round(cpu, 3), "limit_cores": CPU_LIMIT, "ratio": round(cpu / CPU_LIMIT, 3)},
            },
            {
                **base,
                "metric_type": MetricType.POD_MEMORY.value,
                "metrics": {
                    "usage_mb": round(memory, 1),
                    "limit_mb": MEMORY_LIMIT_MB,
                    "ratio": round(memory / MEMORY_LIMIT_MB, 3),
                },
            },
            {
                **base,
                "metric_type": MetricType.CONTAINER_RESTARTS.value,
                "metrics": {"restarts": int(state["restarts"]), "window_seconds": 300},
            },
            {
                **base,
                "metric_type": MetricType.DEPLOYMENT_REPLICAS.value,
                "metrics": {"desired": desired, "available": available, "unavailable": desired - available},
            },
            {
                **base,
                "metric_type": MetricType.POD_STATUS.value,
                "metrics": {
                    "phase": "Running",
                    "ready": ready,
                    "container_ready": ready,
                    "waiting_reason": None if ready else "CrashLoopBackOff",
                },
            },
            {
                **base,
                "metric_type": MetricType.RESOURCE_LIMITS.value,
                "metrics": {"cpu_limit_cores": CPU_LIMIT, "memory_limit_mb": MEMORY_LIMIT_MB, "has_limits": True},
            },
        ]

    def _node_events(self, node: str, collector_id: str) -> list[dict[str, Any]]:
        fault = self.faults.get(f"{self.cluster_id}/{node}")
        cpu_ratio = self.random.uniform(0.3, 0.6)
        memory_ratio = self.random.uniform(0.4, 0.65)
        if fault == "node_resource_exhaustion":
            cpu_ratio = self.random.uniform(0.93, 0.99)
            memory_ratio = self.random.uniform(0.9, 0.97)
        base = {
            "cluster_id": self.cluster_id,
            "collector_id": collector_id,
            "app_id": f"node-{self.cluster_id}-{node}",
            "namespace": None,
            "pod": None,
            "timestamp": utcnow_iso(),
            "labels": {"kind": "node", "node": node, "source": "seed-k8s"},
        }
        return [
            {**base, "metric_type": MetricType.NODE_CPU.value, "metrics": {"ratio": round(cpu_ratio, 3), "cores_used": round(cpu_ratio * 8, 2)}},
            {**base, "metric_type": MetricType.NODE_MEMORY.value, "metrics": {"ratio": round(memory_ratio, 3), "used_mb": round(memory_ratio * 16384, 1)}},
            {
                **base,
                "metric_type": MetricType.NODE_CONDITION.value,
                "metrics": {
                    "ready": fault != "node_resource_exhaustion",
                    "memory_pressure": memory_ratio > 0.9,
                    "disk_pressure": False,
                },
            },
        ]

    def collect(self, collector_id: str) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for app_id in self.apps:
            events.extend(self._workload_events(app_id, collector_id))
        for node in self.nodes:
            events.extend(self._node_events(node, collector_id))
        return events

    def discovery_snapshot(self) -> list[dict[str, Any]]:
        items = [
            {
                "app_id": app_id,
                "cluster_id": self.cluster_id,
                "namespace": self.namespace,
                "labels": {"app.kubernetes.io/name": app_id, "kind": "workload", "source": "seed-k8s"},
            }
            for app_id in self.apps
        ]
        items.extend(
            {
                "app_id": f"node-{self.cluster_id}-{node}",
                "cluster_id": self.cluster_id,
                "namespace": None,
                "labels": {"kind": "node", "node": node, "source": "seed-k8s"},
            }
            for node in self.nodes
        )
        return items
