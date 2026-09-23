"""Kubernetes-native collection using the Kubernetes API directly.

Discovers workloads automatically from pods, deployments and nodes, and reads
usage from metrics.k8s.io when metrics-server is available. Multiple clusters are
supported through kubeconfig contexts (K8S_CONTEXTS) or in-cluster config.
"""

from __future__ import annotations

from typing import Any

from config.logging_utils import get_logger
from models.schema import MetricType, utcnow_iso

logger = get_logger("collectors.k8s")


class KubernetesSource:
    def __init__(self, contexts: list[str], collector_id: str, kubeconfig: str = "") -> None:
        self.contexts = contexts or [""]
        self.collector_id = collector_id
        self.kubeconfig = kubeconfig
        self._clients: dict[str, Any] = {}

    def _clients_for(self, context: str) -> tuple[Any, Any, Any]:
        if context in self._clients:
            return self._clients[context]

        from kubernetes import client, config

        if self.kubeconfig:
            config.load_kube_config(config_file=self.kubeconfig, context=context or None)
        else:
            try:
                config.load_incluster_config()
            except Exception:  # noqa: BLE001 - fall back to local kubeconfig
                config.load_kube_config(context=context or None)

        bundle = (client.CoreV1Api(), client.AppsV1Api(), client.CustomObjectsApi())
        self._clients[context] = bundle
        return bundle

    def _cluster_id(self, context: str) -> str:
        return context or "in-cluster"

    def collect(self, context: str = "") -> list[dict[str, Any]]:
        core, apps, custom = self._clients_for(context)
        cluster_id = self._cluster_id(context)
        events: list[dict[str, Any]] = []

        pod_metrics = self._pod_usage(custom)
        for pod in core.list_pod_for_all_namespaces().items:
            labels = pod.metadata.labels or {}
            app_id = labels.get("app") or labels.get("app.kubernetes.io/name") or pod.metadata.name
            namespace = pod.metadata.namespace
            usage = pod_metrics.get(f"{namespace}/{pod.metadata.name}", {})
            restarts = sum(status.restart_count or 0 for status in (pod.status.container_statuses or []))
            ready = all(bool(status.ready) for status in (pod.status.container_statuses or [])) and bool(
                pod.status.container_statuses
            )
            waiting = [
                status.state.waiting.reason
                for status in (pod.status.container_statuses or [])
                if status.state and status.state.waiting
            ]
            base = {
                "cluster_id": cluster_id,
                "collector_id": self.collector_id,
                "app_id": app_id,
                "namespace": namespace,
                "pod": pod.metadata.name,
                "timestamp": utcnow_iso(),
                "labels": {**labels, "kind": "workload"},
            }
            events.append({**base, "metric_type": MetricType.POD_CPU.value, "metrics": {"usage_cores": usage.get("cpu_cores")}})
            events.append({**base, "metric_type": MetricType.POD_MEMORY.value, "metrics": {"usage_mb": usage.get("memory_mb")}})
            events.append(
                {
                    **base,
                    "metric_type": MetricType.CONTAINER_RESTARTS.value,
                    "metrics": {"restarts": restarts, "window_seconds": 300},
                }
            )
            events.append(
                {
                    **base,
                    "metric_type": MetricType.POD_STATUS.value,
                    "metrics": {
                        "phase": pod.status.phase,
                        "ready": ready,
                        "container_ready": ready,
                        "waiting_reason": waiting[0] if waiting else None,
                    },
                }
            )

        for deployment in apps.list_deployment_for_all_namespaces().items:
            labels = deployment.metadata.labels or {}
            app_id = labels.get("app") or labels.get("app.kubernetes.io/name") or deployment.metadata.name
            desired = deployment.spec.replicas or 0
            available = deployment.status.available_replicas or 0
            events.append(
                {
                    "cluster_id": cluster_id,
                    "collector_id": self.collector_id,
                    "app_id": app_id,
                    "namespace": deployment.metadata.namespace,
                    "pod": None,
                    "timestamp": utcnow_iso(),
                    "metric_type": MetricType.DEPLOYMENT_REPLICAS.value,
                    "metrics": {"desired": desired, "available": available, "unavailable": max(0, desired - available)},
                    "labels": {**labels, "kind": "workload"},
                }
            )

        for node in core.list_node().items:
            labels = node.metadata.labels or {}
            conditions = {condition.type: condition.status for condition in (node.status.conditions or [])}
            base = {
                "cluster_id": cluster_id,
                "collector_id": self.collector_id,
                "app_id": f"node-{node.metadata.name}",
                "namespace": None,
                "pod": None,
                "timestamp": utcnow_iso(),
                "labels": {**labels, "kind": "node", "node": node.metadata.name},
            }
            events.append(
                {
                    **base,
                    "metric_type": MetricType.NODE_CONDITION.value,
                    "metrics": {
                        "ready": conditions.get("Ready") == "True",
                        "memory_pressure": conditions.get("MemoryPressure") == "True",
                        "disk_pressure": conditions.get("DiskPressure") == "True",
                    },
                }
            )
            capacity = (node.status.capacity or {}).get("memory", "0Ki")
            allocatable = (node.status.allocatable or {}).get("memory", "0Ki")
            events.append(
                {
                    **base,
                    "metric_type": MetricType.NODE_MEMORY.value,
                    "metrics": {"capacity": capacity, "allocatable": allocatable},
                }
            )

        for event in core.list_event_for_all_namespaces().items:
            involved = event.involved_object
            events.append(
                {
                    "cluster_id": cluster_id,
                    "collector_id": self.collector_id,
                    "app_id": involved.name if involved else "unknown",
                    "namespace": event.metadata.namespace,
                    "pod": involved.name if involved and involved.kind == "Pod" else None,
                    "timestamp": utcnow_iso(),
                    "metric_type": MetricType.K8S_EVENT.value,
                    "metrics": {"reason": event.reason, "type": event.type, "count": event.count or 1},
                    "labels": {"kind": "event"},
                }
            )
        return events

    @staticmethod
    def _pod_usage(custom: Any) -> dict[str, dict[str, float]]:
        usage: dict[str, dict[str, float]] = {}
        try:
            metrics = custom.list_cluster_custom_object("metrics.k8s.io", "v1beta1", "pods")
        except Exception as exc:  # noqa: BLE001 - metrics-server may be absent
            logger.warning("pod metrics unavailable", extra={"error": str(exc)})
            return usage
        for item in metrics.get("items", []):
            key = f"{item['metadata']['namespace']}/{item['metadata']['name']}"
            cpu = _parse_cpu((item.get("containers") or [{}])[0].get("usage", {}).get("cpu", "0"))
            memory = _parse_memory((item.get("containers") or [{}])[0].get("usage", {}).get("memory", "0"))
            usage[key] = {"cpu_cores": cpu, "memory_mb": memory}
        return usage

    def discovery_snapshot(self) -> list[dict[str, Any]]:
        discovered: list[dict[str, Any]] = []
        for context in self.contexts:
            try:
                core, _, _ = self._clients_for(context)
                for pod in core.list_pod_for_all_namespaces().items:
                    labels = pod.metadata.labels or {}
                    app_id = labels.get("app") or labels.get("app.kubernetes.io/name") or pod.metadata.name
                    discovered.append(
                        {
                            "app_id": app_id,
                            "cluster_id": self._cluster_id(context),
                            "namespace": pod.metadata.namespace,
                            "labels": labels,
                        }
                    )
            except Exception as exc:  # noqa: BLE001
                logger.error("discovery failed", extra={"context": context, "error": str(exc)})
        unique: dict[str, dict[str, Any]] = {}
        for item in discovered:
            unique[f"{item['cluster_id']}/{item['app_id']}"] = item
        return list(unique.values())


def _parse_cpu(value: str) -> float:
    if value.endswith("n"):
        return float(value[:-1]) / 1_000_000_000
    if value.endswith("u"):
        return float(value[:-1]) / 1_000_000
    if value.endswith("m"):
        return float(value[:-1]) / 1000
    try:
        return float(value)
    except ValueError:
        return 0.0


def _parse_memory(value: str) -> float:
    factors = {"Ki": 1 / 1024, "Mi": 1.0, "Gi": 1024.0, "K": 1 / 1000, "M": 1.0, "G": 1000.0}
    for suffix, factor in factors.items():
        if value.endswith(suffix):
            try:
                return float(value[: -len(suffix)]) * factor
            except ValueError:
                return 0.0
    try:
        return float(value) / (1024 * 1024)
    except ValueError:
        return 0.0
