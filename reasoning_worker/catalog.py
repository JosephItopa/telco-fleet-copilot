"""Deterministic recommendation catalog.

Used as a grounded fallback when the LLM is unavailable, and to enrich findings
that are below the reasoning severity threshold. The LLM only explains this
plan; it never overrides it.
"""

from __future__ import annotations

from common.models import AIAnalysis, Finding, utcnow

CATALOG: dict[str, dict[str, object]] = {
    "service_down": {
        "summary": "The workload is not serving traffic and is reporting unhealthy.",
        "root_cause": "The app health probe reports 0, indicating the process is down or failing readiness.",
        "remediation_steps": [
            "Confirm the affected workload and pod readiness.",
            "Inspect recent restarts, crash loops, and container exit codes.",
            "Restart the workload only after the failure cause is confirmed.",
            "Verify health recovers and traffic resumes.",
        ],
        "risk": "Restarting without confirming the cause can repeat the outage.",
    },
    "error_spike": {
        "summary": "A large share of requests are failing with 5xx responses.",
        "root_cause": "Upstream errors or an unhealthy downstream dependency are surfacing as 5xx responses.",
        "remediation_steps": [
            "Inspect recent logs and traces for the failing path.",
            "Check downstream dependencies for correlated failures.",
            "Apply traffic protection (retry budget, circuit breaker) if the dependency is degraded.",
            "Roll back the most recent deployment if errors started with a release.",
        ],
        "risk": "Blind retries can amplify load on an already failing dependency.",
    },
    "latency_spike": {
        "summary": "Request latency is well above the expected threshold.",
        "root_cause": "Slow downstream calls, resource saturation, or lock contention are inflating latency.",
        "remediation_steps": [
            "Identify the slowest downstream call in the trace.",
            "Check CPU and memory saturation for the workload and its nodes.",
            "Apply timeouts and bulkheads to contain the slow path.",
            "Scale the workload only after the bottleneck is identified.",
        ],
        "risk": "Scaling can hide the symptom without fixing the root cause.",
    },
    "dependency_failure": {
        "summary": "A downstream dependency is failing from the caller's perspective.",
        "root_cause": "The upstream service observed repeated failures calling its dependency.",
        "remediation_steps": [
            "Confirm the dependency's own health and error rate.",
            "Protect the caller with a circuit breaker and bounded retries.",
            "Escalate to the dependency owner if it is unhealthy.",
        ],
        "risk": "Without protection, dependency failures cascade to callers.",
    },
    "payment_failures": {
        "summary": "Application failures are recorded without a matching 5xx ratio.",
        "root_cause": "Business-level failures or partial error handling are being recorded as failures.",
        "remediation_steps": [
            "Distinguish business-rule rejections from technical errors.",
            "Correlate failures with deploys and configuration changes.",
            "Add targeted error handling for the dominant failure mode.",
        ],
        "risk": "Treating expected business failures as incidents causes alert fatigue.",
    },
    "cluster_unhealthy": {
        "summary": "The cluster health probe reports the cluster as unhealthy.",
        "root_cause": "Control plane or a critical node component is failing.",
        "remediation_steps": [
            "Check control-plane component status and recent node events.",
            "Verify node readiness and taints across the cluster.",
            "Escalate to the platform team before scheduling changes.",
        ],
        "risk": "Acting on an unhealthy control plane can worsen cluster-wide impact.",
    },
    "node_down": {
        "summary": "One or more cluster nodes are not ready.",
        "root_cause": "Node failure, kubelet issues, or resource exhaustion removed the node from service.",
        "remediation_steps": [
            "Inspect node events and kubelet status.",
            "Drain the node if it is repeatedly failing.",
            "Confirm pod rescheduling onto healthy nodes.",
            "Replace the node if it does not recover.",
        ],
        "risk": "Unschedulable nodes reduce capacity and degrade redundancy.",
    },
    "high_cpu": {
        "summary": "Cluster CPU usage is near saturation.",
        "root_cause": "High request load, noisy neighbours, or inefficient workloads are consuming CPU.",
        "remediation_steps": [
            "Identify the top CPU consumers by namespace and workload.",
            "Apply resource requests and limits where missing.",
            "Scale out or move workloads off saturated nodes.",
        ],
        "risk": "Saturation increases latency and can trigger cascading failures.",
    },
    "high_memory": {
        "summary": "Cluster memory usage is near saturation.",
        "root_cause": "Memory growth, leaks, or over-commitment are consuming node memory.",
        "remediation_steps": [
            "Identify the top memory consumers.",
            "Look for leaks and unbounded caches.",
            "Evict or reschedule workloads before the OOM killer acts.",
        ],
        "risk": "Memory pressure triggers OOMKills and pod evictions.",
    },
    "scheduling_pressure": {
        "summary": "A large number of pods are pending scheduling.",
        "root_cause": "Insufficient capacity or restrictive scheduling constraints.",
        "remediation_steps": [
            "Inspect pending pods for unschedulable reasons.",
            "Check node capacity, taints, and affinity rules.",
            "Add capacity or relax constraints that block scheduling.",
        ],
        "risk": "Pending pods delay recovery and reduce availability.",
    },
    "workloads_unavailable": {
        "summary": "Workloads have replicas that are currently unavailable.",
        "root_cause": "Crash loops, failed readiness probes, or scheduling failures.",
        "remediation_steps": [
            "Identify the unavailable workloads and their replica counts.",
            "Inspect the reason replicas are not becoming ready.",
            "Restore capacity on the hosting cluster.",
        ],
        "risk": "Reduced replicas lower redundancy and can break SLOs.",
    },
}

DEFAULT_ENTRY = {
    "summary": "An anomaly was detected on this target.",
    "root_cause": "Collect additional telemetry to establish the root cause.",
    "remediation_steps": [
        "Review the attached evidence.",
        "Correlate with recent deployments and configuration changes.",
        "Escalate to the owning team if the anomaly persists.",
    ],
    "risk": "Acting without sufficient evidence may cause unnecessary change.",
}


def deterministic(finding: Finding) -> AIAnalysis:
    entry = CATALOG.get(finding.anomaly, DEFAULT_ENTRY)
    return AIAnalysis(
        status="completed",
        provider="deterministic-fallback",
        model="rules-v1",
        summary=str(entry["summary"]),
        root_cause=str(entry["root_cause"]),
        remediation_steps=[str(step) for step in entry["remediation_steps"]],  # type: ignore[arg-type]
        risk=str(entry["risk"]),
        confidence=0.5,
        latency_seconds=0.0,
        generated_at=utcnow(),
    )
