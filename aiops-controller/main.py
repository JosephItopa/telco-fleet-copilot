import os
import time
import uuid
from datetime import datetime, timezone
import requests
from fastapi import FastAPI
from prometheus_client import Gauge, Counter, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

app = FastAPI(title="AIOps Controller")

API_URL = os.getenv("API_SERVICE_URL", "http://localhost:8000")
PAYMENT_URL = os.getenv("PAYMENT_SERVICE_URL", "http://localhost:8001")

incidents = []
DETECTION_COUNT = Counter("aiops_incidents_detected_total", "Detected incidents", ["service", "type"])
LAST_ANOMALY = Gauge("aiops_last_anomaly", "Whether the latest analysis found an anomaly")


def get_metrics():
    """Read Prometheus text from each service and extract the few MVP metrics we need."""
    result = {}
    for service, url in [("api-service", API_URL), ("payment-service", PAYMENT_URL)]:
        try:
            text = requests.get(f"{url}/metrics", timeout=3).text
            result[service] = parse_prometheus(text)
        except requests.RequestException as exc:
            result[service] = {"_collection_error": str(exc)}
    return result


def parse_prometheus(text: str):
    values = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        name_value = line.split(" ", 1)
        if len(name_value) != 2:
            continue
        name, raw = name_value
        try:
            values[name] = float(raw)
        except ValueError:
            continue
    return values


def detect(metrics):
    p = metrics.get("payment-service", {})
    a = metrics.get("api-service", {})
    findings = []

    # Failure switch is an explicit ground-truth signal for this prototype.
    try:
        switch = p.get("payment_failure_switch", 0)
        switch = int(switch)
    except (TypeError, ValueError):
        switch = 0

    if switch == 1:
        findings.append(("payment-service", "latency_spike", "Injected high latency"))
    elif switch == 2:
        findings.append(("payment-service", "error_spike", "Injected error spike"))
    elif switch == 3:
        findings.append(("payment-service", "service_down", "Payment health is down"))
    elif switch == 4:
        findings.append(("payment-service", "high_cpu", "Injected CPU pressure"))
    elif switch == 5:
        findings.append(("payment-service", "high_memory", "Injected memory pressure"))

    # Basic observed telemetry checks.
    if p.get("payment_failures_total", 0) > 0 and switch == 0:
        findings.append(("payment-service", "payment_failures", "Payment failures observed"))

    if a.get("api_dependency_failures_total", 0) > 0 and switch == 0:
        findings.append(("api-service", "dependency_failure", "API observed payment dependency failures"))

    return findings


def create_incident(service, anomaly_type, reason, metrics):
    incident = {
        "incident_id": f"INC-{uuid.uuid4().hex[:8].upper()}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "cluster_id": "prototype-cluster-01",
        "namespace": "aiops-demo",
        "severity": "HIGH",
        "anomaly": anomaly_type,
        "confidence": 0.95,
        "reason": reason,
        "evidence": {
            "payment": metrics.get("payment-service", {}),
            "api": metrics.get("api-service", {}),
        },
        "status": "DETECTED",
    }
    incidents.insert(0, incident)
    DETECTION_COUNT.labels(service, anomaly_type).inc()
    return incident


@app.get("/")
def root():
    return {"service": "aiops-controller", "status": "running"}


@app.get("/telemetry")
def telemetry():
    return get_metrics()


@app.post("/analyze")
def analyze():
    metrics = get_metrics()
    findings = detect(metrics)
    LAST_ANOMALY.set(1 if findings else 0)

    created = []
    # Avoid generating duplicate incidents on every polling call for the same active switch.
    active_keys = {(x["service"], x["anomaly"]) for x in incidents[:20] if x["status"] == "DETECTED"}
    for service, anomaly_type, reason in findings:
        if (service, anomaly_type) not in active_keys:
            created.append(create_incident(service, anomaly_type, reason, metrics))

    return {
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
        "findings": len(findings),
        "new_incidents": created,
        "telemetry": metrics,
    }


@app.get("/incidents")
def list_incidents():
    return {"count": len(incidents), "incidents": incidents}


@app.get("/incident/{incident_id}")
def get_incident(incident_id: str):
    for incident in incidents:
        if incident["incident_id"] == incident_id:
            return incident
    return {"error": "Incident not found", "incident_id": incident_id}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
