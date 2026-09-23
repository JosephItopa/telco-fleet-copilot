from detectors.rules import RuleEngine
from models.schema import AnomalyType, MetricType


def event(metric_type: str, metrics: dict, app_id: str = "app-a") -> dict:
    return {
        "cluster_id": "cluster-a",
        "collector_id": "collector-1",
        "app_id": app_id,
        "namespace": "ns-a",
        "pod": "app-a-0",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "metric_type": metric_type,
        "metrics": metrics,
        "labels": {},
    }


def detect(engine: RuleEngine, events: list[dict]) -> set[str]:
    return {detection.anomaly_type for detection in engine.evaluate_app("app-a", events)}


def test_healthy_application_produces_no_detections():
    engine = RuleEngine()
    events = [
        event(MetricType.POD_CPU.value, {"ratio": 0.2}),
        event(MetricType.POD_MEMORY.value, {"ratio": 0.3, "usage_mb": 128}),
        event(MetricType.CONTAINER_RESTARTS.value, {"restarts": 0}),
        event(MetricType.DEPLOYMENT_REPLICAS.value, {"desired": 3, "available": 3, "unavailable": 0}),
        event(MetricType.POD_STATUS.value, {"ready": True, "waiting_reason": None}),
    ]
    assert detect(engine, events) == set()


def test_cpu_and_memory_saturation():
    engine = RuleEngine()
    findings = detect(engine, [event(MetricType.POD_CPU.value, {"ratio": 0.97})])
    assert AnomalyType.CPU_SATURATION.value in findings

    engine = RuleEngine()
    findings = detect(engine, [event(MetricType.POD_MEMORY.value, {"ratio": 0.95, "usage_mb": 490})])
    assert AnomalyType.MEMORY_SATURATION.value in findings


def test_container_restart_storm():
    engine = RuleEngine()
    findings = detect(engine, [event(MetricType.CONTAINER_RESTARTS.value, {"restarts": 7})])
    assert AnomalyType.CONTAINER_RESTART_STORM.value in findings


def test_unavailable_replicas_is_critical():
    engine = RuleEngine()
    detections = engine.evaluate_app(
        "app-a", [event(MetricType.DEPLOYMENT_REPLICAS.value, {"desired": 4, "available": 0, "unavailable": 4})]
    )
    assert [d.anomaly_type for d in detections] == [AnomalyType.UNAVAILABLE_REPLICAS.value]
    assert detections[0].severity == "critical"


def test_deployment_degradation_is_medium():
    engine = RuleEngine()
    detections = engine.evaluate_app(
        "app-a", [event(MetricType.DEPLOYMENT_REPLICAS.value, {"desired": 4, "available": 2, "unavailable": 2})]
    )
    assert detections[0].anomaly_type == AnomalyType.DEPLOYMENT_DEGRADATION.value
    assert detections[0].severity == "medium"


def test_unhealthy_pod_state():
    engine = RuleEngine()
    findings = detect(
        engine,
        [event(MetricType.POD_STATUS.value, {"ready": False, "waiting_reason": "CrashLoopBackOff"})],
    )
    assert AnomalyType.UNHEALTHY_POD_STATE.value in findings


def test_node_resource_exhaustion():
    engine = RuleEngine()
    findings = detect(engine, [event(MetricType.NODE_CPU.value, {"ratio": 0.95})])
    assert AnomalyType.NODE_RESOURCE_EXHAUSTION.value in findings

    engine = RuleEngine()
    findings = detect(engine, [event(MetricType.NODE_CONDITION.value, {"ready": False, "memory_pressure": True})])
    assert AnomalyType.NODE_RESOURCE_EXHAUSTION.value in findings


def test_abnormal_resource_growth_across_observations():
    engine = RuleEngine()
    detect(engine, [event(MetricType.POD_MEMORY.value, {"ratio": 0.2, "usage_mb": 100})])
    findings = detect(engine, [event(MetricType.POD_MEMORY.value, {"ratio": 0.4, "usage_mb": 160})])
    assert AnomalyType.ABNORMAL_RESOURCE_GROWTH.value in findings
