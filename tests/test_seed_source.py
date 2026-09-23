from detectors.rules import RuleEngine
from models.schema import MetricType
from seed_k8s.dataset import SeedSource

REQUIRED_FIELDS = ("cluster_id", "collector_id", "app_id", "timestamp", "metric_type", "metrics", "labels")


def test_seed_events_match_the_normalized_schema():
    source = SeedSource(apps=20, unhealthy_ratio=0.25, seed=1)
    events = source.collect("collector-test")

    assert events
    for event in events:
        for field in REQUIRED_FIELDS:
            assert field in event, f"missing {field}"
        assert isinstance(event["metrics"], dict)
        assert isinstance(event["labels"], dict)
        assert event["labels"].get("source") == "seed-k8s"

    metric_types = {event["metric_type"] for event in events}
    assert {
        MetricType.POD_CPU.value,
        MetricType.POD_MEMORY.value,
        MetricType.CONTAINER_RESTARTS.value,
        MetricType.POD_STATUS.value,
        MetricType.DEPLOYMENT_REPLICAS.value,
        MetricType.NODE_CPU.value,
        MetricType.NODE_MEMORY.value,
        MetricType.NODE_CONDITION.value,
    } <= metric_types


def test_pod_metrics_carry_a_ratio_for_the_rules_engine():
    source = SeedSource(apps=10, seed=2)
    events = source.collect("collector-test")
    cpu = [event for event in events if event["metric_type"] == MetricType.POD_CPU.value]
    memory = [event for event in events if event["metric_type"] == MetricType.POD_MEMORY.value]
    assert cpu and all("ratio" in event["metrics"] for event in cpu)
    assert memory and all("ratio" in event["metrics"] for event in memory)


def test_unhealthy_seed_apps_produce_detections():
    source = SeedSource(apps=30, unhealthy_ratio=0.3, seed=3)
    assert source.faults, "seed should mark some workloads unhealthy"

    events = source.collect("collector-test")
    engine = RuleEngine()
    detections = 0
    for app_id in source.apps:
        app_events = [event for event in events if event["app_id"] == app_id]
        detections += len(engine.evaluate_app(app_id, app_events))
    assert detections > 0


def test_seed_is_deterministic_for_a_given_seed():
    first = SeedSource(apps=15, unhealthy_ratio=0.2, seed=11)
    second = SeedSource(apps=15, unhealthy_ratio=0.2, seed=11)
    assert first.faults == second.faults


def test_discovery_snapshot_lists_workloads_and_nodes():
    source = SeedSource(apps=5)
    snapshot = source.discovery_snapshot()
    app_ids = {item["app_id"] for item in snapshot}
    assert "demo-app-001" in app_ids
    assert any(item["app_id"].startswith("node-") for item in snapshot)
    assert all("labels" in item and "cluster_id" in item for item in snapshot)
