from database.repository import (
    attach_ai,
    list_incidents,
    record_observations,
    resolve_stale,
    summary,
    touch_applications,
)
from database.session import init_db


def observation(fingerprint: str, app_id: str = "app-db", severity: str = "high") -> dict:
    return {
        "fingerprint": fingerprint,
        "app_id": app_id,
        "cluster_id": "cluster-a",
        "namespace": "ns-a",
        "detector_id": "detector-1",
        "kafka_partition": 3,
        "anomaly_type": "cpu_saturation",
        "metric": "pod_cpu",
        "severity": severity,
        "evidence": [{"metric_type": "pod_cpu", "metrics": {"ratio": 0.95}}],
    }


def test_repeated_observations_update_one_incident():
    init_db()
    touch_applications([{"app_id": "app-db", "cluster_id": "cluster-a", "namespace": "ns-a", "labels": {}}])

    first = record_observations([observation("fp-dedup-1")])
    assert first[0]["transition"] == "NEW"

    second = record_observations([observation("fp-dedup-1")])
    assert second[0]["transition"] == "ONGOING"
    assert second[0]["incident_id"] == first[0]["incident_id"]

    items = list_incidents(app_id="app-db", limit=100)
    matching = [item for item in items if item["fingerprint"] == "fp-dedup-1"]
    assert len(matching) == 1
    assert matching[0]["observation_count"] == 2
    assert matching[0]["status"] == "ONGOING"


def test_resolved_incident_does_not_block_a_new_occurrence():
    init_db()
    first = record_observations([observation("fp-lifecycle-1")])
    assert resolve_stale(older_than_seconds=0) >= 1

    resolved = [item for item in list_incidents(status="RESOLVED", limit=200) if item["fingerprint"] == "fp-lifecycle-1"]
    assert resolved

    again = record_observations([observation("fp-lifecycle-1")])
    assert again[0]["transition"] == "NEW"
    assert again[0]["incident_id"] != first[0]["incident_id"]


def test_severity_escalates_on_update():
    init_db()
    record_observations([observation("fp-sev-1", severity="medium")])
    record_observations([observation("fp-sev-1", severity="critical")])
    items = [item for item in list_incidents(app_id="app-db", limit=200) if item["fingerprint"] == "fp-sev-1"]
    assert items[0]["severity"] == "critical"


def test_attach_ai_and_summary():
    init_db()
    result = record_observations([observation("fp-ai-1", app_id="app-ai")])
    incident_id = result[0]["incident_id"]
    updated = attach_ai(incident_id, {"status": "completed", "model": "glm-5-3"}, "restart workload")
    assert updated is not None
    assert updated["ai"]["model"] == "glm-5-3"
    assert updated["remediation"] == "restart workload"

    totals = summary()
    assert totals["applications_monitored"] >= 1
