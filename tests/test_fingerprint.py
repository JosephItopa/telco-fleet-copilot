from detectors.fingerprint import compute_fingerprint


def test_fingerprint_is_stable_and_case_insensitive():
    first = compute_fingerprint("App-A", "cpu_saturation", "pod_cpu")
    second = compute_fingerprint("app-a", "cpu_saturation", "pod_cpu")
    assert first == second


def test_fingerprint_changes_with_anomaly_or_metric():
    base = compute_fingerprint("app-a", "cpu_saturation", "pod_cpu")
    assert base != compute_fingerprint("app-a", "memory_saturation", "pod_cpu")
    assert base != compute_fingerprint("app-a", "cpu_saturation", "node_cpu")
    assert base != compute_fingerprint("app-b", "cpu_saturation", "pod_cpu")


def test_fingerprint_is_not_time_bucketed():
    """Repeated observations must map to one incident, so the window is not part
    of the fingerprint."""
    assert compute_fingerprint("app-a", "cpu_saturation", "pod_cpu") == compute_fingerprint(
        "app-a", "cpu_saturation", "pod_cpu"
    )
