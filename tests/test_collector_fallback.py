import dataclasses

from collectors import main as collector_main


def _settings(**overrides):
    return dataclasses.replace(collector_main.settings, **overrides)


def test_seed_mode_uses_seed_source(monkeypatch):
    monkeypatch.setattr(collector_main, "settings", _settings(collector_mode="seed"))

    def k8s_must_not_be_built():
        raise AssertionError("the k8s source must not be built in seed mode")

    source, mode = collector_main.build_source(k8s_factory=k8s_must_not_be_built)
    assert mode == "seed"
    events = source.collect("collector-test")
    assert events and events[0]["labels"]["source"] == "seed-k8s"


def test_k8s_mode_falls_back_when_cluster_unreachable(monkeypatch):
    monkeypatch.setattr(collector_main, "settings", _settings(collector_mode="k8s"))

    def failing_k8s():
        raise RuntimeError("connection refused")

    source, mode = collector_main.build_source(k8s_factory=failing_k8s)
    assert mode == "seed"
    assert collector_main.state.fallback_reason == "connection refused"
    assert source.collect("collector-test")


def test_k8s_mode_uses_cluster_when_reachable(monkeypatch):
    monkeypatch.setattr(collector_main, "settings", _settings(collector_mode="k8s"))

    class FakeK8s:
        def verify(self):
            return None

        def collect(self, collector_id):
            return [{"app_id": "from-cluster"}]

    source, mode = collector_main.build_source(k8s_factory=lambda: FakeK8s())
    assert mode == "k8s"
    assert source.collect("collector-test") == [{"app_id": "from-cluster"}]


def test_mid_run_k8s_failure_switches_to_seed(monkeypatch):
    monkeypatch.setattr(collector_main, "settings", _settings(collector_mode="k8s"))

    class BrokenK8s:
        def verify(self):
            return None

        def collect(self, collector_id):
            raise RuntimeError("apiserver gone")

    collector_main.source, collector_main.state.source_mode = collector_main.build_source(
        k8s_factory=lambda: BrokenK8s()
    )
    assert collector_main.state.source_mode == "k8s"

    collector_main.use_seed_fallback("apiserver gone")
    assert collector_main.state.source_mode == "seed"
    assert collector_main.state.fallback_reason == "apiserver gone"
    assert collector_main.source.collect("collector-test")


def test_k8s_source_probe_raises_without_a_cluster():
    """The real Kubernetes source must fail its probe rather than pretend to work."""
    from collectors.k8s_source import KubernetesSource

    source = KubernetesSource(contexts=["definitely-not-a-cluster"], collector_id="test", kubeconfig="/nonexistent")
    try:
        source.verify()
    except Exception:
        return
    raise AssertionError("verify() should fail when no cluster is reachable")
