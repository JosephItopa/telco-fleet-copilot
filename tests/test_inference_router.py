import asyncio

from inference.router import ModelRouter


def messages() -> list[dict]:
    return [{"role": "user", "content": "analyse this incident"}]


def test_switches_model_only_after_three_failed_trials():
    attempts_seen: list[str] = []

    async def call(model: str, _messages: list[dict]) -> str:
        attempts_seen.append(model)
        if model == "glm-5-3":
            raise RuntimeError("unreachable")
        return '{"explanation":"ok","confidence":0.7}'

    router = ModelRouter(
        models=["glm-5-3", "glm-5-3-flash", "kimi-k3", "muse-glimmer-30b"],
        failure_threshold=3,
        call=call,
        timeout_seconds=5,
    )
    model, text, attempts, _latency = asyncio.run(router.generate(messages()))

    assert model == "glm-5-3-flash"
    assert attempts_seen.count("glm-5-3") == 3, "must try the active model three times before switching"
    assert attempts_seen.count("glm-5-3-flash") == 1
    assert router.active_model == "glm-5-3-flash"
    assert [a["outcome"] for a in attempts[:3]] == ["failure", "failure", "failure"]
    assert attempts[3]["outcome"] == "success"


def test_healthy_model_is_not_switched():
    async def call(_model: str, _messages: list[dict]) -> str:
        return '{"explanation":"fine","confidence":0.9}'

    router = ModelRouter(
        models=["glm-5-3", "glm-5-3-flash"],
        failure_threshold=3,
        call=call,
        timeout_seconds=5,
    )
    asyncio.run(router.generate(messages()))
    asyncio.run(router.generate(messages()))

    assert router.active_model == "glm-5-3"
    assert router.states[0].successful_calls == 2
    assert router.states[1].successful_calls == 0


def test_all_models_failing_raises_and_rotates_through_list():
    async def call(_model: str, _messages: list[dict]) -> str:
        raise RuntimeError("down")

    router = ModelRouter(
        models=["a", "b"],
        failure_threshold=2,
        call=call,
        timeout_seconds=5,
    )
    try:
        asyncio.run(router.generate(messages()))
        raise AssertionError("expected all models to fail")
    except Exception as exc:  # noqa: BLE001
        assert "all configured models failed" in str(exc).lower() or "down" in str(exc).lower()
    assert {state.healthy for state in router.states} == {False}


def test_zero_threshold_is_clamped_and_still_attempts_models():
    """A threshold below 1 must not silently skip every trial."""
    calls: list[str] = []

    async def call(model: str, _messages: list[dict]) -> str:
        calls.append(model)
        raise RuntimeError("unreachable")

    router = ModelRouter(models=["m1", "m2"], failure_threshold=0, call=call, timeout_seconds=5)
    try:
        asyncio.run(router.generate(messages()))
        raise AssertionError("expected failure")
    except Exception as exc:  # noqa: BLE001
        attempts = getattr(exc, "attempts", []) or []

    assert router.failure_threshold >= 1
    assert calls == ["m1", "m2"], "each model should be attempted at least once"
    assert len(attempts) >= 2, "the failure must carry an attempt trail"
    assert all("error" in attempt for attempt in attempts)


def test_no_models_configured_reports_clearly():
    async def call(_model: str, _messages: list[dict]) -> str:
        return "{}"

    router = ModelRouter(models=[], failure_threshold=3, call=call, timeout_seconds=5)
    try:
        asyncio.run(router.generate(messages()))
        raise AssertionError("expected failure")
    except Exception as exc:  # noqa: BLE001
        assert "no models configured" in str(exc)
        assert getattr(exc, "attempts", []), "even this failure must carry an attempt entry"


def test_inference_failure_carries_attempts(monkeypatch):
    """The service must never return attempts: [] for a failure."""
    from inference import main as inference_main
    from inference.router import ModelUnavailable

    class FailingRouter:
        active_model = "z-ai/glm-5.3"

        async def generate(self, _messages):
            raise ModelUnavailable("all configured models failed after retries")

    monkeypatch.setattr(inference_main, "router", FailingRouter())
    request = inference_main.AnalyzeRequest(incident={"incident_id": "INC-X", "app_id": "demo-app-001"}, history=[])
    result = asyncio.run(inference_main.analyze(request))

    assert result["status"] == "failed"
    assert result["attempts"], "a failed analysis must explain why each model failed"
