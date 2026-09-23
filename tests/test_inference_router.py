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
