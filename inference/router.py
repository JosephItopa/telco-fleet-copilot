"""Model routing with sticky active model and 3-trial failover.

The router uses one model at a time. A model is only abandoned after it fails
``MODEL_FAILURE_THRESHOLD`` consecutive trials; when that happens the router
switches to the next model. A model that keeps succeeding is never switched away
from, so there is no unnecessary churn.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from config.logging_utils import get_logger
from config.metrics import AI_LATENCY, AI_REQUESTS
from config.settings import settings

logger = get_logger("inference.router")


class ModelUnavailable(RuntimeError):
    pass


@dataclass
class ModelState:
    name: str
    healthy: bool = True
    consecutive_switch_events: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    last_error: str | None = None
    last_latency_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "model": self.name,
            "healthy": self.healthy,
            "successful_calls": self.successful_calls,
            "failed_calls": self.failed_calls,
            "last_error": self.last_error,
            "last_latency_seconds": round(self.last_latency_seconds, 3),
        }


@dataclass
class ModelRouter:
    models: list[str]
    failure_threshold: int
    call: Callable[[str, list[dict[str, Any]]], Any]
    timeout_seconds: float
    active_index: int = 0
    states: list[ModelState] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.states:
            self.states = [ModelState(name=name) for name in self.models]
        # A threshold below 1 means no trial would ever run, which looks exactly
        # like "all models failed" while never contacting a model. Clamp it.
        self.failure_threshold = max(1, int(self.failure_threshold))

    @property
    def active_model(self) -> str:
        return self.states[self.active_index].name

    def snapshot(self) -> dict[str, Any]:
        return {
            "active_model": self.active_model,
            "failure_threshold": self.failure_threshold,
            "models": [state.to_dict() for state in self.states],
        }

    async def generate(
        self, messages: list[dict[str, Any]]
    ) -> tuple[str, str, list[dict[str, Any]], float]:
        """Return (model, text, attempts, latency_seconds)."""
        attempts: list[dict[str, Any]] = []
        if not self.states:
            error = ModelUnavailable("no models configured; set NVIDIA_MODELS")
            error.attempts = [
                {"model": "", "trial": 0, "outcome": "failure", "error": "no models configured"}
            ]
            raise error
        for _ in range(len(self.states)):
            state = self.states[self.active_index]
            for trial in range(1, self.failure_threshold + 1):
                started = time.perf_counter()
                try:
                    text = await asyncio.wait_for(self.call(state.name, messages), timeout=self.timeout_seconds)
                    latency = time.perf_counter() - started
                    if not str(text).strip():
                        raise RuntimeError("empty response from model")
                    state.healthy = True
                    state.successful_calls += 1
                    state.last_latency_seconds = latency
                    AI_REQUESTS.labels("inference", state.name, "success").inc()
                    AI_LATENCY.labels("inference", state.name).observe(latency)
                    attempts.append(
                        {"model": state.name, "trial": trial, "outcome": "success", "latency_seconds": round(latency, 3)}
                    )
                    return state.name, str(text), attempts, latency
                except Exception as exc:  # noqa: BLE001 - any failure counts as a trial failure
                    state.failed_calls += 1
                    state.last_error = str(exc)[:500]
                    AI_REQUESTS.labels("inference", state.name, "failure").inc()
                    attempts.append(
                        {"model": state.name, "trial": trial, "outcome": "failure", "error": str(exc)[:300]}
                    )
                    logger.warning(
                        "model trial failed",
                        extra={"model": state.name, "trial": trial, "error": str(exc)[:200]},
                    )
                    if trial < self.failure_threshold:
                        await asyncio.sleep(0.5 * trial)

            # Three failed trials: mark unhealthy and switch to the next model.
            state.healthy = False
            state.consecutive_switch_events += 1
            previous = state.name
            self.active_index = (self.active_index + 1) % len(self.states)
            logger.warning(
                "switching active model",
                extra={"failed_model": previous, "next_model": self.active_model, "threshold": self.failure_threshold},
            )

        error = ModelUnavailable(
            f"all configured models failed after retries (threshold={self.failure_threshold} trials/model)"
        )
        error.attempts = attempts or [
            {"model": self.active_model, "trial": 0, "outcome": "failure", "error": "no trials executed"}
        ]
        raise error


def build_client() -> Any:
    from openai import AsyncOpenAI

    if not settings.nvidia_api_key or settings.nvidia_api_key.startswith("your_"):
        return None
    return AsyncOpenAI(
        api_key=settings.nvidia_api_key,
        base_url=settings.nvidia_base_url,
        timeout=settings.llm_timeout_seconds,
        max_retries=0,
    )


async def call_model(model: str, messages: list[dict[str, Any]]) -> str:
    client = build_client()
    if client is None:
        raise ModelUnavailable("NVIDIA_API_KEY is not configured")
    completion = await client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=settings.llm_temperature,
        max_tokens=settings.llm_max_tokens,
        stream=False,
    )
    message = completion.choices[0].message
    content = message.content or ""
    if not content.strip():
        reasoning = getattr(message, "reasoning_content", None)
        detail = (
            "the model spent its whole token budget on the thinking channel"
            if reasoning
            else "the model returned an empty message"
        )
        raise RuntimeError(f"{model}: no content returned ({detail}); increase LLM_MAX_TOKENS")
    return content


def build_router() -> ModelRouter:
    return ModelRouter(
        models=settings.nvidia_models,
        failure_threshold=settings.model_failure_threshold,
        call=call_model,
        timeout_seconds=settings.llm_timeout_seconds,
    )
