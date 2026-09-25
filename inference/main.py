"""AI inference service.

Called by the API service when an operator requests analysis for an incident.
Receives the incident plus supporting telemetry evidence and historical context,
and returns a grounded explanation, root-cause hypotheses, remediation and next
diagnostic action. Uses a sticky multi-model router with 3-trial failover.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel, Field

from config.logging_utils import configure_logging, get_logger
from config.metrics import mark_up, metrics_response
from config.settings import settings
from inference.prompt import build_messages, extract_json, normalize_list
from inference.router import build_router
from models.schema import AIAnalysis, AIStatus, utcnow_iso

configure_logging("inference")
logger = get_logger("inference.main")

router = build_router()
app = FastAPI(title="AIOps AI Inference", version="1.0.0")


def _openai_available() -> bool:
    try:
        import openai  # noqa: F401
    except ImportError:
        return False
    return True


OPENAI_AVAILABLE = _openai_available()


class AnalyzeRequest(BaseModel):
    incident: dict[str, Any]
    history: list[dict[str, Any]] = Field(default_factory=list)


@app.on_event("startup")
def startup() -> None:
    mark_up("inference")
    if not OPENAI_AVAILABLE:
        logger.error(
            "the 'openai' package is missing from this image; every AI call will fail. "
            "Rebuild inference with: docker compose up -d --build --force-recreate inference"
        )
    logger.info("inference started", extra={"models": settings.nvidia_models, "active": router.active_model})


def _confidence(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.5
    return max(0.0, min(1.0, number))


@app.post("/analyze")
async def analyze(request: AnalyzeRequest) -> dict[str, Any]:
    messages = build_messages(request.incident, request.history)
    attempts: list[dict[str, Any]] = []
    try:
        model, text, attempts, latency = await router.generate(messages)
        data = extract_json(text)
        analysis = AIAnalysis(
            status=AIStatus.COMPLETED,
            provider="nvidia",
            model=model,
            explanation=str(data.get("explanation", "")).strip(),
            evidence=normalize_list(data.get("evidence"))[:20],
            root_causes=normalize_list(data.get("root_causes"))[:10],
            remediation=normalize_list(data.get("remediation"))[:10],
            confidence=_confidence(data.get("confidence", 0.5)),
            next_action=str(data.get("next_action", "")).strip(),
            latency_seconds=latency,
            generated_at=utcnow_iso(),
            attempts=attempts,
        )
    except Exception as exc:  # noqa: BLE001 - report failure, never crash the caller
        logger.error("analysis failed", extra={"error": str(exc)})
        attempts = getattr(exc, "attempts", None) or attempts
        if not attempts:
            attempts = [
                {"model": router.active_model, "trial": 0, "outcome": "failure", "error": str(exc)[:300]}
            ]
        analysis = AIAnalysis(
            status=AIStatus.FAILED,
            provider="nvidia",
            model=router.active_model,
            error=str(exc)[:500],
            generated_at=utcnow_iso(),
            attempts=attempts,
        )
    return analysis.model_dump(mode="json")


@app.get("/")
def root() -> dict[str, Any]:
    return {"service": "inference", "active_model": router.active_model, "models": settings.nvidia_models}


@app.get("/health")
def health() -> dict[str, Any]:
    return {"service": "inference", "status": "healthy", "active_model": router.active_model}


@app.get("/status")
def status() -> dict[str, Any]:
    return {
        "service": "inference",
        "configured": bool(settings.nvidia_api_key) and not settings.nvidia_api_key.startswith("your_"),
        "openai_installed": OPENAI_AVAILABLE,
        "provider": "nvidia",
        "base_url": settings.nvidia_base_url,
        **router.snapshot(),
    }


@app.get("/models")
def models() -> dict[str, Any]:
    return router.snapshot()


@app.get("/metrics")
def metrics():
    return metrics_response()
