"""LLM reasoning: grounded recommendation generation for detected findings."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from openai import AsyncOpenAI

from common.models import AIAnalysis, Finding, utcnow

from . import catalog, config

logger = logging.getLogger("reasoning-worker.reasoner")

SYSTEM_PROMPT = (
    "You are the explanation and recommendation layer of an AIOps platform. "
    "You receive one detected finding with its measured evidence. You must only "
    "use the supplied evidence; never invent telemetry, services, or root causes. "
    "Recommend operational remediation for an engineer, ordered from safest to most "
    "impactful. Do not claim any action has been executed. Respond with a single JSON "
    "object and nothing else, using exactly these keys: "
    '{"summary": string, "root_cause": string, "remediation_steps": [string], '
    '"risk": string, "confidence": number between 0 and 1}.'
)


def _client() -> AsyncOpenAI | None:
    if not config.NVIDIA_API_KEY or config.NVIDIA_API_KEY.startswith("your_"):
        return None
    return AsyncOpenAI(
        api_key=config.NVIDIA_API_KEY,
        base_url=config.NVIDIA_BASE_URL,
        timeout=config.LLM_TIMEOUT_SECONDS,
    )


def build_prompt(finding: Finding) -> str:
    payload: dict[str, Any] = {
        "incident_id": finding.incident_id,
        "target": finding.target,
        "target_kind": finding.kind,
        "cluster": finding.cluster,
        "namespace": finding.namespace,
        "anomaly": finding.anomaly,
        "severity": finding.severity,
        "detector_reason": finding.reason,
        "evidence": finding.evidence,
    }
    return json.dumps(payload, indent=2, default=str)


def extract_json(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("```", 2)[1] if cleaned.count("```") >= 2 else cleaned.strip("`")
        if cleaned.startswith("json"):
            cleaned = cleaned[4:]
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError("response did not contain a JSON object")
    return json.loads(cleaned[start : end + 1])


def _to_analysis(data: dict[str, Any], latency: float) -> AIAnalysis:
    steps = data.get("remediation_steps") or data.get("steps") or []
    if isinstance(steps, str):
        steps = [steps]
    try:
        confidence = float(data.get("confidence", 0.6))
    except (TypeError, ValueError):
        confidence = 0.6
    return AIAnalysis(
        status="completed",
        provider="nvidia",
        model=config.NVIDIA_MODEL,
        summary=str(data.get("summary", "")).strip(),
        root_cause=str(data.get("root_cause", "")).strip(),
        remediation_steps=[str(step).strip() for step in steps if str(step).strip()][:10],
        risk=str(data.get("risk", "")).strip(),
        confidence=max(0.0, min(1.0, confidence)),
        latency_seconds=round(latency, 3),
        generated_at=utcnow(),
    )


async def analyze(finding: Finding) -> AIAnalysis:
    """Call the LLM, with retries; fall back to the deterministic catalog."""
    client = _client()
    if client is None:
        analysis = catalog.deterministic(finding)
        analysis.status = "failed"
        analysis.provider = "nvidia"
        analysis.model = config.NVIDIA_MODEL
        analysis.error = "NVIDIA_API_KEY is not configured; showing deterministic recommendation."
        return analysis

    last_error: str | None = None
    for attempt in range(1, config.LLM_RETRIES + 1):
        started = time.perf_counter()
        try:
            completion = await client.chat.completions.create(
                model=config.NVIDIA_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": build_prompt(finding)},
                ],
                temperature=config.LLM_TEMPERATURE,
                top_p=config.LLM_TOP_P,
                max_tokens=config.LLM_MAX_TOKENS,
                stream=False,
            )
            content = completion.choices[0].message.content or ""
            data = extract_json(content)
            analysis = _to_analysis(data, time.perf_counter() - started)
            if not analysis.summary:
                analysis.summary = catalog.deterministic(finding).summary
            logger.info("llm ok incident=%s latency=%.2fs", finding.incident_id, analysis.latency_seconds)
            return analysis
        except Exception as exc:  # noqa: BLE001 - retried, then falls back
            last_error = str(exc)
            logger.warning("llm attempt %s/%s failed: %s", attempt, config.LLM_RETRIES, last_error)
            if attempt < config.LLM_RETRIES:
                await asyncio.sleep(1.5 * attempt)

    analysis = catalog.deterministic(finding)
    analysis.status = "failed"
    analysis.provider = "nvidia"
    analysis.model = config.NVIDIA_MODEL
    analysis.error = last_error or "LLM call failed"
    return analysis