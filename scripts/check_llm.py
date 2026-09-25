"""Check NVIDIA model connectivity without starting the platform.

    python scripts/check_llm.py

Reads .env, tries each configured model in order, and prints the exact outcome
(latency, timeout, HTTP error, or empty content). Exits non-zero if none work.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

# Allow `python scripts/check_llm.py` from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.logging_utils import configure_logging
from config.settings import settings
from inference.prompt import build_messages, extract_json
from inference.router import build_client

SAMPLE_INCIDENT = {
    "incident_id": "INC-CHECK",
    "app_id": "demo-app-004",
    "cluster_id": "demo-cluster",
    "namespace": "demo",
    "anomaly_type": "cpu_saturation",
    "metric": "pod_cpu",
    "severity": "high",
    "status": "ONGOING",
    "observation_count": 2,
    "first_detected": "2026-09-25T00:00:00+00:00",
    "last_detected": "2026-09-25T00:05:00+00:00",
    "evidence": [
        {"metric_type": "pod_cpu", "metrics": {"usage_cores": 0.95, "limit_cores": 1.0, "ratio": 0.95}}
    ],
}


async def check() -> int:
    key = settings.nvidia_api_key
    configured = bool(key) and not key.startswith("your_")
    print("=== AI model connectivity ===")
    print(f"base_url          : {settings.nvidia_base_url}")
    print(f"models            : {settings.nvidia_models}")
    print(f"api key           : {'configured' if configured else 'MISSING'}")
    print(f"timeout / tokens  : {settings.llm_timeout_seconds}s / {settings.llm_max_tokens}")
    print(f"failure threshold : {settings.model_failure_threshold} trials per model\n")

    client = build_client()
    if client is None:
        print("NVIDIA_API_KEY is not configured. Set it in .env and retry.")
        return 2

    messages = build_messages(SAMPLE_INCIDENT, [])
    for model in settings.nvidia_models:
        started = time.perf_counter()
        try:
            completion = await asyncio.wait_for(
                client.chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=settings.llm_temperature,
                    max_tokens=settings.llm_max_tokens,
                    stream=False,
                ),
                timeout=settings.llm_timeout_seconds,
            )
            elapsed = time.perf_counter() - started
            message = completion.choices[0].message
            content = message.content or ""
            if not content.strip():
                print(f"EMPTY   {model}  {elapsed:.1f}s  thinking used the whole budget; raise LLM_MAX_TOKENS")
                continue
            data = extract_json(content)
            print(f"OK      {model}  {elapsed:.1f}s  finish={completion.choices[0].finish_reason}  keys={sorted(data)[:6]}")
            print("\nModel access works. The stack can now analyse incidents.")
            return 0
        except asyncio.TimeoutError:
            print(f"TIMEOUT {model}  >{settings.llm_timeout_seconds:.0f}s  reasoning model needs more time")
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL    {model}  {time.perf_counter() - started:.1f}s  {type(exc).__name__}: {str(exc)[:200]}")

    print("\nAll configured models failed. Check NVIDIA_MODELS, the API key, and network access.")
    return 1


if __name__ == "__main__":
    configure_logging("check-llm")
    sys.exit(asyncio.run(check()))
