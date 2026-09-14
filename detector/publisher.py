"""Publishes findings from the detector to the async reasoning worker."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from common.models import Finding

from . import config


async def publish(finding: Finding, client: httpx.AsyncClient | None = None) -> dict[str, Any]:
    """POST a finding to the reasoning worker with bounded retries.

    Never raises: the detector must stay operational even if the worker is down.
    """
    payload = finding.model_dump(mode="json")
    url = f"{config.WORKER_URL}/findings"
    last_error: str | None = None

    owns_client = client is None
    client = client or httpx.AsyncClient(timeout=10.0)
    try:
        for attempt in range(1, config.PUBLISH_RETRIES + 1):
            try:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                return {"ok": True, "attempt": attempt, "response": response.json()}
            except Exception as exc:  # noqa: BLE001 - retried, then reported
                last_error = str(exc)
                if attempt < config.PUBLISH_RETRIES:
                    await asyncio.sleep(config.PUBLISH_BACKOFF_SECONDS * attempt)
    finally:
        if owns_client:
            await client.aclose()

    return {"ok": False, "error": last_error}
