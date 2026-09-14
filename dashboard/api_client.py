"""HTTP client for the AIOps prototype services."""

from __future__ import annotations

import os
from typing import Any

import requests

WORKER_URL = os.getenv("WORKER_URL", "http://localhost:9100").rstrip("/")
DETECTOR_URL = os.getenv("DETECTOR_URL", "http://localhost:9000").rstrip("/")
TIMEOUT = float(os.getenv("DASHBOARD_TIMEOUT_SECONDS", "10"))


class ServiceError(RuntimeError):
    """Raised when a backend service is unreachable or returns an error."""


def _get(base: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        response = requests.get(f"{base}{path}", params=params, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        raise ServiceError(f"{base}{path}: {exc}") from exc


def get_status() -> dict[str, Any]:
    return _get(WORKER_URL, "/status")


def get_summary() -> dict[str, Any]:
    return _get(WORKER_URL, "/summary")


def get_findings(severity: str | None = None, kind: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"limit": limit}
    if severity:
        params["severity"] = severity
    if kind:
        params["kind"] = kind
    return _get(WORKER_URL, "/findings", params).get("items", [])


def reanalyze(incident_id: str) -> dict[str, Any]:
    try:
        response = requests.post(f"{WORKER_URL}/findings/{incident_id}/reanalyze", timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        raise ServiceError(f"reanalyze {incident_id}: {exc}") from exc


def detector_status() -> dict[str, Any] | None:
    try:
        return _get(DETECTOR_URL, "/status")
    except ServiceError:
        return None


def run_analysis() -> dict[str, Any]:
    try:
        response = requests.post(f"{DETECTOR_URL}/analyze", timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        raise ServiceError(f"analyze: {exc}") from exc
