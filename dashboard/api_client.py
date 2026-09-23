"""Dashboard data access. The dashboard only talks to the API service."""

from __future__ import annotations

import os
from typing import Any

import requests

API_URL = os.getenv("API_URL", "http://localhost:8000").rstrip("/")
TIMEOUT = float(os.getenv("DASHBOARD_TIMEOUT_SECONDS", "30"))


class ApiError(RuntimeError):
    pass


def _get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        response = requests.get(f"{API_URL}{path}", params=params, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        raise ApiError(f"{path}: {exc}") from exc


def _post(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        response = requests.post(f"{API_URL}{path}", params=params, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        raise ApiError(f"{path}: {exc}") from exc


def summary() -> dict[str, Any]:
    return _get("/summary")


def current_anomalies(severity: str | None = None) -> list[dict[str, Any]]:
    params = {"limit": 500}
    if severity:
        params["severity"] = severity
    return _get("/anomalies/current", params).get("items", [])


def incidents(status: str | None = None, severity: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"limit": limit}
    if status:
        params["status"] = status
    if severity:
        params["severity"] = severity
    return _get("/incidents", params).get("items", [])


def incident(incident_id: str) -> dict[str, Any]:
    return _get(f"/incidents/{incident_id}")


def applications_health() -> dict[str, Any]:
    return _get("/applications/health")


def clusters_health() -> dict[str, Any]:
    return _get("/clusters/health")


def detectors_status() -> dict[str, Any]:
    return _get("/detectors/status")


def kafka_health() -> dict[str, Any]:
    return _get("/kafka/health")


def collectors_health() -> dict[str, Any]:
    return _get("/collectors/health")


def analyze(incident_id: str, force: bool = False) -> dict[str, Any]:
    return _post(f"/incidents/{incident_id}/analyze", {"force": str(force).lower()})
