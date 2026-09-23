"""Grounded prompt construction and strict JSON parsing for incident analysis."""

from __future__ import annotations

import json
from typing import Any

SYSTEM_PROMPT = (
    "You are the analysis layer of an AIOps platform. You are given one detected "
    "incident and the telemetry evidence that produced it. You must reason ONLY from "
    "the supplied evidence: never invent, assume, or fabricate telemetry, service "
    "names, metrics, or root causes that are not present in the input. If the evidence "
    "is insufficient, say so and lower your confidence. "
    "Respond with a single JSON object and nothing else, using exactly these keys: "
    '{"explanation": string, "evidence": [string], "root_causes": [string], '
    '"remediation": [string], "confidence": number between 0 and 1, "next_action": string}. '
    "remediation steps must be ordered safest first, and must not claim any action was executed."
)


def build_messages(incident: dict[str, Any], history: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    evidence = incident.get("evidence", [])
    context = {
        "incident": {
            "incident_id": incident.get("incident_id"),
            "app_id": incident.get("app_id"),
            "cluster_id": incident.get("cluster_id"),
            "namespace": incident.get("namespace"),
            "anomaly_type": incident.get("anomaly_type"),
            "metric": incident.get("metric"),
            "severity": incident.get("severity"),
            "status": incident.get("status"),
            "first_detected": incident.get("first_detected"),
            "last_detected": incident.get("last_detected"),
            "observation_count": incident.get("observation_count"),
        },
        "evidence": evidence,
        "historical_context": [
            {
                "incident_id": item.get("incident_id"),
                "anomaly_type": item.get("anomaly_type"),
                "severity": item.get("severity"),
                "status": item.get("status"),
                "first_detected": item.get("first_detected"),
            }
            for item in (history or [])[:10]
        ],
    }
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps(context, indent=2, default=str)},
    ]


def extract_json(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        parts = cleaned.split("```")
        cleaned = parts[1] if len(parts) >= 2 else cleaned.strip("`")
        if cleaned.lstrip().startswith("json"):
            cleaned = cleaned.lstrip()[4:]
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError("model response did not contain a JSON object")
    return json.loads(cleaned[start : end + 1])


def normalize_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return [str(value)]
