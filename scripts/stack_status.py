"""Print the health of a locally running AIOps stack.

    python scripts/stack_status.py

The collector, consumer and detector publish no host ports (so they can be
scaled), so they are inspected through the API service, which reaches them over
the compose network. Exits non-zero if a core service is unreachable.
Requires no third-party packages.
"""

from __future__ import annotations

import json
import sys
import urllib.request

API = "http://127.0.0.1:8000"

SERVICES = [
    ("api", f"{API}/summary", True),
    ("collector", f"{API}/collectors/health", True),
    ("consumer", f"{API}/kafka/health", True),
    ("detector", f"{API}/detectors/status", True),
    ("inference", "http://127.0.0.1:9300/status", False),
    ("dashboard", "http://127.0.0.1:8501/", False),
]

HIGHLIGHT = {
    "active_source", "fallback_reason", "kafka_connected", "cycles", "events_published",
    "records_consumed", "records_forwarded", "records_dlq", "failures", "batches",
    "detections", "incidents_new", "incidents_ongoing", "resolved", "applications_monitored",
    "applications_healthy", "applications_with_anomalies", "incidents_active", "last_error",
    "assigned_partitions", "active_model", "reachable", "status",
}


def _flatten(data: dict) -> dict:
    flat: dict = {}
    for key, value in data.items():
        if isinstance(value, dict):
            for inner_key, inner_value in value.items():
                if not isinstance(inner_value, (dict, list)):
                    flat[f"{key}.{inner_key}"] = inner_value
        elif not isinstance(value, list):
            flat[key] = value
    return flat


def main() -> int:
    failed = 0
    for name, url, core in SERVICES:
        try:
            with urllib.request.urlopen(url, timeout=6) as response:
                body = response.read(8000).decode("utf-8", "replace")
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                print(f"OK    {name:<10} {url} (non-JSON response)")
                continue
            print(f"OK    {name:<10} {url}")
            for key, value in _flatten(data).items():
                if key.split(".")[-1] in HIGHLIGHT:
                    print(f"          {key}: {value}")
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL  {name:<10} {url} -> {type(exc).__name__}: {exc}")
            if core:
                failed += 1
    if failed:
        print("\nSome core services are unreachable. Start or restart the stack:")
        print("  docker compose up -d --build")
        print("Scale a fleet (no host ports, so any count works):")
        print("  docker compose up -d --build --scale detector=3")
        return 1
    print("\nAll core services reachable.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
