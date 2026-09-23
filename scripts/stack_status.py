"""Print the health of a locally running AIOps stack.

    python scripts/stack_status.py

Exits non-zero if a core service is unreachable. Requires no third-party packages.
"""

from __future__ import annotations

import json
import sys
import urllib.request

SERVICES = [
    ("collector", 9100, "/status", True),
    ("consumer", 9150, "/status", True),
    ("detector", 9200, "/status", True),
    ("api", 8000, "/summary", True),
    ("inference", 9300, "/status", False),
    ("dashboard", 8501, "/", False),
]

HIGHLIGHT = (
    "active_source", "fallback_reason", "kafka_connected", "cycles", "events_published",
    "consumed", "records_consumed", "records_forwarded", "records_dlq", "failures",
    "batches", "detections", "applications_monitored", "incidents_active", "last_error",
    "assigned_partitions", "active_model",
)


def main() -> int:
    failed = 0
    for name, port, path, core in SERVICES:
        url = f"http://127.0.0.1:{port}{path}"
        try:
            with urllib.request.urlopen(url, timeout=6) as response:
                body = response.read(8000).decode("utf-8", "replace")
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                print(f"OK    {name:<10} :{port} (non-JSON response)")
                continue
            print(f"OK    {name:<10} :{port}")
            for key in HIGHLIGHT:
                if key in data:
                    print(f"          {key}: {data[key]}")
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL  {name:<10} :{port} -> {type(exc).__name__}: {exc}")
            if core:
                failed += 1
    if failed:
        print(f"\n{failed} core service(s) unreachable. Start the stack with: docker compose up -d --build")
        return 1
    print("\nAll core services reachable.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
