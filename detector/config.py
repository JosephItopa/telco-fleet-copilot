from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


def _bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090").rstrip("/")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "180"))
COLLECT_TIMEOUT_SECONDS = float(os.getenv("COLLECT_TIMEOUT_SECONDS", "10"))
COLLECT_RETRIES = int(os.getenv("COLLECT_RETRIES", "2"))
FALLBACK_ENABLED = _bool("FALLBACK_ENABLED", True)
INCIDENT_COOLDOWN_SECONDS = int(os.getenv("INCIDENT_COOLDOWN_SECONDS", "900"))

WORKER_URL = os.getenv("WORKER_URL", "http://localhost:9100").rstrip("/")
PUBLISH_RETRIES = int(os.getenv("PUBLISH_RETRIES", "3"))
PUBLISH_BACKOFF_SECONDS = float(os.getenv("PUBLISH_BACKOFF_SECONDS", "1.5"))

DETECTOR_HOST = os.getenv("DETECTOR_HOST", "0.0.0.0")
DETECTOR_PORT = int(os.getenv("DETECTOR_PORT", "9000"))

# Detection thresholds (prototype, single-scrape friendly).
THRESH_ERROR_RATIO = float(os.getenv("THRESH_ERROR_RATIO", "0.05"))
THRESH_LATENCY_SECONDS = float(os.getenv("THRESH_LATENCY_SECONDS", "2.0"))
THRESH_DEP_FAILURES = float(os.getenv("THRESH_DEP_FAILURES", "50"))
THRESH_FAILURES = float(os.getenv("THRESH_FAILURES", "100"))
THRESH_CLUSTER_CPU = float(os.getenv("THRESH_CLUSTER_CPU", "0.90"))
THRESH_CLUSTER_MEMORY = float(os.getenv("THRESH_CLUSTER_MEMORY", "0.85"))
THRESH_PODS_PENDING = float(os.getenv("THRESH_PODS_PENDING", "20"))
