from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

def _bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY", "")
NVIDIA_BASE_URL = os.getenv("NVIDIA_BASE_URL", "https://integrate.api.nvidia.com/v1")
NVIDIA_MODEL = os.getenv("NVIDIA_MODEL", "openai/gpt-oss-20b")
LLM_TIMEOUT_SECONDS = float(os.getenv("LLM_TIMEOUT_SECONDS", "60"))
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "2048"))
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "1"))
LLM_TOP_P = float(os.getenv("LLM_TOP_P", "1"))
LLM_RETRIES = int(os.getenv("LLM_RETRIES", "2"))

# Only reason (call the LLM) for findings at or above this severity.
REASON_MIN_SEVERITY = os.getenv("REASON_MIN_SEVERITY", "high").strip().lower()
ENRICH_MEDIUM_WITH_RULES = _bool("ENRICH_MEDIUM_WITH_RULES", True)

# Suppress duplicate findings with the same target/cluster/anomaly signature.
DEDUP_WINDOW_SECONDS = int(os.getenv("DEDUP_WINDOW_SECONDS", "900"))

WORKER_HOST = os.getenv("WORKER_HOST", "0.0.0.0")
WORKER_PORT = int(os.getenv("WORKER_PORT", "9100"))
