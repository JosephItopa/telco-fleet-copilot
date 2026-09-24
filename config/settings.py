"""Environment-driven configuration shared by every service."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def _bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def _list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name, "")
    if not raw.strip():
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    # --- runtime -----------------------------------------------------------
    service_name: str = field(default_factory=lambda: os.getenv("SERVICE_NAME", "aiops"))
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    hostname: str = field(default_factory=lambda: os.getenv("HOSTNAME", "local"))

    # --- kafka -------------------------------------------------------------
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    kafka_topic: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC", "app-performance-metrics"))
    kafka_dlq_topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_DLQ_TOPIC", "app-performance-metrics-dlq")
    )
    kafka_partitions: int = field(default_factory=lambda: _int("KAFKA_PARTITIONS", 12))
    kafka_replication_factor: int = field(default_factory=lambda: _int("KAFKA_REPLICATION_FACTOR", 1))
    kafka_consumer_group: str = field(
        default_factory=lambda: os.getenv("KAFKA_CONSUMER_GROUP", "aiops-partition-consumers")
    )
    kafka_max_poll_records: int = field(default_factory=lambda: _int("KAFKA_MAX_POLL_RECORDS", 500))
    kafka_linger_ms: int = field(default_factory=lambda: _int("KAFKA_LINGER_MS", 50))
    kafka_max_batch_size: int = field(default_factory=lambda: _int("KAFKA_MAX_BATCH_SIZE", 1_000_000))
    kafka_compression: str = field(default_factory=lambda: os.getenv("KAFKA_COMPRESSION", "gzip"))

    # --- postgres ----------------------------------------------------------
    database_url: str = field(
        default_factory=lambda: os.getenv(
            "DATABASE_URL", "postgresql+psycopg2://aiops:aiops@localhost:5432/aiops"
        )
    )
    db_pool_size: int = field(default_factory=lambda: _int("DB_POOL_SIZE", 10))
    db_max_overflow: int = field(default_factory=lambda: _int("DB_MAX_OVERFLOW", 20))

    # --- collector ---------------------------------------------------------
    collector_mode: str = field(default_factory=lambda: os.getenv("COLLECTOR_MODE", "seed"))
    collector_id: str = field(
        default_factory=lambda: os.getenv("COLLECTOR_ID") or os.getenv("HOSTNAME", "collector-local")
    )
    collector_port: int = field(default_factory=lambda: _int("COLLECTOR_PORT", 9100))
    collect_interval_seconds: int = field(default_factory=lambda: _int("COLLECT_INTERVAL_SECONDS", 15))
    clusters: list[str] = field(default_factory=lambda: _list("CLUSTERS", ["cluster-a", "cluster-b", "cluster-c"]))
    kubeconfig: str = field(default_factory=lambda: os.getenv("KUBECONFIG", ""))
    k8s_contexts: list[str] = field(default_factory=lambda: _list("K8S_CONTEXTS", []))
    # seed-k8s fallback source (used when no live cluster is reachable)
    seed_apps: int = field(default_factory=lambda: _int("SEED_APPS", 50))
    seed_unhealthy_ratio: float = field(default_factory=lambda: _float("SEED_UNHEALTHY_RATIO", 0.08))
    seed_cluster: str = field(default_factory=lambda: os.getenv("SEED_CLUSTER", "demo-cluster"))
    seed_namespace: str = field(default_factory=lambda: os.getenv("SEED_NAMESPACE", "demo"))
    seed_collector_id: str = field(default_factory=lambda: os.getenv("SEED_COLLECTOR_ID", "seed-publisher"))
    # The seed publishes on a fixed 120s cadence, in the collector and standalone.
    seed_interval_seconds: int = field(default_factory=lambda: _int("SEED_INTERVAL_SECONDS", 120))

    # --- consumer ----------------------------------------------------------
    consumer_port: int = field(default_factory=lambda: _int("CONSUMER_PORT", 9150))
    consumer_max_retries: int = field(default_factory=lambda: _int("CONSUMER_MAX_RETRIES", 3))
    detector_url: str = field(default_factory=lambda: os.getenv("DETECTOR_URL", "http://localhost:9200"))

    # --- detector ----------------------------------------------------------
    detector_id: str = field(
        default_factory=lambda: os.getenv("DETECTOR_ID") or os.getenv("HOSTNAME", "detector-local")
    )
    detector_port: int = field(default_factory=lambda: _int("DETECTOR_PORT", 9200))
    detection_window_seconds: int = field(default_factory=lambda: _int("DETECTION_WINDOW_SECONDS", 300))
    incident_resolve_after_seconds: int = field(
        default_factory=lambda: _int("INCIDENT_RESOLVE_AFTER_SECONDS", 900)
    )
    evidence_max_items: int = field(default_factory=lambda: _int("EVIDENCE_MAX_ITEMS", 20))

    thresh_cpu_ratio: float = field(default_factory=lambda: _float("THRESH_CPU_RATIO", 0.90))
    thresh_memory_ratio: float = field(default_factory=lambda: _float("THRESH_MEMORY_RATIO", 0.90))
    thresh_restarts: int = field(default_factory=lambda: _int("THRESH_RESTARTS", 5))
    thresh_growth_ratio: float = field(default_factory=lambda: _float("THRESH_GROWTH_RATIO", 0.40))
    thresh_node_cpu_ratio: float = field(default_factory=lambda: _float("THRESH_NODE_CPU_RATIO", 0.90))
    thresh_node_memory_ratio: float = field(default_factory=lambda: _float("THRESH_NODE_MEMORY_RATIO", 0.90))

    # --- api ---------------------------------------------------------------
    api_port: int = field(default_factory=lambda: _int("API_PORT", 8000))
    inference_url: str = field(default_factory=lambda: os.getenv("INFERENCE_URL", "http://localhost:9300"))
    collector_url: str = field(default_factory=lambda: os.getenv("COLLECTOR_URL", "http://localhost:9100"))
    consumer_url: str = field(default_factory=lambda: os.getenv("CONSUMER_URL", "http://localhost:9150"))

    # --- inference ---------------------------------------------------------
    inference_port: int = field(default_factory=lambda: _int("INFERENCE_PORT", 9300))
    nvidia_api_key: str = field(default_factory=lambda: os.getenv("NVIDIA_API_KEY", ""))
    nvidia_base_url: str = field(
        default_factory=lambda: os.getenv("NVIDIA_BASE_URL", "https://integrate.api.nvidia.com/v1")
    )
    nvidia_models: list[str] = field(
        default_factory=lambda: _list(
            "NVIDIA_MODELS", ["glm-5-3", "glm-5-3-flash", "kimi-k3", "muse-glimmer-30b"]
        )
    )
    model_failure_threshold: int = field(default_factory=lambda: _int("MODEL_FAILURE_THRESHOLD", 3))
    llm_timeout_seconds: float = field(default_factory=lambda: _float("LLM_TIMEOUT_SECONDS", 60))
    llm_max_tokens: int = field(default_factory=lambda: _int("LLM_MAX_TOKENS", 2048))
    llm_temperature: float = field(default_factory=lambda: _float("LLM_TEMPERATURE", 0.2))

    # --- dashboard ---------------------------------------------------------
    dashboard_refresh_seconds: int = field(default_factory=lambda: _int("DASHBOARD_REFRESH_SECONDS", 15))


settings = Settings()
