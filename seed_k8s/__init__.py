"""Seed Kubernetes fallback source for the AIOps platform."""

from seed_k8s.dataset import (
    DEFAULT_APPS,
    DEFAULT_CLUSTER,
    DEFAULT_NAMESPACE,
    DEFAULT_UNHEALTHY_RATIO,
    SeedSource,
)

__all__ = ["DEFAULT_APPS", "DEFAULT_CLUSTER", "DEFAULT_NAMESPACE", "DEFAULT_UNHEALTHY_RATIO", "SeedSource"]
