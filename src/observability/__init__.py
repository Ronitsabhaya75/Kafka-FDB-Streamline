"""Observability: Prometheus metrics, health endpoints, and JSON audit logs.

Public contract: [`docs/OBSERVABILITY-SPEC.md`](../../docs/OBSERVABILITY-SPEC.md).
"""

from src.observability.health import heartbeat, set_fdb_ready, set_kafka_ready
from src.observability.logging import audit_version_end
from src.observability.metrics import (
    record_cdc_versions,
    record_kafka_publish,
    record_mutation_polled,
    record_serializer_bytes,
    record_serializer_error,
)

__all__ = [
    "audit_version_end",
    "heartbeat",
    "record_cdc_versions",
    "record_kafka_publish",
    "record_mutation_polled",
    "record_serializer_bytes",
    "record_serializer_error",
    "set_fdb_ready",
    "set_kafka_ready",
]
