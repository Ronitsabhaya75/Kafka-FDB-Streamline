"""Observability: Prometheus metrics, health endpoints, and JSON audit logs.

Public contract: [`docs/OBSERVABILITY-SPEC.md`](../../docs/OBSERVABILITY-SPEC.md).
"""

from src.observability.health import (
    configure as configure_health,
)
from src.observability.health import (
    heartbeat,
    is_live,
    is_ready,
    set_fdb_ready,
    set_kafka_ready,
)
from src.observability.http import start_http_server, stop_http_server
from src.observability.logging import audit_version_end, configure_logging, get_logger
from src.observability.metrics import (
    REGISTRY,
    record_cdc_versions,
    record_kafka_publish,
    record_mutation_polled,
    record_serializer_bytes,
    record_serializer_error,
)

__all__ = [
    "REGISTRY",
    "audit_version_end",
    "configure_health",
    "configure_logging",
    "get_logger",
    "heartbeat",
    "is_live",
    "is_ready",
    "record_cdc_versions",
    "record_kafka_publish",
    "record_mutation_polled",
    "record_serializer_bytes",
    "record_serializer_error",
    "set_fdb_ready",
    "set_kafka_ready",
    "start_http_server",
    "stop_http_server",
]
