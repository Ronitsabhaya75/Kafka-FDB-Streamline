"""Prometheus metric definitions for the CDC → Kafka bridge."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

# Isolated registry so tests and the HTTP exporter share one coherent set and
# do not clash with the process-wide default REGISTRY.
REGISTRY = CollectorRegistry()

FDB_MUTATIONS_POLLED_TOTAL = Counter(
    "fdb_mutations_polled_total",
    "Native CDC mutations polled, labeled by opcode name.",
    ("opcode",),
    registry=REGISTRY,
)
FDB_CDC_LATEST_READ_VERSION = Gauge(
    "fdb_cdc_latest_read_version",
    "Latest FDB commit version processed from a CDC reply watermark.",
    registry=REGISTRY,
)
FDB_CDC_LAG_VERSIONS = Gauge(
    "fdb_cdc_lag_versions",
    "Distance between the cluster read version and the listener cursor.",
    registry=REGISTRY,
)
FDB_SERIALIZER_BYTES_OUT_TOTAL = Counter(
    "fdb_serializer_bytes_out_total",
    "Serialized Protobuf wire bytes produced by the CDC listener.",
    registry=REGISTRY,
)
FDB_SERIALIZER_ERRORS_TOTAL = Counter(
    "fdb_serializer_errors_total",
    "Serialization failures in the CDC poll path.",
    registry=REGISTRY,
)
KAFKA_RECORDS_PUBLISHED_TOTAL = Counter(
    "kafka_records_published_total",
    "Records successfully acknowledged by the Kafka publish path (acks=all).",
    registry=REGISTRY,
)
KAFKA_PUBLISH_LATENCY_SECONDS = Histogram(
    "kafka_publish_latency_seconds",
    "Round-trip publish latency for Kafka produce calls.",
    registry=REGISTRY,
)
KAFKA_VERSION_END_MARKERS_TOTAL = Counter(
    "kafka_version_end_markers_total",
    "VersionEnd watermark records published to Kafka.",
    registry=REGISTRY,
)

_OPCODE_NAMES: dict[int, str] = {
    0: "SET_VALUE",
    1: "CLEAR_RANGE",
    2: "ADD",
    6: "AND",
    7: "OR",
    8: "XOR",
    9: "APPEND_IF_FITS",
    12: "MAX",
    13: "MIN",
    14: "SET_VERSIONSTAMPED_KEY",
    15: "SET_VERSIONSTAMPED_VALUE",
    16: "BYTE_MIN",
    17: "BYTE_MAX",
    18: "MIN_V2",
    19: "AND_V2",
    20: "COMPARE_AND_CLEAR",
}


def opcode_name(type_code: int) -> str:
    """Return the Prometheus label for a native mutation type code.

    Args:
        type_code: Native CDC mutation type (0..255).

    Returns:
        A stable opcode name, or ``TYPE_<code>`` for undeclared codes.
    """
    return _OPCODE_NAMES.get(type_code, f"TYPE_{type_code}")


def record_mutation_polled(type_code: int) -> None:
    """Increment the polled-mutation counter for one native mutation.

    Args:
        type_code: Native CDC mutation type code.
    """
    FDB_MUTATIONS_POLLED_TOTAL.labels(opcode=opcode_name(type_code)).inc()


def record_cdc_versions(*, latest: int, cluster_read_version: int | None) -> None:
    """Update CDC watermark gauges.

    Args:
        latest: Latest consumed / reply watermark version.
        cluster_read_version: Fresh cluster read version, or ``None`` if unknown.
    """
    FDB_CDC_LATEST_READ_VERSION.set(latest)
    if cluster_read_version is None:
        FDB_CDC_LAG_VERSIONS.set(0)
    else:
        FDB_CDC_LAG_VERSIONS.set(max(0, cluster_read_version - latest))


def record_serializer_bytes(byte_count: int) -> None:
    """Add serialized payload bytes to the out counter.

    Args:
        byte_count: Number of bytes produced.
    """
    if byte_count > 0:
        FDB_SERIALIZER_BYTES_OUT_TOTAL.inc(byte_count)


def record_serializer_error() -> None:
    """Increment the serializer error counter."""
    FDB_SERIALIZER_ERRORS_TOTAL.inc()


def record_kafka_publish(*, latency_seconds: float, version_end: bool) -> None:
    """Record one successful Kafka publish (broker-ack semantics for the stub).

    Args:
        latency_seconds: Observed produce latency.
        version_end: Whether the published record is a VersionEnd marker.
    """
    KAFKA_RECORDS_PUBLISHED_TOTAL.inc()
    KAFKA_PUBLISH_LATENCY_SECONDS.observe(latency_seconds)
    if version_end:
        KAFKA_VERSION_END_MARKERS_TOTAL.inc()
