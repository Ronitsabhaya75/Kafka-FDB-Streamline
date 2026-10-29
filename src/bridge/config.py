"""Declarative configuration models and dictionary parsers for the Kafka Bridge."""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Any


@dataclass(frozen=True, slots=True)
class FDBConfig:
    """FoundationDB connection settings."""

    api_version: int = 800
    cluster_file: str | None = None


@dataclass(frozen=True, slots=True)
class CDCConfig:
    """FoundationDB Native CDC stream configuration."""

    stream_name: str = ""
    begin_key: bytes = b""
    end_key: bytes = b""
    max_record_bytes: int = 1_000_000
    cursor_file: str | None = None


@dataclass(frozen=True, slots=True)
class KafkaConfig:
    """Kafka producer and broker destination settings."""

    bootstrap_servers: str = "localhost:9092"
    topic: str = "fdb-cdc"
    transactional_id: str | None = None


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    """Runtime timeouts and backoff intervals for the bridge loop."""

    flush_timeout_seconds: float = 10.0
    backoff_base_seconds: float = 0.1
    backoff_max_seconds: float = 2.0
    shutdown_timeout_seconds: float = 15.0


@dataclass(frozen=True, slots=True)
class BridgeConfig:
    """Top-level declarative configuration for the autonomous Bridge daemon."""

    fdb: FDBConfig = field(default_factory=FDBConfig)
    cdc: CDCConfig = field(default_factory=CDCConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)


def _require_dict(section: str, value: Any) -> dict[str, Any]:
    """Validate that a configuration section is a dictionary."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise TypeError(f"The '{section}' section must be a dictionary")
    return value


def _reject_unknown_keys(section: str, value: dict[str, Any], cls: type) -> None:
    """Raise if a section carries keys that are not fields of its config class."""
    unknown = sorted(set(value) - {f.name for f in fields(cls)})
    if unknown:
        raise ValueError(f"Unknown key(s) in '{section}': {', '.join(unknown)}")


def _parse_key(key: Any, name: str) -> bytes:
    """Parse a key configuration value into bytes."""
    if isinstance(key, str):
        try:
            return bytes.fromhex(key)
        except ValueError as exc:
            raise ValueError(f"cdc.{name} is not a valid hex string: {key!r}") from exc
    if isinstance(key, (bytes, bytearray)):
        return bytes(key)
    raise TypeError(
        f"cdc.{name} must be a hex string or bytes, got {type(key).__name__}"
    )


def from_dict(data: dict[str, Any]) -> BridgeConfig:
    """Parse a dictionary into a validated BridgeConfig instance.

    Args:
        data: Nested dictionary carrying configuration values.

    Returns:
        Structured and typed BridgeConfig instance.

    Raises:
        TypeError: If data or a sub-configuration section is not a dictionary,
            or if any string or numeric setting has an invalid type.
        ValueError: If any section has an unknown key, stream_name, topic or
            transactional_id is empty or whitespace-only, a key range is
            one-sided or has begin_key >= end_key, a hex string is invalid, or
            numeric intervals are not positive.
    """
    if not isinstance(data, dict):
        raise TypeError(
            f"Configuration must be a dictionary, got {type(data).__name__}"
        )

    _reject_unknown_keys("<root>", data, BridgeConfig)
    fdb_dict = _require_dict("fdb", data.get("fdb"))
    cdc_dict = _require_dict("cdc", data.get("cdc"))
    kafka_dict = _require_dict("kafka", data.get("kafka"))
    pipeline_dict = _require_dict("pipeline", data.get("pipeline"))
    _reject_unknown_keys("fdb", fdb_dict, FDBConfig)
    _reject_unknown_keys("cdc", cdc_dict, CDCConfig)
    _reject_unknown_keys("kafka", kafka_dict, KafkaConfig)
    _reject_unknown_keys("pipeline", pipeline_dict, PipelineConfig)

    # FDB validation
    raw_api_version = fdb_dict.get("api_version", 800)
    if not isinstance(raw_api_version, int) or isinstance(raw_api_version, bool):
        raise TypeError("fdb.api_version must be an integer")
    if raw_api_version < 1:
        raise ValueError("fdb.api_version must be positive")

    cluster_file = fdb_dict.get("cluster_file")
    if cluster_file is not None and not isinstance(cluster_file, str):
        raise TypeError("fdb.cluster_file must be a string or None")

    # CDC validation
    stream_name = cdc_dict.get("stream_name", "")
    if not isinstance(stream_name, str):
        raise TypeError("cdc.stream_name must be a string")
    if not stream_name.strip():
        raise ValueError("cdc.stream_name must be a non-empty string")

    raw_begin = cdc_dict.get("begin_key", "")
    raw_end = cdc_dict.get("end_key", "")

    begin_key = _parse_key(raw_begin, "begin_key")
    end_key = _parse_key(raw_end, "end_key")

    if bool(begin_key) != bool(end_key):
        raise ValueError(
            "cdc.begin_key and cdc.end_key must either both be specified "
            "or both omitted"
        )
    if begin_key and begin_key >= end_key:
        raise ValueError("cdc.begin_key must be less than cdc.end_key")

    max_record_bytes = cdc_dict.get("max_record_bytes", 1_000_000)
    if not isinstance(max_record_bytes, int) or isinstance(max_record_bytes, bool):
        raise TypeError("cdc.max_record_bytes must be an integer")
    if max_record_bytes < 1:
        raise ValueError("cdc.max_record_bytes must be positive")

    cursor_file = cdc_dict.get("cursor_file")
    if cursor_file is not None and not isinstance(cursor_file, str):
        raise TypeError("cdc.cursor_file must be a string or None")

    # Kafka validation
    bootstrap_servers = kafka_dict.get("bootstrap_servers", "localhost:9092")
    if not isinstance(bootstrap_servers, str) or not bootstrap_servers.strip():
        raise ValueError("kafka.bootstrap_servers must be a non-empty string")

    topic = kafka_dict.get("topic", "fdb-cdc")
    if not isinstance(topic, str) or not topic.strip():
        raise ValueError("kafka.topic must be a non-empty string")

    transactional_id = kafka_dict.get("transactional_id")
    if transactional_id is not None:
        if not isinstance(transactional_id, str):
            raise TypeError("kafka.transactional_id must be a string or None")
        if not transactional_id.strip():
            raise ValueError("kafka.transactional_id must be a non-empty string")

    # Pipeline validation
    flush_timeout = pipeline_dict.get("flush_timeout_seconds", 10.0)
    if (
        not isinstance(flush_timeout, (int, float))
        or isinstance(flush_timeout, bool)
        or flush_timeout <= 0
    ):
        raise ValueError("pipeline.flush_timeout_seconds must be a positive number")

    backoff_base = pipeline_dict.get("backoff_base_seconds", 0.1)
    if (
        not isinstance(backoff_base, (int, float))
        or isinstance(backoff_base, bool)
        or backoff_base <= 0
    ):
        raise ValueError("pipeline.backoff_base_seconds must be a positive number")

    backoff_max = pipeline_dict.get("backoff_max_seconds", 2.0)
    if (
        not isinstance(backoff_max, (int, float))
        or isinstance(backoff_max, bool)
        or backoff_max <= 0
    ):
        raise ValueError("pipeline.backoff_max_seconds must be a positive number")

    shutdown_timeout = pipeline_dict.get("shutdown_timeout_seconds", 15.0)
    if (
        not isinstance(shutdown_timeout, (int, float))
        or isinstance(shutdown_timeout, bool)
        or shutdown_timeout <= 0
    ):
        raise ValueError("pipeline.shutdown_timeout_seconds must be a positive number")

    return BridgeConfig(
        fdb=FDBConfig(
            api_version=raw_api_version,
            cluster_file=cluster_file,
        ),
        cdc=CDCConfig(
            stream_name=stream_name,
            begin_key=begin_key,
            end_key=end_key,
            max_record_bytes=max_record_bytes,
            cursor_file=cursor_file,
        ),
        kafka=KafkaConfig(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            transactional_id=transactional_id,
        ),
        pipeline=PipelineConfig(
            flush_timeout_seconds=float(flush_timeout),
            backoff_base_seconds=float(backoff_base),
            backoff_max_seconds=float(backoff_max),
            shutdown_timeout_seconds=float(shutdown_timeout),
        ),
    )
