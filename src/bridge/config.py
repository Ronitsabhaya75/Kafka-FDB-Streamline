"""Declarative configuration models and dictionary parsers for the Kafka Bridge."""

from __future__ import annotations

from dataclasses import dataclass, field
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


def from_dict(data: dict[str, Any]) -> BridgeConfig:
    """Parse a dictionary into a validated BridgeConfig instance.

    Args:
        data: Nested dictionary carrying configuration values.

    Returns:
        Structured and typed BridgeConfig instance.
    """
    fdb_dict = data.get("fdb", {})
    cdc_dict = data.get("cdc", {})
    kafka_dict = data.get("kafka", {})
    pipeline_dict = data.get("pipeline", {})

    raw_begin = cdc_dict.get("begin_key", "")
    raw_end = cdc_dict.get("end_key", "")

    begin_key = bytes.fromhex(raw_begin) if isinstance(raw_begin, str) else raw_begin
    end_key = bytes.fromhex(raw_end) if isinstance(raw_end, str) else raw_end

    return BridgeConfig(
        fdb=FDBConfig(
            api_version=int(fdb_dict.get("api_version", 800)),
            cluster_file=fdb_dict.get("cluster_file"),
        ),
        cdc=CDCConfig(
            stream_name=str(cdc_dict.get("stream_name", "")),
            begin_key=begin_key,
            end_key=end_key,
            max_record_bytes=int(cdc_dict.get("max_record_bytes", 1_000_000)),
            cursor_file=cdc_dict.get("cursor_file"),
        ),
        kafka=KafkaConfig(
            bootstrap_servers=str(
                kafka_dict.get("bootstrap_servers", "localhost:9092")
            ),
            topic=str(kafka_dict.get("topic", "fdb-cdc")),
        ),
        pipeline=PipelineConfig(
            flush_timeout_seconds=float(
                pipeline_dict.get("flush_timeout_seconds", 10.0)
            ),
            backoff_base_seconds=float(pipeline_dict.get("backoff_base_seconds", 0.1)),
            backoff_max_seconds=float(pipeline_dict.get("backoff_max_seconds", 2.0)),
            shutdown_timeout_seconds=float(
                pipeline_dict.get("shutdown_timeout_seconds", 15.0)
            ),
        ),
    )
