"""Declarative configuration models and dictionary parsers for the Kafka Bridge."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
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
    idle_ack_max_replies: int | None = None
    idle_ack_max_seconds: float | None = None


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

    idle_ack_max_replies = cdc_dict.get("idle_ack_max_replies")
    if idle_ack_max_replies is not None:
        if type(idle_ack_max_replies) is not int:
            raise TypeError("cdc.idle_ack_max_replies must be an integer")
        if idle_ack_max_replies < 1:
            raise ValueError("cdc.idle_ack_max_replies must be positive")

    idle_ack_max_seconds = cdc_dict.get("idle_ack_max_seconds")
    if idle_ack_max_seconds is not None:
        if not isinstance(idle_ack_max_seconds, (int, float)) or isinstance(
            idle_ack_max_seconds, bool
        ):
            raise TypeError("cdc.idle_ack_max_seconds must be numeric")
        try:
            idle_ack_max_seconds = float(idle_ack_max_seconds)
        except OverflowError:
            raise ValueError("cdc.idle_ack_max_seconds must be finite") from None
        if not math.isfinite(idle_ack_max_seconds):
            raise ValueError("cdc.idle_ack_max_seconds must be finite")
        if idle_ack_max_seconds <= 0:
            raise ValueError("cdc.idle_ack_max_seconds must be positive")

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
            idle_ack_max_replies=idle_ack_max_replies,
            idle_ack_max_seconds=idle_ack_max_seconds,
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


def env_to_dict(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Map environment variables into a nested configuration dictionary.

    Environment variables map as follows:
        - FDB_API_VERSION -> fdb.api_version (int)
        - FDB_CLUSTER_FILE -> fdb.cluster_file (str)
        - FDB_CDC_STREAM_NAME or CDC_STREAM_NAME -> cdc.stream_name (str)
        - FDB_CDC_BEGIN_KEY or CDC_BEGIN_KEY -> cdc.begin_key (hex str)
        - FDB_CDC_END_KEY or CDC_END_KEY -> cdc.end_key (hex str)
        - FDB_CDC_MAX_RECORD_BYTES or CDC_MAX_RECORD_BYTES -> cdc.max_record_bytes (int)
        - FDB_CDC_CURSOR_FILE or CDC_CURSOR_FILE -> cdc.cursor_file (str)
        - FDB_CDC_IDLE_ACK_MAX_REPLIES or CDC_IDLE_ACK_MAX_REPLIES ->
          cdc.idle_ack_max_replies (int)
        - FDB_CDC_IDLE_ACK_MAX_SECONDS or CDC_IDLE_ACK_MAX_SECONDS ->
          cdc.idle_ack_max_seconds (float)
        - KAFKA_BOOTSTRAP_SERVERS -> kafka.bootstrap_servers (str)
        - KAFKA_TOPIC -> kafka.topic (str)
        - KAFKA_TRANSACTIONAL_ID -> kafka.transactional_id (str)
        - PIPELINE_FLUSH_TIMEOUT_SECONDS -> pipeline.flush_timeout_seconds (float)
        - PIPELINE_BACKOFF_BASE_SECONDS -> pipeline.backoff_base_seconds (float)
        - PIPELINE_BACKOFF_MAX_SECONDS -> pipeline.backoff_max_seconds (float)
        - PIPELINE_SHUTDOWN_TIMEOUT_SECONDS -> pipeline.shutdown_timeout_seconds (float)

    Args:
        env: Optional mapping of environment variables; defaults to ``os.environ``.

    Returns:
        Nested dictionary suitable for passing to ``from_dict``.

    Raises:
        TypeError: If an environment value cannot be coerced to the expected type.
        ValueError: If an environment value fails validation.
    """
    if env is None:
        env = os.environ

    fdb_dict: dict[str, Any] = {}
    cdc_dict: dict[str, Any] = {}
    kafka_dict: dict[str, Any] = {}
    pipeline_dict: dict[str, Any] = {}

    if "FDB_API_VERSION" in env:
        try:
            fdb_dict["api_version"] = int(env["FDB_API_VERSION"])
        except ValueError as exc:
            raise TypeError("fdb.api_version must be an integer") from exc
    if "FDB_CLUSTER_FILE" in env:
        fdb_dict["cluster_file"] = env["FDB_CLUSTER_FILE"]

    stream = env.get("FDB_CDC_STREAM_NAME") or env.get("CDC_STREAM_NAME")
    if stream is not None:
        cdc_dict["stream_name"] = stream
    begin = env.get("FDB_CDC_BEGIN_KEY") or env.get("CDC_BEGIN_KEY")
    if begin is not None:
        cdc_dict["begin_key"] = begin
    end = env.get("FDB_CDC_END_KEY") or env.get("CDC_END_KEY")
    if end is not None:
        cdc_dict["end_key"] = end
    max_rec = env.get("FDB_CDC_MAX_RECORD_BYTES") or env.get("CDC_MAX_RECORD_BYTES")
    if max_rec is not None:
        try:
            cdc_dict["max_record_bytes"] = int(max_rec)
        except ValueError as exc:
            raise TypeError("cdc.max_record_bytes must be an integer") from exc
    cursor = env.get("FDB_CDC_CURSOR_FILE") or env.get("CDC_CURSOR_FILE")
    if cursor is not None:
        cdc_dict["cursor_file"] = cursor
    idle_replies = env.get("FDB_CDC_IDLE_ACK_MAX_REPLIES") or env.get(
        "CDC_IDLE_ACK_MAX_REPLIES"
    )
    if idle_replies is not None:
        try:
            cdc_dict["idle_ack_max_replies"] = int(idle_replies)
        except ValueError as exc:
            raise TypeError("cdc.idle_ack_max_replies must be an integer") from exc
    idle_secs = env.get("FDB_CDC_IDLE_ACK_MAX_SECONDS") or env.get(
        "CDC_IDLE_ACK_MAX_SECONDS"
    )
    if idle_secs is not None:
        try:
            cdc_dict["idle_ack_max_seconds"] = float(idle_secs)
        except ValueError as exc:
            raise TypeError("cdc.idle_ack_max_seconds must be numeric") from exc

    if "KAFKA_BOOTSTRAP_SERVERS" in env:
        kafka_dict["bootstrap_servers"] = env["KAFKA_BOOTSTRAP_SERVERS"]
    if "KAFKA_TOPIC" in env:
        kafka_dict["topic"] = env["KAFKA_TOPIC"]
    if "KAFKA_TRANSACTIONAL_ID" in env:
        kafka_dict["transactional_id"] = env["KAFKA_TRANSACTIONAL_ID"]

    if "PIPELINE_FLUSH_TIMEOUT_SECONDS" in env:
        try:
            pipeline_dict["flush_timeout_seconds"] = float(
                env["PIPELINE_FLUSH_TIMEOUT_SECONDS"]
            )
        except ValueError as exc:
            raise ValueError(
                "pipeline.flush_timeout_seconds must be a positive number"
            ) from exc
    if "PIPELINE_BACKOFF_BASE_SECONDS" in env:
        try:
            pipeline_dict["backoff_base_seconds"] = float(
                env["PIPELINE_BACKOFF_BASE_SECONDS"]
            )
        except ValueError as exc:
            raise ValueError(
                "pipeline.backoff_base_seconds must be a positive number"
            ) from exc
    if "PIPELINE_BACKOFF_MAX_SECONDS" in env:
        try:
            pipeline_dict["backoff_max_seconds"] = float(
                env["PIPELINE_BACKOFF_MAX_SECONDS"]
            )
        except ValueError as exc:
            raise ValueError(
                "pipeline.backoff_max_seconds must be a positive number"
            ) from exc
    if "PIPELINE_SHUTDOWN_TIMEOUT_SECONDS" in env:
        try:
            pipeline_dict["shutdown_timeout_seconds"] = float(
                env["PIPELINE_SHUTDOWN_TIMEOUT_SECONDS"]
            )
        except ValueError as exc:
            raise ValueError(
                "pipeline.shutdown_timeout_seconds must be a positive number"
            ) from exc

    return {
        "fdb": fdb_dict,
        "cdc": cdc_dict,
        "kafka": kafka_dict,
        "pipeline": pipeline_dict,
    }


def from_env(env: Mapping[str, str] | None = None) -> BridgeConfig:
    """Parse environment variables into a validated BridgeConfig instance.

    Args:
        env: Optional mapping of environment variables; defaults to ``os.environ``.

    Returns:
        Structured and validated BridgeConfig instance.

    Raises:
        TypeError: If an environment value cannot be coerced to the expected type.
        ValueError: If an environment value fails validation.
    """
    return from_dict(env_to_dict(env))
