"""Declarative configuration models and dictionary parsers for the Kafka Bridge."""

from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from typing import Any

from fdb_kafka_bridge.cdc.directory import validate_directory_path
from fdb_kafka_bridge.record_size import (
    DEFAULT_MESSAGE_MAX_BYTES,
    KAFKA_MESSAGE_OVERHEAD_BYTES,
    max_record_bytes_for_key,
    min_record_cap,
)


@dataclass(frozen=True, slots=True)
class FDBConfig:
    """FoundationDB connection settings."""

    api_version: int = 800
    cluster_file: str | None = None


@dataclass(frozen=True, slots=True)
class CDCConfig:
    """FoundationDB Native CDC stream configuration.

    ``max_record_bytes`` of ``None`` lets the listener derive the largest
    record that fits Kafka's default ``message.max.bytes`` with the stream
    name as the message key.

    ``directory_path`` selects directory mode: the bridge resolves that
    existing Directory Layer path at startup (with every descendant when
    ``directory_recursive``) and registers one multi-range stream. It excludes
    ``begin_key``/``end_key``.
    """

    stream_name: str = ""
    begin_key: bytes = b""
    end_key: bytes = b""
    max_record_bytes: int | None = None
    cursor_file: str | None = None
    idle_ack_max_replies: int | None = None
    idle_ack_max_seconds: float | None = None
    directory_path: tuple[str, ...] | None = None
    directory_recursive: bool = False


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


def parse_directory_path_json(text: str, source: str) -> list[Any]:
    """Decode a directory path given as a JSON array, e.g. ``["app", "users"]``.

    JSON keeps every component intact, including ones containing ``/``, ``,``
    or whitespace, so no splitting convention is needed. Component types are
    checked later by :func:`from_dict`.

    Args:
        text: The JSON text.
        source: Where the text came from, for error messages.

    Returns:
        The decoded array.

    Raises:
        ValueError: If ``text`` is not valid JSON or not a JSON array.
    """
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{source} must be a JSON array of strings, e.g. "
            f"'[\"my_application\"]': {exc}"
        ) from exc
    if not isinstance(value, list):
        raise ValueError(f"{source} must be a JSON array of strings")
    return value


def _parse_directory_path(value: Any) -> tuple[str, ...] | None:
    """Validate ``cdc.directory_path`` from a list or tuple of strings."""
    if value is None:
        return None
    if isinstance(value, str):
        raise TypeError(
            "cdc.directory_path must be a list of path components, not a string"
        )
    if not isinstance(value, (list, tuple)):
        raise TypeError("cdc.directory_path must be a list of path components")
    return validate_directory_path(tuple(value), name="cdc.directory_path")


def _parse_env_bool(text: str, name: str) -> bool:
    """Parse ``true``/``false``/``1``/``0`` (case-insensitive)."""
    lowered = text.strip().lower()
    if lowered in ("true", "1"):
        return True
    if lowered in ("false", "0"):
        return False
    raise ValueError(f"{name} must be one of true, false, 1, 0; got {text!r}")


def _check_message_fits(
    max_record_bytes: int | None,
    stream_name: str,
    message_max_bytes: int = DEFAULT_MESSAGE_MAX_BYTES,
) -> None:
    """Reject a record cap whose largest Kafka message would exceed the limit.

    Every produced message carries the stream name as its key, so a record at
    the cap plus the key and librdkafka's per-message overhead must fit within
    ``message.max.bytes``; otherwise produce() rejects it after CDC has already
    delivered it. The name also sits in every record's envelope, so the cap must
    still hold the stream's largest ``VersionEnd``.

    Args:
        max_record_bytes: Configured cap on one serialized record value, or
            ``None`` for the cap derived from the key.
        stream_name: CDC stream name, used verbatim as the Kafka message key.
        message_max_bytes: Effective producer ``message.max.bytes``.

    Raises:
        ValueError: If an explicit cap exceeds the key's budget, or the
            effective cap cannot hold the stream's largest ``VersionEnd``.
    """
    key = stream_name.encode("utf-8")
    budget = max_record_bytes_for_key(key, message_max_bytes)
    if max_record_bytes is not None and max_record_bytes > budget:
        raise ValueError(
            f"cdc.max_record_bytes ({max_record_bytes}) exceeds the "
            f"{budget}-byte budget left by the {len(key)}-byte "
            f"cdc.stream_name key and {KAFKA_MESSAGE_OVERHEAD_BYTES} bytes of "
            f"message overhead within Kafka message.max.bytes "
            f"({message_max_bytes})"
        )
    floor = min_record_cap(stream_name)
    if max_record_bytes is not None and max_record_bytes < floor:
        raise ValueError(
            f"cdc.max_record_bytes ({max_record_bytes}) is smaller than the "
            f"{floor}-byte largest VersionEnd for the {len(key)}-byte "
            f"cdc.stream_name"
        )
    if max_record_bytes is None and budget < floor:
        raise ValueError(
            f"{len(key)}-byte cdc.stream_name, counted as the Kafka key and in "
            f"every record, leaves a {budget}-byte record budget within Kafka "
            f"message.max.bytes ({message_max_bytes}), smaller than the "
            f"{floor}-byte largest VersionEnd"
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
            one-sided or has begin_key >= end_key, a hex string is invalid,
            numeric intervals are not positive, an explicit max_record_bytes
            plus the UTF-8 stream_name key and Kafka per-message overhead
            exceeds the producer's message.max.bytes, the effective
            max_record_bytes cannot hold the stream's largest VersionEnd,
            directory_path is empty or combined with begin_key/end_key, or
            directory_recursive is set without directory_path.
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

    directory_path = _parse_directory_path(cdc_dict.get("directory_path"))
    directory_recursive = cdc_dict.get("directory_recursive", False)
    if type(directory_recursive) is not bool:
        raise TypeError("cdc.directory_recursive must be a boolean")
    if directory_path is not None and begin_key:
        raise ValueError(
            "cdc.directory_path cannot be combined with cdc.begin_key/cdc.end_key"
        )
    if directory_recursive and directory_path is None:
        raise ValueError("cdc.directory_recursive requires cdc.directory_path")

    max_record_bytes = cdc_dict.get("max_record_bytes")
    if max_record_bytes is not None:
        if not isinstance(max_record_bytes, int) or isinstance(max_record_bytes, bool):
            raise TypeError("cdc.max_record_bytes must be an integer or None")
        if max_record_bytes < 1:
            raise ValueError("cdc.max_record_bytes must be positive")
    _check_message_fits(max_record_bytes, stream_name)

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
            directory_path=directory_path,
            directory_recursive=directory_recursive,
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
        - FDB_CDC_DIRECTORY_PATH or CDC_DIRECTORY_PATH -> cdc.directory_path
          (JSON array of strings, e.g. ``["my_application"]``)
        - FDB_CDC_DIRECTORY_RECURSIVE or CDC_DIRECTORY_RECURSIVE ->
          cdc.directory_recursive (``true``/``false``/``1``/``0``)
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
    directory = env.get("FDB_CDC_DIRECTORY_PATH") or env.get("CDC_DIRECTORY_PATH")
    if directory is not None:
        cdc_dict["directory_path"] = parse_directory_path_json(
            directory, "FDB_CDC_DIRECTORY_PATH"
        )
    recursive = env.get("FDB_CDC_DIRECTORY_RECURSIVE") or env.get(
        "CDC_DIRECTORY_RECURSIVE"
    )
    if recursive is not None:
        cdc_dict["directory_recursive"] = _parse_env_bool(
            recursive, "FDB_CDC_DIRECTORY_RECURSIVE"
        )

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
