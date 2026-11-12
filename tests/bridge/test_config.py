"""Unit tests for Bridge declarative configuration parsing and validation."""

from __future__ import annotations

import pytest

from fdb_kafka_bridge.bridge.config import (
    BridgeConfig,
    CDCConfig,
    FDBConfig,
    KafkaConfig,
    PipelineConfig,
    from_dict,
)

_CDC = {"stream_name": "s"}


def test_default_config() -> None:
    """Verify default configurations have expected baseline settings."""
    cfg = BridgeConfig()
    assert cfg.fdb.api_version == 800
    assert cfg.fdb.cluster_file is None
    assert cfg.cdc.stream_name == ""
    assert cfg.cdc.begin_key == b""
    assert cfg.cdc.end_key == b""
    assert cfg.cdc.max_record_bytes == 1_000_000
    assert cfg.cdc.cursor_file is None
    assert cfg.kafka.bootstrap_servers == "localhost:9092"
    assert cfg.kafka.topic == "fdb-cdc"
    assert cfg.pipeline.flush_timeout_seconds == 10.0
    assert cfg.pipeline.backoff_base_seconds == 0.1
    assert cfg.pipeline.backoff_max_seconds == 2.0
    assert cfg.pipeline.shutdown_timeout_seconds == 15.0


def test_from_dict_minimal() -> None:
    """Verify from_dict with only stream_name returns default configuration."""
    cfg = from_dict({"cdc": {"stream_name": "s"}})
    assert isinstance(cfg, BridgeConfig)
    assert isinstance(cfg.fdb, FDBConfig)
    assert isinstance(cfg.cdc, CDCConfig)
    assert isinstance(cfg.kafka, KafkaConfig)
    assert isinstance(cfg.pipeline, PipelineConfig)
    assert cfg.fdb.api_version == 800
    assert cfg.cdc.stream_name == "s"
    assert cfg.kafka.topic == "fdb-cdc"


def test_from_dict_custom_and_hex_keys() -> None:
    """Verify from_dict parses nested values and hex keys properly."""
    raw = {
        "fdb": {
            "api_version": 730,
            "cluster_file": "/tmp/test.cluster",
        },
        "cdc": {
            "stream_name": "test_stream",
            "begin_key": "616161",  # 'aaa' in hex
            "end_key": "626262",  # 'bbb' in hex
            "max_record_bytes": 500_000,
            "cursor_file": "/tmp/cursor.json",
        },
        "kafka": {
            "bootstrap_servers": "broker:9092",
            "topic": "events",
        },
        "pipeline": {
            "flush_timeout_seconds": 5.0,
            "backoff_base_seconds": 0.05,
            "backoff_max_seconds": 1.0,
            "shutdown_timeout_seconds": 8.0,
        },
    }
    cfg = from_dict(raw)
    assert cfg.fdb.api_version == 730
    assert cfg.fdb.cluster_file == "/tmp/test.cluster"
    assert cfg.cdc.stream_name == "test_stream"
    assert cfg.cdc.begin_key == b"aaa"
    assert cfg.cdc.end_key == b"bbb"
    assert cfg.cdc.max_record_bytes == 500_000
    assert cfg.cdc.cursor_file == "/tmp/cursor.json"
    assert cfg.kafka.bootstrap_servers == "broker:9092"
    assert cfg.kafka.topic == "events"
    assert cfg.pipeline.flush_timeout_seconds == 5.0
    assert cfg.pipeline.backoff_base_seconds == 0.05
    assert cfg.pipeline.backoff_max_seconds == 1.0
    assert cfg.pipeline.shutdown_timeout_seconds == 8.0


def test_from_dict_raw_bytes_keys() -> None:
    """Verify from_dict handles raw bytes for begin_key and end_key."""
    raw = {
        "cdc": {
            "stream_name": "s",
            "begin_key": b"\x01\x02",
            "end_key": b"\x01\x03",
        }
    }
    cfg = from_dict(raw)
    assert cfg.cdc.begin_key == b"\x01\x02"
    assert cfg.cdc.end_key == b"\x01\x03"


def test_from_dict_invalid_root_type() -> None:
    """Verify non-dictionary input raises TypeError."""
    with pytest.raises(TypeError, match="Configuration must be a dictionary"):
        from_dict("not-a-dict")  # type: ignore[arg-type]


def test_from_dict_invalid_section_type() -> None:
    """Verify non-dictionary section raises TypeError."""
    with pytest.raises(TypeError, match="The 'fdb' section must be a dictionary"):
        from_dict({"fdb": "not-a-dict"})


def test_from_dict_invalid_stream_name_type() -> None:
    """Verify non-string stream name raises TypeError."""
    with pytest.raises(TypeError, match="cdc.stream_name must be a string"):
        from_dict({"cdc": {"stream_name": 12345}})


def test_from_dict_invalid_hex_keys() -> None:
    """Verify invalid hex string in key range raises ValueError."""
    with pytest.raises(ValueError, match="not a valid hex string"):
        from_dict(
            {"cdc": {"stream_name": "s", "begin_key": "invalid-hex", "end_key": "61"}}
        )


def test_from_dict_one_sided_range() -> None:
    """Verify one-sided key range raises ValueError."""
    with pytest.raises(ValueError, match="both be specified or both omitted"):
        from_dict({"cdc": {"stream_name": "s", "begin_key": "6161", "end_key": ""}})

    with pytest.raises(ValueError, match="both be specified or both omitted"):
        from_dict({"cdc": {"stream_name": "s", "begin_key": "", "end_key": "6161"}})


def test_from_dict_invalid_key_type() -> None:
    """Verify non-hex/bytes key raises TypeError."""
    with pytest.raises(TypeError, match="must be a hex string or bytes"):
        from_dict({"cdc": {"stream_name": "s", "begin_key": 123, "end_key": 456}})


def test_from_dict_invalid_numeric_values() -> None:
    """Verify non-positive or invalid numeric values raise errors."""
    with pytest.raises(ValueError, match="fdb.api_version must be positive"):
        from_dict({"cdc": _CDC, "fdb": {"api_version": 0}})

    with pytest.raises(ValueError, match="cdc.max_record_bytes must be positive"):
        from_dict({"cdc": {**_CDC, "max_record_bytes": -10}})

    with pytest.raises(
        ValueError, match="pipeline.flush_timeout_seconds must be a positive number"
    ):
        from_dict({"cdc": _CDC, "pipeline": {"flush_timeout_seconds": 0}})

    with pytest.raises(ValueError, match="kafka.topic must be a non-empty string"):
        from_dict({"cdc": _CDC, "kafka": {"topic": "   "}})


@pytest.mark.parametrize("name", ["", "   ", "\t\n"])
def test_from_dict_rejects_blank_stream_name(name: str) -> None:
    """Verify an empty or whitespace-only stream name is rejected."""
    with pytest.raises(ValueError, match="cdc.stream_name must be a non-empty"):
        from_dict({"cdc": {"stream_name": name}})


def test_from_dict_missing_stream_name_rejected() -> None:
    """Verify omitting stream_name is rejected rather than defaulting to ''."""
    with pytest.raises(ValueError, match="cdc.stream_name must be a non-empty"):
        from_dict({"cdc": {}})


@pytest.mark.parametrize(
    ("raw", "where"),
    [
        ({"cdc": {"stream_name": "s"}, "kafak": {}}, "<root>"),
        ({"cdc": {"stream_name": "s"}, "fdb": {"apiversion": 800}}, "fdb"),
        ({"cdc": {"stream_name": "s", "cursor": "/tmp/c"}}, "cdc"),
        ({"cdc": {"stream_name": "s"}, "kafka": {"topics": "t"}}, "kafka"),
        (
            {"cdc": {"stream_name": "s"}, "pipeline": {"flush_timeout": 1.0}},
            "pipeline",
        ),
    ],
)
def test_from_dict_rejects_unknown_keys(raw: dict[str, object], where: str) -> None:
    """Verify a typo'd key at any level fails instead of silently defaulting."""
    with pytest.raises(ValueError, match=f"Unknown key.*{where}"):
        from_dict(raw)


@pytest.mark.parametrize(
    ("begin", "end"), [("6262", "6161"), ("6161", "6161"), ("616162", "6161")]
)
def test_from_dict_rejects_inverted_or_empty_range(begin: str, end: str) -> None:
    """Verify begin_key must sort strictly before end_key."""
    with pytest.raises(ValueError, match="begin_key must be less than"):
        from_dict({"cdc": {"stream_name": "s", "begin_key": begin, "end_key": end}})


def test_kafka_transactional_id_default_none() -> None:
    """Verify transactional mode is off unless configured."""
    assert KafkaConfig().transactional_id is None
    assert from_dict({"cdc": {"stream_name": "s"}}).kafka.transactional_id is None


def test_from_dict_parses_transactional_id() -> None:
    """Verify kafka.transactional_id is carried through."""
    cfg = from_dict({"cdc": {"stream_name": "s"}, "kafka": {"transactional_id": "b-1"}})
    assert cfg.kafka.transactional_id == "b-1"


@pytest.mark.parametrize("value", ["", "  "])
def test_from_dict_rejects_blank_transactional_id(value: str) -> None:
    """Verify an empty transactional_id is rejected rather than treated as off."""
    with pytest.raises(ValueError, match="kafka.transactional_id must be"):
        from_dict({"cdc": {"stream_name": "s"}, "kafka": {"transactional_id": value}})


def test_from_dict_rejects_non_string_transactional_id() -> None:
    """Verify a non-string transactional_id is a type error."""
    with pytest.raises(TypeError, match="kafka.transactional_id must be"):
        from_dict({"cdc": {"stream_name": "s"}, "kafka": {"transactional_id": 7}})


def test_cdc_config_idle_coalescing_defaults() -> None:
    """Verify idle ack coalescing is disabled by default."""
    cfg = CDCConfig()
    assert cfg.idle_ack_max_replies is None
    assert cfg.idle_ack_max_seconds is None


def test_from_dict_parses_idle_coalescing() -> None:
    """Verify cdc.idle_ack_max_replies and cdc.idle_ack_max_seconds are parsed."""
    cfg = from_dict(
        {
            "cdc": {
                "stream_name": "s",
                "idle_ack_max_replies": 5,
                "idle_ack_max_seconds": 12.5,
            }
        }
    )
    assert cfg.cdc.idle_ack_max_replies == 5
    assert cfg.cdc.idle_ack_max_seconds == 12.5


@pytest.mark.parametrize(
    ("val", "err", "match"),
    [
        (True, TypeError, "cdc.idle_ack_max_replies must be an integer"),
        ("5", TypeError, "cdc.idle_ack_max_replies must be an integer"),
        (2.5, TypeError, "cdc.idle_ack_max_replies must be an integer"),
        (0, ValueError, "cdc.idle_ack_max_replies must be positive"),
        (-1, ValueError, "cdc.idle_ack_max_replies must be positive"),
    ],
)
def test_from_dict_validates_idle_ack_max_replies(
    val: object, err: type[Exception], match: str
) -> None:
    """Verify idle_ack_max_replies rejects non-integers and non-positive values."""
    with pytest.raises(err, match=match):
        from_dict({"cdc": {"stream_name": "s", "idle_ack_max_replies": val}})


@pytest.mark.parametrize(
    ("val", "err", "match"),
    [
        (True, TypeError, "cdc.idle_ack_max_seconds must be numeric"),
        ("10.0", TypeError, "cdc.idle_ack_max_seconds must be numeric"),
        (0, ValueError, "cdc.idle_ack_max_seconds must be positive"),
        (-0.1, ValueError, "cdc.idle_ack_max_seconds must be positive"),
        (float("nan"), ValueError, "cdc.idle_ack_max_seconds must be finite"),
        (float("inf"), ValueError, "cdc.idle_ack_max_seconds must be finite"),
        (10**400, ValueError, "cdc.idle_ack_max_seconds must be finite"),
    ],
)
def test_from_dict_validates_idle_ack_max_seconds(
    val: object, err: type[Exception], match: str
) -> None:
    """Verify idle_ack_max_seconds rejects invalid or non-positive values."""
    with pytest.raises(err, match=match):
        from_dict({"cdc": {"stream_name": "s", "idle_ack_max_seconds": val}})


def test_from_env_parses_environment_variables() -> None:
    """Verify from_env correctly maps environment variables into BridgeConfig."""
    from fdb_kafka_bridge.bridge.config import from_env

    env = {
        "FDB_API_VERSION": "800",
        "FDB_CLUSTER_FILE": "/tmp/test.cluster",
        "FDB_CDC_STREAM_NAME": "env_stream",
        "FDB_CDC_BEGIN_KEY": "61",
        "FDB_CDC_END_KEY": "62",
        "FDB_CDC_MAX_RECORD_BYTES": "500000",
        "FDB_CDC_CURSOR_FILE": "/tmp/cursor.json",
        "FDB_CDC_IDLE_ACK_MAX_REPLIES": "10",
        "FDB_CDC_IDLE_ACK_MAX_SECONDS": "15.5",
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "KAFKA_TOPIC": "env-topic",
        "KAFKA_TRANSACTIONAL_ID": "env-txn",
        "PIPELINE_FLUSH_TIMEOUT_SECONDS": "8.0",
        "PIPELINE_BACKOFF_BASE_SECONDS": "0.2",
        "PIPELINE_BACKOFF_MAX_SECONDS": "3.0",
        "PIPELINE_SHUTDOWN_TIMEOUT_SECONDS": "12.0",
    }
    cfg = from_env(env)
    assert cfg.fdb.api_version == 800
    assert cfg.fdb.cluster_file == "/tmp/test.cluster"
    assert cfg.cdc.stream_name == "env_stream"
    assert cfg.cdc.begin_key == b"a"
    assert cfg.cdc.end_key == b"b"
    assert cfg.cdc.max_record_bytes == 500_000
    assert cfg.cdc.cursor_file == "/tmp/cursor.json"
    assert cfg.cdc.idle_ack_max_replies == 10
    assert cfg.cdc.idle_ack_max_seconds == 15.5
    assert cfg.kafka.bootstrap_servers == "kafka:9092"
    assert cfg.kafka.topic == "env-topic"
    assert cfg.kafka.transactional_id == "env-txn"
    assert cfg.pipeline.flush_timeout_seconds == 8.0
    assert cfg.pipeline.backoff_base_seconds == 0.2
    assert cfg.pipeline.backoff_max_seconds == 3.0
    assert cfg.pipeline.shutdown_timeout_seconds == 12.0
