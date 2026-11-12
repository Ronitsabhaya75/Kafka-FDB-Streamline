"""Production CLI configuration, supervisor codes and signal-driven cleanup."""

import os
import signal
import threading
from unittest.mock import MagicMock, patch

import pytest

from fdb_kafka_bridge.bridge.cli import load_config, main, run_daemon
from fdb_kafka_bridge.bridge.config import from_dict
from fdb_kafka_bridge.bridge.startup import StartupError
from fdb_kafka_bridge.cdc.checkpoint import (
    CheckpointError,
    CheckpointFencedError,
    CheckpointRegressionError,
    CheckpointSinkMismatchError,
    CheckpointWriteError,
)
from fdb_kafka_bridge.cdc.errors import CDCInvalidCursorError, CDCTerminalError


def test_config_defaults_and_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STREAMLINE_STREAM_NAME", "orders")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "env:9092")
    cfg = load_config(
        ["--kafka-bootstrap-servers", "flag:9092", "--subspace-prefix", "0x00ff"]
    )
    assert cfg.kafka.topic == "fdb.cdc.orders"
    assert cfg.kafka.bootstrap_servers == "flag:9092"
    assert cfg.cdc.begin_key == b"\x00\xff"
    assert cfg.cdc.end_key == b"\x01"
    assert cfg.cdc.checkpoint_path == ("fdb-kafka-bridge", "checkpoints")
    cfg = load_config(
        [
            "--topic",
            "custom",
            "--transactional-id",
            "tx",
            "--subspace-prefix",
            "orders:",
        ]
    )
    assert cfg.kafka.topic == "custom" and cfg.kafka.transactional_id == "tx"
    assert cfg.cdc.begin_key == b"orders:"


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--stream-name", ""],
        ["--stream-name", "s", "--subspace-prefix", "0xgg"],
        ["--stream-name", "s", "--subspace-prefix", "0xffff"],
        ["--stream-name", "s", "--checkpoint-path", "a//b"],
        ["--stream-name", "s", "--flush-timeout-s", "nan"],
        ["--stream-name", "s", "--shutdown-timeout-s", "inf"],
        ["--stream-name", "s", "--topic", "a/b"],
        ["--stream-name", "s", "--max-record-bytes", "0"],
        ["--stream-name", "s", "--checkpoint-retry-limit", "-1"],
    ],
)
def test_invalid_flags(args: list[str]) -> None:
    with pytest.raises(SystemExit) as error:
        load_config(args)
    assert error.value.code == 2


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (None, 0),
        (RuntimeError("broker"), 1),
        (StartupError("range"), 3),
        (CheckpointSinkMismatchError("sink"), 3),
        (CheckpointRegressionError("older"), 3),
        (CheckpointFencedError("owner changed"), 3),
        (CheckpointError("permanent"), 3),
        (CheckpointWriteError("transient"), 1),
        (CDCInvalidCursorError("invalid"), 3),
        (CDCTerminalError("old", code=1007), 3),
        (CDCTerminalError("collision", code=2000), 3),
    ],
)
def test_exit_codes(error: Exception | None, code: int) -> None:
    with patch("fdb_kafka_bridge.bridge.cli.run_daemon", side_effect=error):
        assert main(["--stream-name", "s"]) == code


def test_worker_failure_cleanup_and_signal_restore() -> None:
    bridge = MagicMock()
    bridge.config = from_dict({"cdc": {"stream_name": "s"}})
    bridge.run.side_effect = RuntimeError("worker failed")
    previous = signal.getsignal(signal.SIGTERM)
    with pytest.raises(RuntimeError, match="worker failed"):
        run_daemon(bridge)
    bridge.stop.assert_called_once()
    assert signal.getsignal(signal.SIGTERM) == previous


def test_start_failure_cleanup() -> None:
    bridge = MagicMock()
    bridge.config = from_dict({"cdc": {"stream_name": "s"}})
    bridge.start.side_effect = RuntimeError("start failed")
    with pytest.raises(RuntimeError, match="start failed"):
        run_daemon(bridge)
    bridge.stop.assert_called_once()


def test_sigterm_requests_shutdown_and_cleans_up() -> None:
    bridge = MagicMock()
    bridge.config = from_dict({"cdc": {"stream_name": "s"}})
    stopped = threading.Event()
    bridge.request_shutdown.side_effect = stopped.set
    bridge.stop.side_effect = lambda **_: stopped.set()

    def run() -> None:
        os.kill(os.getpid(), signal.SIGTERM)
        assert stopped.wait(5)

    bridge.run.side_effect = run
    previous = signal.getsignal(signal.SIGTERM)
    run_daemon(bridge)
    bridge.request_shutdown.assert_called_once()
    bridge.stop.assert_called_once()
    assert signal.getsignal(signal.SIGTERM) == previous


def test_cli_operation_timeout_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CDC_PROXY_CONSUME_POLL_TIMEOUT", "2")
    monkeypatch.setenv("STREAMLINE_ACK_TIMEOUT_S", "3")
    cfg = load_config(["--stream-name", "s"])
    assert cfg.cdc.proxy_consume_poll_timeout_seconds == 2
    assert cfg.cdc.ack_timeout_seconds == 3
    cfg = load_config(
        [
            "--stream-name",
            "s",
            "--ack-timeout-s",
            "1",
            "--proxy-consume-poll-timeout-s",
            "4",
        ]
    )
    assert cfg.cdc.ack_timeout_seconds == 1
    assert cfg.cdc.proxy_consume_poll_timeout_seconds == 4
    assert cfg.pipeline.shutdown_timeout_seconds < 30


def test_signal_during_startup_uses_total_join_deadline() -> None:
    import time

    bridge = MagicMock()
    bridge.config = from_dict(
        {"cdc": {"stream_name": "s"}, "pipeline": {"shutdown_timeout_seconds": 0.05}}
    )
    release = threading.Event()

    def start() -> None:
        os.kill(os.getpid(), signal.SIGTERM)
        release.wait(2)

    bridge.start.side_effect = start
    previous = signal.getsignal(signal.SIGTERM)
    began = time.monotonic()
    try:
        with pytest.raises(RuntimeError, match="shutdown budget"):
            run_daemon(bridge)
        assert time.monotonic() - began < 0.3
        assert signal.getsignal(signal.SIGTERM) == previous
    finally:
        release.set()
