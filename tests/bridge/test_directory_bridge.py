"""Directory-mode bridge: configuration, frozen resolution, fingerprinted cursors."""

from __future__ import annotations

import json
from collections import namedtuple
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, PropertyMock, create_autospec, patch

import pytest

from fdb_kafka_bridge.bridge.cli import main, parse_config
from fdb_kafka_bridge.bridge.config import from_dict, from_env
from fdb_kafka_bridge.bridge.pipeline import (
    Bridge,
    _load_cursor_from_file,
    _save_cursor_to_file,
)
from fdb_kafka_bridge.cdc.client import FDBClient
from fdb_kafka_bridge.cdc.directory import DirectoryRange, ResolvedDirectoryTree
from fdb_kafka_bridge.cdc.errors import CDCStreamMismatchError
from fdb_kafka_bridge.cdc.listener import FDBMutationListener
from fdb_kafka_bridge.kafka.producer import MutationProducer

STREAM = "app-cdc"
Cursor = namedtuple("Cursor", ["stream_id", "last_consumed_version"])
Mut = namedtuple("Mut", ["type", "param1", "param2"])
Group = namedtuple("Group", ["version", "mutations"])
Reply = namedtuple("Reply", ["mutations", "last_consumed_version"])

TREE = ResolvedDirectoryTree(
    ("app",),
    True,
    (
        DirectoryRange(("app",), b"\x15\x10"),
        DirectoryRange(("app", "users"), b"\x15\x30"),
        DirectoryRange(("app", "orders"), b"\x15\x05"),
    ),
)
OTHER_TREE = ResolvedDirectoryTree(
    ("app",), True, (*TREE.ranges, DirectoryRange(("app", "new"), b"\x15\x50"))
)


def _config(cursor_file: str | None, **cdc: Any) -> Any:
    return from_dict(
        {
            "cdc": {
                "stream_name": STREAM,
                "directory_path": ["app"],
                "directory_recursive": True,
                "cursor_file": cursor_file,
                **cdc,
            }
        }
    )


class _Consumer:
    """Native consumer double: replays replies, records acks."""

    def __init__(self, replies: list[Reply], initial: int = 0) -> None:
        self.replies = list(replies)
        self.current: Reply | None = None
        self.initial = initial
        self.acks: list[int] = []

    def consume(self) -> Any:
        if self.replies:
            reply = self.replies.pop(0)
        else:
            last = self.current.last_consumed_version if self.current else 1000
            reply = Reply([], last + 100)
        self.current = reply
        return MagicMock(wait=MagicMock(return_value=reply))

    def _version(self) -> int:
        return self.current.last_consumed_version if self.current else self.initial

    def acknowledge(self) -> Any:
        self.acks.append(self._version())
        return MagicMock(wait=MagicMock(return_value=None))

    def get_position(self) -> Cursor:
        return Cursor(41, self._version())

    def close(self) -> None:
        pass


def _bridge(
    cursor_file: str | None,
    replies: list[Reply],
    *,
    tree: ResolvedDirectoryTree | None = TREE,
    checkpoint_store: Any = None,
    **cdc: Any,
) -> tuple[Bridge, MagicMock, _Consumer, MagicMock]:
    client = create_autospec(FDBClient, instance=True)
    client.ensure_cdc_stream.return_value = 41
    client.get_read_version.return_value = 10**9
    consumer = _Consumer(replies)
    client.create_cdc_consumer.return_value = consumer

    def _resume(cursor: Any) -> _Consumer:
        consumer.initial = cursor.last_consumed_version
        return consumer

    client.resume_cdc_consumer.side_effect = _resume
    producer = create_autospec(MutationProducer, instance=True)
    type(producer).transactional = PropertyMock(return_value=False)
    bridge = Bridge(
        _config(cursor_file, **cdc),
        client=client,
        producer=producer,
        directory_tree=tree,
        checkpoint_store=checkpoint_store,
    )
    return bridge, client, consumer, producer


def _data_reply(version: int, *keys: bytes) -> Reply:
    return Reply([Group(version, [Mut(0, k, b"v") for k in keys])], version)


def _cursor_json(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text())


# --- configuration -----------------------------------------------------------


def test_from_dict_parses_directory_mode() -> None:
    cfg = _config(None)
    assert cfg.cdc.directory_path == ("app",)
    assert cfg.cdc.directory_recursive is True
    assert cfg.cdc.begin_key == b""


def test_directory_mode_defaults_off() -> None:
    cfg = from_dict({"cdc": {"stream_name": STREAM}})
    assert cfg.cdc.directory_path is None
    assert cfg.cdc.directory_recursive is False


@pytest.mark.parametrize(
    ("cdc", "error", "match"),
    [
        ({"directory_path": "app"}, TypeError, "not a string"),
        ({"directory_path": []}, ValueError, "must not be empty"),
        ({"directory_path": ["a", 1]}, TypeError, "must be str"),
        ({"directory_path": {"a": 1}}, TypeError, "list of path components"),
        (
            {"directory_path": ["a"], "begin_key": "01", "end_key": "02"},
            ValueError,
            "cannot be combined",
        ),
        ({"directory_recursive": True}, ValueError, "requires cdc.directory_path"),
        ({"directory_path": ["a"], "directory_recursive": "yes"}, TypeError, "bool"),
    ],
)
def test_from_dict_rejects_invalid_directory_config(
    cdc: dict[str, Any], error: type[Exception], match: str
) -> None:
    with pytest.raises(error, match=match):
        from_dict({"cdc": {"stream_name": STREAM, **cdc}})


def test_env_uses_json_array_and_strict_bool() -> None:
    env = {
        "FDB_CDC_STREAM_NAME": STREAM,
        "FDB_CDC_DIRECTORY_PATH": '["a/b", "c,d", " e "]',
        "FDB_CDC_DIRECTORY_RECURSIVE": "TRUE",
    }
    cfg = from_env(env)
    assert cfg.cdc.directory_path == ("a/b", "c,d", " e ")
    assert cfg.cdc.directory_recursive is True


@pytest.mark.parametrize(
    ("env", "match"),
    [
        ({"FDB_CDC_DIRECTORY_PATH": "app/users"}, "JSON array"),
        ({"FDB_CDC_DIRECTORY_PATH": '"app"'}, "JSON array"),
        (
            {"FDB_CDC_DIRECTORY_PATH": '["a"]', "FDB_CDC_DIRECTORY_RECURSIVE": "y"},
            "true, false, 1, 0",
        ),
    ],
)
def test_env_rejects_ambiguous_encodings(env: dict[str, str], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        from_env({"FDB_CDC_STREAM_NAME": STREAM, **env})


def test_precedence_file_then_env_then_cli(tmp_path: Path) -> None:
    config_file = tmp_path / "bridge.json"
    config_file.write_text(
        json.dumps({"cdc": {"stream_name": STREAM, "directory_path": ["from_file"]}}),
        encoding="utf-8",
    )
    argv = ["--config", str(config_file)]
    assert parse_config(argv, env={}).cdc.directory_path == ("from_file",)
    env = {"CDC_DIRECTORY_PATH": '["from_env"]', "CDC_DIRECTORY_RECURSIVE": "1"}
    cfg = parse_config(argv, env=env)
    assert cfg.cdc.directory_path == ("from_env",)
    assert cfg.cdc.directory_recursive is True
    cfg = parse_config(
        [
            *argv,
            "--cdc-directory-path",
            '["from", "cli"]',
            "--no-cdc-directory-recursive",
        ],
        env=env,
    )
    assert cfg.cdc.directory_path == ("from", "cli")
    assert cfg.cdc.directory_recursive is False


def test_cli_rejects_non_json_path() -> None:
    with pytest.raises(ValueError, match="JSON array"):
        parse_config(
            ["--cdc-stream-name", STREAM, "--cdc-directory-path", "app"], env={}
        )


def test_single_range_config_is_unchanged() -> None:
    cfg = from_dict(
        {"cdc": {"stream_name": STREAM, "begin_key": "01", "end_key": "02"}}
    )
    assert (cfg.cdc.begin_key, cfg.cdc.end_key) == (b"\x01", b"\x02")
    assert cfg.cdc.directory_path is None


# --- cursor fingerprint persistence ------------------------------------------


def test_save_records_fingerprint_and_load_accepts_match(tmp_path: Path) -> None:
    path = str(tmp_path / "c.json")
    _save_cursor_to_file(
        path, Cursor(41, 7), stream_name=STREAM, directory_fingerprint=TREE.fingerprint
    )
    assert _cursor_json(path) == {
        "stream_id": 41,
        "last_consumed_version": 7,
        "stream_name": STREAM,
        "directory_fingerprint": TREE.fingerprint,
    }
    cursor = _load_cursor_from_file(
        path, stream_name=STREAM, directory_fingerprint=TREE.fingerprint
    )
    assert (cursor.stream_id, cursor.last_consumed_version) == (41, 7)


def test_load_refuses_mismatched_fingerprint_and_keeps_file(tmp_path: Path) -> None:
    path = str(tmp_path / "c.json")
    _save_cursor_to_file(
        path, Cursor(41, 7), stream_name=STREAM, directory_fingerprint=TREE.fingerprint
    )
    before = Path(path).read_bytes()
    with pytest.raises(ValueError, match="Refusing to resume"):
        _load_cursor_from_file(
            path, stream_name=STREAM, directory_fingerprint=OTHER_TREE.fingerprint
        )
    assert Path(path).read_bytes() == before


def test_load_refuses_legacy_cursor_in_directory_mode(tmp_path: Path) -> None:
    path = str(tmp_path / "c.json")
    _save_cursor_to_file(path, Cursor(41, 7), stream_name=STREAM)
    with pytest.raises(ValueError, match="no directory_fingerprint"):
        _load_cursor_from_file(
            path, stream_name=STREAM, directory_fingerprint=TREE.fingerprint
        )
    assert "directory_fingerprint" not in _cursor_json(path)


def test_load_refuses_directory_cursor_in_range_mode(tmp_path: Path) -> None:
    path = str(tmp_path / "c.json")
    _save_cursor_to_file(
        path, Cursor(41, 7), stream_name=STREAM, directory_fingerprint=TREE.fingerprint
    )
    with pytest.raises(ValueError, match="written in directory mode"):
        _load_cursor_from_file(path, stream_name=STREAM)


def test_load_rejects_malformed_fingerprint(tmp_path: Path) -> None:
    path = tmp_path / "c.json"
    path.write_text(
        json.dumps(
            {
                "stream_id": 1,
                "last_consumed_version": 2,
                "stream_name": STREAM,
                "directory_fingerprint": 5,
            }
        )
    )
    with pytest.raises(ValueError, match="must be a string"):
        _load_cursor_from_file(
            str(path), stream_name=STREAM, directory_fingerprint=TREE.fingerprint
        )


def test_legacy_range_cursor_still_loads_in_range_mode(tmp_path: Path) -> None:
    path = tmp_path / "c.json"
    path.write_text(
        json.dumps({"stream_id": 1, "last_consumed_version": 2, "stream_name": STREAM})
    )
    cursor = _load_cursor_from_file(str(path), stream_name=STREAM)
    assert cursor.last_consumed_version == 2


def test_foreign_stream_check_still_applies(tmp_path: Path) -> None:
    path = str(tmp_path / "c.json")
    _save_cursor_to_file(
        path, Cursor(41, 7), stream_name="other", directory_fingerprint=TREE.fingerprint
    )
    with pytest.raises(ValueError, match="belongs to stream"):
        _load_cursor_from_file(
            path, stream_name=STREAM, directory_fingerprint=TREE.fingerprint
        )


# --- bridge execution path ---------------------------------------------------


def test_bridge_registers_tree_ranges_once_with_stable_key(tmp_path: Path) -> None:
    path = str(tmp_path / "c.json")
    reply = _data_reply(500, b"\x15\x05a", b"\x15\x10b", b"\x15\x30c")
    bridge, client, consumer, producer = _bridge(path, [reply])
    bridge.start()
    client.ensure_cdc_stream.assert_called_once_with(STREAM.encode(), TREE.key_ranges)
    client.register_cdc_stream.assert_not_called()

    assert bridge._pipeline is not None
    assert bridge._pipeline.step() == 2  # one batch + its VersionEnd
    keys = {c.kwargs["key"] for c in producer.produce.call_args_list}
    assert keys == {STREAM.encode()}
    assert consumer.acks == [500]
    assert _cursor_json(path)["directory_fingerprint"] == TREE.fingerprint


def test_bridge_resolves_once_and_rebuilds_reuse_frozen_tree(tmp_path: Path) -> None:
    path = str(tmp_path / "c.json")
    resolver_cls = MagicMock()
    resolver_cls.return_value.resolve.side_effect = [TREE, OTHER_TREE]
    with patch("fdb_kafka_bridge.bridge.pipeline.DirectoryCDCResolver", resolver_cls):
        bridge, client, _, _ = _bridge(
            path, [_data_reply(500, b"\x15\x10x")], tree=None
        )
        client.db = MagicMock()
        bridge.start()
        assert bridge._pipeline is not None
        bridge._pipeline.step()
        bridge._rebuild_pipeline()
    resolver_cls.assert_called_once_with(client.db, ("app",), recursive=True)
    assert bridge.directory_tree is TREE
    assert [c.args[1] for c in client.ensure_cdc_stream.call_args_list] == [
        TREE.key_ranges,
        TREE.key_ranges,
    ]
    # The rebuild resumed from the cursor written under the frozen fingerprint.
    client.resume_cdc_consumer.assert_called_once()
    assert _cursor_json(path)["directory_fingerprint"] == TREE.fingerprint


def test_fingerprint_mismatch_refuses_before_native_resume(tmp_path: Path) -> None:
    path = str(tmp_path / "c.json")
    _save_cursor_to_file(
        path, Cursor(41, 7), stream_name=STREAM, directory_fingerprint=TREE.fingerprint
    )
    bridge, client, consumer, _ = _bridge(path, [], tree=OTHER_TREE)
    with pytest.raises(ValueError, match="Refusing to resume"):
        bridge.start()
    client.ensure_cdc_stream.assert_not_called()
    client.resume_cdc_consumer.assert_not_called()
    assert consumer.acks == []


def test_legacy_cursor_refused_in_directory_mode(tmp_path: Path) -> None:
    path = str(tmp_path / "c.json")
    _save_cursor_to_file(path, Cursor(41, 7), stream_name=STREAM)
    bridge, client, _, _ = _bridge(path, [])
    with pytest.raises(ValueError, match="no directory_fingerprint"):
        bridge.start()
    client.resume_cdc_consumer.assert_not_called()


def test_matching_cursor_resumes_with_same_stream_id(tmp_path: Path) -> None:
    path = str(tmp_path / "c.json")
    _save_cursor_to_file(
        path, Cursor(41, 7), stream_name=STREAM, directory_fingerprint=TREE.fingerprint
    )
    bridge, client, consumer, _ = _bridge(path, [])
    bridge.start()
    client.resume_cdc_consumer.assert_called_once()
    assert consumer.acks == [7]  # re-acknowledged restored position


def test_recreated_stream_id_refused_before_resume(tmp_path: Path) -> None:
    path = str(tmp_path / "c.json")
    _save_cursor_to_file(
        path, Cursor(40, 7), stream_name=STREAM, directory_fingerprint=TREE.fingerprint
    )
    bridge, client, _, _ = _bridge(path, [])
    with pytest.raises(CDCStreamMismatchError):
        bridge.start()
    client.resume_cdc_consumer.assert_not_called()


def test_checkpoint_failure_blocks_ack_in_directory_mode(tmp_path: Path) -> None:
    path = str(tmp_path / "c.json")
    bridge, _, consumer, _ = _bridge(path, [_data_reply(500, b"\x15\x10x")])
    bridge.start()
    assert bridge._pipeline is not None
    with patch(
        "fdb_kafka_bridge.bridge.pipeline._save_cursor_to_file",
        side_effect=OSError("disk full"),
    ):
        with pytest.raises(OSError):
            bridge._pipeline.step()
    assert consumer.acks == []
    assert not Path(path).exists()


def test_coalesced_idle_flush_persists_fingerprint(tmp_path: Path) -> None:
    path = str(tmp_path / "c.json")
    bridge, _, consumer, _ = _bridge(
        path, [Reply([], 100), Reply([], 200)], idle_ack_max_replies=2
    )
    bridge.start()
    assert bridge._pipeline is not None
    assert bridge._pipeline.step() == 0  # withheld: no produce, checkpoint, ack
    assert consumer.acks == [] and not Path(path).exists()
    assert bridge._pipeline.step() == 1
    assert consumer.acks == [200]
    data = _cursor_json(path)
    assert data["last_consumed_version"] == 200
    assert data["directory_fingerprint"] == TREE.fingerprint


def test_shutdown_flush_persists_fingerprint(tmp_path: Path) -> None:
    path = str(tmp_path / "c.json")
    bridge, _, consumer, _ = _bridge(path, [Reply([], 300)], idle_ack_max_replies=5)
    bridge.start()
    assert bridge._pipeline is not None
    assert bridge._pipeline.step() == 0
    bridge.stop()
    assert consumer.acks == [300]
    assert _cursor_json(path)["directory_fingerprint"] == TREE.fingerprint


class _MemoryStore:
    """A caller-supplied durable store double recording every call."""

    def __init__(self, refuse: bool = False) -> None:
        self.saved: list[tuple[int, str, str | None]] = []
        self.loads: list[tuple[str, str | None]] = []
        self.refuse = refuse

    def load(self, *, stream_name: str, directory_fingerprint: str | None) -> Any:
        self.loads.append((stream_name, directory_fingerprint))
        if self.refuse:
            raise ValueError("stored fingerprint differs")
        if not self.saved:
            return None
        return Cursor(41, self.saved[-1][0])

    def save(
        self, position: Any, *, stream_name: str, directory_fingerprint: str | None
    ) -> None:
        self.saved.append(
            (position.last_consumed_version, stream_name, directory_fingerprint)
        )


def test_directory_mode_requires_durable_checkpoint() -> None:
    client = create_autospec(FDBClient, instance=True)
    with pytest.raises(ValueError, match="needs durable checkpoints"):
        Bridge(_config(None), client=client, directory_tree=TREE)
    assert client.method_calls == []


def test_directory_daemon_cli_without_cursor_file_is_config_error() -> None:
    with patch("fdb_kafka_bridge.bridge.pipeline.FDBClient") as fdb_client:
        code = main(["--cdc-stream-name", STREAM, "--cdc-directory-path", '["app"]'])
    assert code == 1
    fdb_client.assert_not_called()


def test_checkpoint_store_and_cursor_file_are_exclusive(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not both"):
        Bridge(
            _config(str(tmp_path / "c.json")),
            client=MagicMock(),
            checkpoint_store=_MemoryStore(),
        )


def test_range_mode_without_cursor_file_is_unchanged() -> None:
    cfg = from_dict(
        {"cdc": {"stream_name": STREAM, "begin_key": "01", "end_key": "02"}}
    )
    assert Bridge(cfg, client=MagicMock()) is not None


def test_supplied_store_receives_fingerprint_on_every_delivery_path() -> None:
    store = _MemoryStore()
    replies = [_data_reply(500, b"\x15\x10x"), Reply([], 600), Reply([], 700)]
    bridge, _, consumer, _ = _bridge(
        None, replies, checkpoint_store=store, idle_ack_max_replies=2
    )
    bridge.start()
    assert store.loads == [(STREAM, TREE.fingerprint)]
    assert bridge._pipeline is not None
    assert bridge._pipeline.step() == 2  # data reply
    assert bridge._pipeline.step() == 0  # idle, withheld
    assert bridge._pipeline.step() == 1  # coalesced flush at the threshold
    assert consumer.acks == [500, 700]
    assert store.saved == [
        (500, STREAM, TREE.fingerprint),
        (700, STREAM, TREE.fingerprint),
    ]


def test_supplied_store_shutdown_flush_carries_fingerprint() -> None:
    store = _MemoryStore()
    bridge, _, consumer, _ = _bridge(
        None, [Reply([], 300)], checkpoint_store=store, idle_ack_max_replies=5
    )
    bridge.start()
    assert bridge._pipeline is not None
    assert bridge._pipeline.step() == 0
    bridge.stop()
    assert consumer.acks == [300]
    assert store.saved == [(300, STREAM, TREE.fingerprint)]


def test_supplied_store_refusal_blocks_resume() -> None:
    bridge, client, consumer, _ = _bridge(
        None, [], checkpoint_store=_MemoryStore(refuse=True)
    )
    with pytest.raises(ValueError, match="stored fingerprint differs"):
        bridge.start()
    client.ensure_cdc_stream.assert_not_called()
    client.resume_cdc_consumer.assert_not_called()
    assert consumer.acks == []


def test_injected_tree_must_match_config() -> None:
    with pytest.raises(ValueError, match="does not match"):
        Bridge(
            _config(None, directory_recursive=False),
            client=MagicMock(),
            directory_tree=TREE,
        )
    with pytest.raises(ValueError, match="requires cdc.directory_path"):
        Bridge(from_dict({"cdc": {"stream_name": STREAM}}), directory_tree=TREE)


def test_injected_listener_must_use_tree_ranges(tmp_path: Path) -> None:
    client = create_autospec(FDBClient, instance=True)
    listener = FDBMutationListener(client, STREAM, key_range=(b"\x15", b"\x16"))
    producer = create_autospec(MutationProducer, instance=True)
    type(producer).transactional = PropertyMock(return_value=False)
    bridge = Bridge(
        _config(str(tmp_path / "c.json")),
        client=client,
        listener=listener,
        producer=producer,
        directory_tree=TREE,
    )
    with pytest.raises(ValueError, match="key_ranges=directory_tree.key_ranges"):
        bridge.start()


def test_run_treats_stream_mismatch_as_permanent(tmp_path: Path) -> None:
    bridge, client, _, _ = _bridge(str(tmp_path / "c.json"), [Reply([], 100)])
    bridge.start()
    assert bridge._pipeline is not None
    with patch.object(
        bridge._pipeline,
        "step",
        side_effect=CDCStreamMismatchError("stream ranges differ"),
    ):
        with pytest.raises(CDCStreamMismatchError):
            bridge.run()
    assert bridge.metrics.errors_retried == 0
