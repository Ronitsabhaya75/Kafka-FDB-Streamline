"""Live test: resolve a Directory Layer tree into one multi-range CDC stream.

Runs only with ``FDB_CDC_INTEGRATION=1`` against a CDC-enabled FoundationDB 8.0
cluster (the CI ``test-cdc-mutations`` job). Missing multi-range support fails
this test; it is never skipped for that reason.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

import pytest

from fdb_kafka_bridge.bridge.pipeline import (
    _load_cursor_from_file,
    _save_cursor_to_file,
)
from fdb_kafka_bridge.cdc.client import FDBClient
from fdb_kafka_bridge.cdc.directory import DirectoryCDCResolver
from fdb_kafka_bridge.cdc.errors import CDCStreamMismatchError
from fdb_kafka_bridge.cdc.listener import FDBMutationListener, strinc

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("FDB_CDC_INTEGRATION") != "1",
        reason="set FDB_CDC_INTEGRATION=1 to run live FoundationDB tests",
    ),
]

MAX_POLLS = 30


def _poll_keys(listener: FDBMutationListener, wanted: set[bytes]) -> set[bytes]:
    """Poll and ack until every key in ``wanted`` arrived; return all keys seen."""
    seen: set[bytes] = set()
    for _ in range(MAX_POLLS):
        records = listener.poll_records()
        for record in records:
            if record.HasField("batch"):
                for mutation in record.batch.mutations:
                    if mutation.WhichOneof("mutation") == "single_key_mutation":
                        seen.add(mutation.single_key_mutation.key)
        if records:
            listener.acknowledge()
        if wanted <= seen:
            return seen
    pytest.fail(f"missing keys after {MAX_POLLS} polls: {sorted(wanted - seen)!r}")


def _commit(db: Any, writes: dict[bytes, bytes]) -> None:
    """Commit every write in one transaction, so all share a commit version."""
    tr = db.create_transaction()
    for key, value in writes.items():
        tr[key] = value
    tr.commit().wait()


def test_live_directory_tree_registers_one_multi_range_stream(tmp_path: Path) -> None:
    """Resolve, register once, capture only the tree, then resume safely."""
    import fdb

    client = FDBClient(api_version=800)
    db = client.open()
    run = uuid.uuid4().hex[:12].encode()
    base = b"integ_dircdc_" + run + b"/"
    stream_name = "integ_dircdc_" + run.decode()
    # An isolated Directory Layer under ``base`` with manual prefixes, so the
    # unrelated directory sits between watched ones and a single enclosing
    # interval would capture it.
    layer = fdb.DirectoryLayer(
        node_subspace=fdb.Subspace(rawPrefix=base + b"\xfe"),
        content_subspace=fdb.Subspace(rawPrefix=base),
        allow_manual_prefixes=True,
    )
    prefixes = {
        ("app",): base + b"\x10",
        ("other",): base + b"\x20",
        ("app", "users"): base + b"\x30",
        ("app", "users", "archive"): base + b"\x40",
        ("app", "orders"): base + b"\x05",
    }
    try:
        for path, prefix in prefixes.items():
            layer.create(db, path, prefix=prefix)

        tree = DirectoryCDCResolver(
            db, ("app",), recursive=True, directory_layer=layer
        ).resolve()
        assert tree.paths == (
            ("app", "orders"),
            ("app",),
            ("app", "users"),
            ("app", "users", "archive"),
        )
        for item in tree.ranges:
            assert item.key_range == (prefixes[item.path], strinc(prefixes[item.path]))

        root_key = prefixes[("app",)]  # a key equal to the raw prefix
        expected = {
            root_key: ("app",),
            root_key + b"r": ("app",),
            prefixes[("app", "users")] + b"u": ("app", "users"),
            prefixes[("app", "users", "archive")] + b"a": ("app", "users", "archive"),
            prefixes[("app", "orders")] + b"o": ("app", "orders"),
        }
        excluded = {
            prefixes[("other",)] + b"x",  # unrelated directory in a gap
            strinc(root_key),  # first key past the root's range
            base + b"\x00gap",  # below every watched range
        }

        cursor_file = str(tmp_path / "cursor.json")
        with FDBMutationListener(
            client, stream_name, key_ranges=tree.key_ranges
        ) as listener:
            info = client.get_cdc_stream(stream_name)
            assert info is not None
            assert listener.stream_id == info.stream_id
            listed = tuple((r.begin_key, r.end_key) for r in info.ranges)
            assert listed == tree.key_ranges  # one stream, every range, no merge

            _commit(db, {key: b"v" for key in [*expected, *excluded]})
            seen = _poll_keys(listener, set(expected))
            assert seen.isdisjoint(excluded)
            for key, path in expected.items():
                assert tree.path_for_key(key) == path
            for key in excluded:
                assert tree.path_for_key(key) is None

            _save_cursor_to_file(
                cursor_file,
                listener.get_position(),
                stream_name=stream_name,
                directory_fingerprint=tree.fingerprint,
            )

        later = prefixes[("app", "users")] + b"after_resume"
        _commit(db, {later: b"v"})

        # A new process rediscovers the unchanged tree and resumes.
        again = DirectoryCDCResolver(
            db, ("app",), recursive=True, directory_layer=layer
        ).resolve()
        assert again.fingerprint == tree.fingerprint
        cursor = _load_cursor_from_file(
            cursor_file,
            stream_name=stream_name,
            directory_fingerprint=again.fingerprint,
        )
        with FDBMutationListener(
            client, stream_name, key_ranges=again.key_ranges, cursor=cursor
        ) as resumed:
            assert later in _poll_keys(resumed, {later})

        # After the tree grows, the checkpoint and the native stream both refuse.
        layer.create(db, ("app", "new"), prefix=base + b"\x50")
        grown = DirectoryCDCResolver(
            db, ("app",), recursive=True, directory_layer=layer
        ).resolve()
        assert grown.fingerprint != tree.fingerprint
        with pytest.raises(ValueError, match="Refusing to resume"):
            _load_cursor_from_file(
                cursor_file,
                stream_name=stream_name,
                directory_fingerprint=grown.fingerprint,
            )
        with pytest.raises(CDCStreamMismatchError):
            client.ensure_cdc_stream(stream_name, grown.key_ranges)
    finally:
        client.remove_cdc_stream(stream_name)
        del db[base : strinc(base)]
