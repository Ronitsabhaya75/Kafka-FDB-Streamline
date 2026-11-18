"""Unit tests for the Directory Layer resolver, resolved tree, and fingerprint."""

from __future__ import annotations

import hashlib
import json
from typing import Any

import pytest

from fdb_kafka_bridge.cdc.directory import (
    DirectoryCDCResolver,
    DirectoryRange,
    ResolvedDirectoryTree,
    directory_fingerprint,
    validate_directory_path,
)
from fdb_kafka_bridge.cdc.errors import (
    CDCInvalidRangeError,
    DirectoryNotFoundError,
    DirectoryPartitionError,
    DirectoryResolutionError,
)
from fdb_kafka_bridge.cdc.listener import strinc


class _FakeFDBError(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(f"fdb error {code}")
        self.code = code


class _FakeDirectory:
    """Mimics DirectorySubspace / DirectoryPartition for the resolver."""

    def __init__(self, prefix: bytes, layer: bytes = b"") -> None:
        self._prefix = prefix
        self._layer = layer

    def key(self) -> bytes:
        if self._layer == b"partition":
            raise Exception("Cannot get key for the root of a directory partition.")
        return self._prefix

    def get_layer(self) -> bytes:
        return self._layer


class _FakeLayer:
    """An in-memory Directory Layer keyed by full path.

    ``order`` reverses child listings to model a different enumeration order.
    Every call records the transaction it was given.
    """

    def __init__(
        self,
        tree: dict[tuple[str, ...], bytes | tuple[bytes, bytes]],
        *,
        reverse_listing: bool = False,
    ) -> None:
        self.tree = tree
        self.reverse_listing = reverse_listing
        self.transactions: list[Any] = []
        self.fail_exists: list[BaseException] = []
        self.created: list[tuple[str, ...]] = []

    def _entry(self, path: tuple[str, ...]) -> tuple[bytes, bytes]:
        value = self.tree[path]
        return value if isinstance(value, tuple) else (value, b"")

    def exists(self, tr: Any, path: tuple[str, ...]) -> bool:
        self.transactions.append(tr)
        if self.fail_exists:
            raise self.fail_exists.pop(0)
        return path in self.tree

    def open(self, tr: Any, path: tuple[str, ...]) -> _FakeDirectory:
        self.transactions.append(tr)
        if path not in self.tree:
            raise ValueError("The directory does not exist.")
        prefix, layer = self._entry(path)
        return _FakeDirectory(prefix, layer)

    def list(self, tr: Any, path: tuple[str, ...]) -> list[str]:
        self.transactions.append(tr)
        names = [p[-1] for p in self.tree if len(p) == len(path) + 1 and p[:-1] == path]
        return list(reversed(names)) if self.reverse_listing else names

    def create_or_open(self, tr: Any, path: tuple[str, ...]) -> None:
        self.created.append(path)
        raise AssertionError("the resolver must never create directories")


class _FakeTransaction:
    def __init__(self) -> None:
        self.on_error_calls: list[BaseException] = []
        self.on_error_raises: BaseException | None = None

    def on_error(self, exc: BaseException) -> Any:
        self.on_error_calls.append(exc)
        outer = self

        class _Future:
            def wait(self) -> None:
                if outer.on_error_raises is not None:
                    raise outer.on_error_raises

        return _Future()


class _FakeDB:
    def __init__(self) -> None:
        self.transactions: list[_FakeTransaction] = []

    def create_transaction(self) -> _FakeTransaction:
        tr = _FakeTransaction()
        self.transactions.append(tr)
        return tr


# Prefixes deliberately interleave with an unrelated directory ("other") so a
# single enclosing interval would capture it.
_TREE: dict[tuple[str, ...], bytes | tuple[bytes, bytes]] = {
    ("app",): b"\x15\x10",
    ("app", "users"): b"\x15\x30",
    ("app", "orders"): b"\x15\x05",
    ("app", "users", "archive"): b"\x15\x40",
    ("other",): b"\x15\x20",
}


def _resolve(
    tree: dict[tuple[str, ...], bytes | tuple[bytes, bytes]] = _TREE,
    root: tuple[str, ...] = ("app",),
    *,
    recursive: bool = True,
    reverse_listing: bool = False,
) -> ResolvedDirectoryTree:
    layer = _FakeLayer(tree, reverse_listing=reverse_listing)
    return DirectoryCDCResolver(
        _FakeDB(), root, recursive=recursive, directory_layer=layer
    ).resolve()


# --- path validation ---------------------------------------------------------


@pytest.mark.parametrize(
    "path",
    [("app",), ("app", "users"), ("", "x"), ("a/b", "c,d", "sp ace", "ünï")],
)
def test_valid_paths_are_accepted(path: tuple[str, ...]) -> None:
    assert validate_directory_path(path) == path


def test_plain_string_path_is_rejected() -> None:
    with pytest.raises(TypeError, match="not a plain string"):
        validate_directory_path("app")


@pytest.mark.parametrize("path", [["app"], None, b"app", 3])
def test_non_tuple_paths_are_rejected(path: object) -> None:
    with pytest.raises(TypeError):
        validate_directory_path(path)


def test_empty_path_is_rejected() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        validate_directory_path(())


@pytest.mark.parametrize("component", [b"app", 1, None, ("a",)])
def test_non_str_components_are_rejected(component: object) -> None:
    with pytest.raises(TypeError, match=r"root_path\[1\] must be str"):
        validate_directory_path(("ok", component))


def test_unencodable_component_is_rejected() -> None:
    with pytest.raises(ValueError, match="UTF-8"):
        validate_directory_path(("bad\ud800",))


def test_resolver_validates_path_before_any_read() -> None:
    db = _FakeDB()
    with pytest.raises(TypeError):
        DirectoryCDCResolver(db, "app", directory_layer=_FakeLayer({}))  # type: ignore[arg-type]
    assert db.transactions == []


# --- discovery ---------------------------------------------------------------


def test_missing_root_fails_without_creating() -> None:
    layer = _FakeLayer({})
    resolver = DirectoryCDCResolver(_FakeDB(), ("missing",), directory_layer=layer)
    with pytest.raises(DirectoryNotFoundError, match="does not exist"):
        resolver.resolve()
    assert layer.created == []


def test_existing_root_without_children_or_data_resolves_to_its_prefix() -> None:
    tree = _resolve({("empty",): b"\x15\x01"}, ("empty",))
    assert tree.paths == (("empty",),)
    assert tree.key_ranges == ((b"\x15\x01", b"\x15\x02"),)


def test_non_recursive_resolves_only_the_root() -> None:
    tree = _resolve(recursive=False)
    assert tree.paths == (("app",),)
    assert tree.key_ranges == ((b"\x15\x10", b"\x15\x11"),)
    assert tree.recursive is False


def test_recursive_includes_nested_and_sibling_non_contiguous_prefixes() -> None:
    tree = _resolve()
    # Sorted by raw prefix bytes, not by path.
    assert tree.paths == (
        ("app", "orders"),
        ("app",),
        ("app", "users"),
        ("app", "users", "archive"),
    )
    assert tree.key_ranges == (
        (b"\x15\x05", b"\x15\x06"),
        (b"\x15\x10", b"\x15\x11"),
        (b"\x15\x30", b"\x15\x31"),
        (b"\x15\x40", b"\x15\x41"),
    )
    # The unrelated directory sits in a gap and is not covered.
    assert tree.lookup(b"\x15\x20") is None
    assert tree.lookup(b"\x15\x20payload") is None


def test_output_is_deterministic_across_child_enumeration_orders() -> None:
    forward = _resolve()
    backward = _resolve(reverse_listing=True)
    assert forward == backward
    assert forward.ranges == backward.ranges
    assert forward.fingerprint == backward.fingerprint


def test_discovery_uses_one_transaction() -> None:
    layer = _FakeLayer(_TREE)
    db = _FakeDB()
    DirectoryCDCResolver(db, ("app",), recursive=True, directory_layer=layer).resolve()
    assert len(db.transactions) == 1
    assert {id(tr) for tr in layer.transactions} == {id(db.transactions[0])}


def test_deep_tree_does_not_hit_recursion_limit() -> None:
    depth = 1000
    tree: dict[tuple[str, ...], bytes | tuple[bytes, bytes]] = {}
    path: tuple[str, ...] = ()
    for i in range(depth):
        path = (*path, f"d{i}")
        tree[path] = b"\x15" + i.to_bytes(2, "big")
    resolved = _resolve(tree, ("d0",))
    assert len(resolved.ranges) == depth


def test_tree_larger_than_native_range_limit_is_rejected() -> None:
    tree: dict[tuple[str, ...], bytes | tuple[bytes, bytes]] = {("r",): b"\x16"}
    for i in range(1024):
        tree[("r", f"c{i}")] = b"\x15" + i.to_bytes(2, "big")
    with pytest.raises(DirectoryResolutionError, match="more than 1024"):
        _resolve(tree, ("r",))


def test_root_partition_is_rejected() -> None:
    tree = {("part",): (b"\x15\x09", b"partition")}
    with pytest.raises(DirectoryPartitionError, match="partition"):
        _resolve(tree, ("part",))


def test_descendant_partition_is_rejected_not_skipped() -> None:
    tree = {**_TREE, ("app", "tenant"): (b"\x15\x50", b"partition")}
    with pytest.raises(DirectoryPartitionError, match=r"\('app', 'tenant'\)"):
        _resolve(tree)


def test_partition_below_root_is_ignored_in_non_recursive_mode() -> None:
    tree = {**_TREE, ("app", "tenant"): (b"\x15\x50", b"partition")}
    assert _resolve(tree, recursive=False).paths == (("app",),)


def test_overlapping_directory_prefixes_are_rejected() -> None:
    tree = {("app",): b"\x15\x10", ("app", "x"): b"\x15\x10\x01"}
    with pytest.raises(CDCInvalidRangeError, match="overlap"):
        _resolve(tree)


def test_non_str_child_name_is_rejected() -> None:
    class _BadLayer(_FakeLayer):
        def list(self, tr: Any, path: tuple[str, ...]) -> list[Any]:
            return [b"raw"]

    resolver = DirectoryCDCResolver(
        _FakeDB(), ("app",), recursive=True, directory_layer=_BadLayer(_TREE)
    )
    with pytest.raises(DirectoryResolutionError, match="non-str name"):
        resolver.resolve()


def test_retryable_fdb_error_restarts_discovery_on_the_same_transaction() -> None:
    layer = _FakeLayer(_TREE)
    layer.fail_exists = [_FakeFDBError(1007)]
    db = _FakeDB()
    tree = DirectoryCDCResolver(
        db, ("app",), recursive=True, directory_layer=layer
    ).resolve()
    assert len(tree.ranges) == 4
    assert len(db.transactions) == 1
    assert [e.code for e in db.transactions[0].on_error_calls] == [1007]


def test_discovery_gives_up_after_max_attempts() -> None:
    layer = _FakeLayer(_TREE)
    layer.fail_exists = [_FakeFDBError(1007) for _ in range(3)]
    resolver = DirectoryCDCResolver(
        _FakeDB(), ("app",), directory_layer=layer, max_attempts=3
    )
    with pytest.raises(DirectoryResolutionError, match="consistent snapshot"):
        resolver.resolve()


def test_non_retryable_fdb_error_propagates_from_on_error() -> None:
    layer = _FakeLayer(_TREE)
    fatal = _FakeFDBError(2000)
    layer.fail_exists = [fatal]
    db = _FakeDB()
    original = db.create_transaction

    def _create() -> _FakeTransaction:
        tr = original()
        tr.on_error_raises = fatal
        return tr

    db.create_transaction = _create  # type: ignore[method-assign]
    with pytest.raises(_FakeFDBError):
        DirectoryCDCResolver(db, ("app",), directory_layer=layer).resolve()


def test_non_fdb_error_is_not_retried() -> None:
    layer = _FakeLayer(_TREE)
    layer.fail_exists = [RuntimeError("boom")]
    db = _FakeDB()
    with pytest.raises(RuntimeError, match="boom"):
        DirectoryCDCResolver(db, ("app",), directory_layer=layer).resolve()
    assert db.transactions[0].on_error_calls == []


# --- prefixes, ranges, lookup ------------------------------------------------


@pytest.mark.parametrize(
    ("prefix", "end"),
    [(b"\x15\x10", b"\x15\x11"), (b"\x15\xff", b"\x16"), (b"\xfe\xff", b"\xff")],
)
def test_directory_range_uses_prefix_successor(prefix: bytes, end: bytes) -> None:
    item = DirectoryRange(("a",), prefix)
    assert item.key_range == (prefix, end) == (prefix, strinc(prefix))


@pytest.mark.parametrize(
    ("prefix", "error"),
    [
        (b"", CDCInvalidRangeError),
        (b"\xff", CDCInvalidRangeError),
        (b"\xff\x02", CDCInvalidRangeError),
        ("abc", TypeError),
    ],
)
def test_directory_range_rejects_invalid_prefixes(
    prefix: object, error: type[Exception]
) -> None:
    with pytest.raises(error):
        DirectoryRange(("a",), prefix)  # type: ignore[arg-type]


def test_lookup_respects_half_open_boundaries() -> None:
    tree = _resolve()
    assert tree.path_for_key(b"\x15\x10") == ("app",)  # key equal to prefix
    assert tree.path_for_key(b"\x15\x10\xff\xff") == ("app",)
    assert tree.path_for_key(b"\x15\x11") is None  # end is exclusive
    assert tree.path_for_key(b"\x15\x30\x00") == ("app", "users")
    assert tree.path_for_key(b"\x15\x40") == ("app", "users", "archive")
    assert tree.path_for_key(b"\x15\x41") is None


def test_lookup_outside_all_ranges_returns_none() -> None:
    tree = _resolve()
    assert tree.lookup(b"") is None
    assert tree.lookup(b"\x00") is None
    assert tree.lookup(b"\x15\x04\xff") is None
    assert tree.lookup(b"\xfe") is None


def test_lookup_rejects_non_bytes_key() -> None:
    with pytest.raises(TypeError):
        _resolve().lookup("app")  # type: ignore[arg-type]


def test_resolved_tree_is_immutable() -> None:
    tree = _resolve()
    with pytest.raises(AttributeError):
        tree.recursive = False  # type: ignore[misc]


@pytest.mark.parametrize(
    ("ranges", "match"),
    [
        ((DirectoryRange(("x",), b"\x01"),), "include the root"),
        (
            (DirectoryRange(("app",), b"\x01"), DirectoryRange(("app",), b"\x02")),
            "unique",
        ),
        (
            (DirectoryRange(("app",), b"\x01"), DirectoryRange(("other",), b"\x02")),
            "outside root",
        ),
    ],
)
def test_tree_rejects_inconsistent_results(
    ranges: tuple[DirectoryRange, ...], match: str
) -> None:
    with pytest.raises(DirectoryResolutionError, match=match):
        ResolvedDirectoryTree(("app",), True, ranges)


def test_non_recursive_tree_rejects_descendants() -> None:
    ranges = (DirectoryRange(("app",), b"\x01"), DirectoryRange(("app", "x"), b"\x02"))
    with pytest.raises(DirectoryResolutionError, match="non-recursive"):
        ResolvedDirectoryTree(("app",), False, ranges)


# --- fingerprint -------------------------------------------------------------


def test_fingerprint_matches_documented_canonical_form() -> None:
    tree = _resolve(recursive=False)
    canonical = json.dumps(
        {
            "format": 1,
            "kind": "fdb-kafka-bridge/directory-cdc",
            "ranges": [{"begin": "1510", "end": "1511", "path": ["app"]}],
            "recursive": False,
            "root_path": ["app"],
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    expected = "v1:sha256:" + hashlib.sha256(canonical.encode()).hexdigest()
    assert tree.fingerprint == expected


def test_fingerprint_is_independent_of_input_order() -> None:
    items = [DirectoryRange(("app",), b"\x02"), DirectoryRange(("app", "b"), b"\x01")]
    assert directory_fingerprint(("app",), True, items) == directory_fingerprint(
        ("app",), True, list(reversed(items))
    )


@pytest.mark.parametrize(
    "change",
    ["root", "recursive", "path", "prefix", "extra_child"],
)
def test_fingerprint_changes_with_configuration(change: str) -> None:
    base_ranges = [DirectoryRange(("app",), b"\x10")]
    base = directory_fingerprint(("app",), True, base_ranges)
    if change == "root":
        other = directory_fingerprint(
            ("app2",), True, [DirectoryRange(("app2",), b"\x10")]
        )
    elif change == "recursive":
        other = directory_fingerprint(("app",), False, base_ranges)
    elif change == "path":
        other = directory_fingerprint(
            ("app",), True, [*base_ranges, DirectoryRange(("app", "y"), b"\x20")]
        )
        base = directory_fingerprint(
            ("app",), True, [*base_ranges, DirectoryRange(("app", "x"), b"\x20")]
        )
    elif change == "prefix":
        other = directory_fingerprint(
            ("app",), True, [DirectoryRange(("app",), b"\x11")]
        )
    else:
        other = directory_fingerprint(
            ("app",), True, [*base_ranges, DirectoryRange(("app", "x"), b"\x20")]
        )
    assert other != base
    assert other.startswith("v1:sha256:")
