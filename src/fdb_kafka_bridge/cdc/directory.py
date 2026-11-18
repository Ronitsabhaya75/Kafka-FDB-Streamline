"""Resolve a Directory Layer tree into the key ranges of one multi-range stream.

CDC tracks byte ranges, not paths. Every directory owns its own allocated
prefix, and a child's prefix is independent of its parent's, so a recursive
directory stream is a union of disjoint ``[prefix, strinc(prefix))`` ranges,
never one enclosing interval. Discovery is a startup snapshot: the result is
frozen and does not follow later directory changes.

Contract: [`docs/CDC-CONSUMER-SPEC.md`](../../../docs/CDC-CONSUMER-SPEC.md) §2.5.
"""

from __future__ import annotations

import bisect
import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Final

try:
    import fdb
except ImportError:  # pragma: no cover
    fdb = None  # type: ignore[assignment]

from fdb_kafka_bridge.cdc.client import _native_error_code
from fdb_kafka_bridge.cdc.errors import (
    CDCInvalidRangeError,
    CDCNotSupportedError,
    DirectoryNotFoundError,
    DirectoryPartitionError,
    DirectoryResolutionError,
)
from fdb_kafka_bridge.cdc.listener import strinc
from fdb_kafka_bridge.cdc.ranges import (
    MAX_CDC_RANGES,
    USER_KEYSPACE_END,
    KeyRange,
    validate_key_ranges,
)

DirectoryPath = tuple[str, ...]

#: Version of the canonical fingerprint representation; bump on any change.
FINGERPRINT_FORMAT_VERSION: Final[int] = 1
_FINGERPRINT_KIND: Final[str] = "fdb-kafka-bridge/directory-cdc"
_FINGERPRINT_PREFIX: Final[str] = f"v{FINGERPRINT_FORMAT_VERSION}:sha256:"

DEFAULT_MAX_DISCOVERY_ATTEMPTS: Final[int] = 5
_PARTITION_LAYER: Final[bytes] = b"partition"


def validate_directory_path(path: object, *, name: str = "root_path") -> DirectoryPath:
    """Validate a logical Directory Layer path.

    A path is a non-empty ``tuple`` of ``str`` components, each encodable as
    UTF-8. The empty tuple is rejected: it names the Directory Layer root,
    whose contents span the whole user key space, including the layer's own
    metadata. A component may be any such string, including ``""``, because the
    Directory Layer accepts it. ``bytes`` components are rejected even though
    the binding decodes them, so ``("a",)`` and ``(b"a",)`` cannot alias one
    directory under two fingerprints.

    Args:
        path: The candidate path.
        name: Parameter name used in error messages.

    Returns:
        The path as a tuple of ``str``.

    Raises:
        TypeError: If ``path`` is not a tuple (a plain string is rejected) or a
            component is not ``str``.
        ValueError: If ``path`` is empty or a component is not UTF-8-encodable.
    """
    if isinstance(path, str):
        raise TypeError(
            f"{name} must be a tuple of path components, not a plain string; "
            f"use ({path!r},)"
        )
    if not isinstance(path, tuple):
        raise TypeError(f"{name} must be a tuple of str, got {type(path).__name__}")
    if not path:
        raise ValueError(
            f"{name} must not be empty; the Directory Layer root spans the "
            "whole key space"
        )
    for index, component in enumerate(path):
        if type(component) is not str:
            raise TypeError(
                f"{name}[{index}] must be str, got {type(component).__name__}"
            )
        try:
            component.encode("utf-8")
        except UnicodeEncodeError:
            raise ValueError(
                f"{name}[{index}] is not UTF-8-encodable (length {len(component)})"
            ) from None
    return path


@dataclass(frozen=True, slots=True)
class DirectoryRange:
    """One resolved directory and the key range its raw prefix covers.

    Attributes:
        path: Full logical path of the directory.
        prefix: The directory's raw allocated prefix.
    """

    path: DirectoryPath
    prefix: bytes

    def __post_init__(self) -> None:
        """Validate the path and prefix.

        Raises:
            TypeError: If the path is malformed or ``prefix`` is not bytes.
            ValueError: If the path is empty.
            CDCInvalidRangeError: If ``prefix`` is empty or lies in system key
                space.
        """
        validate_directory_path(self.path, name="path")
        if type(self.prefix) is not bytes:
            raise TypeError(
                f"prefix of {self.path!r} must be bytes, got "
                f"{type(self.prefix).__name__}"
            )
        if not self.prefix:
            raise CDCInvalidRangeError(
                f"directory {self.path!r} has an empty prefix, which would watch "
                "the whole key space"
            )
        if self.prefix >= USER_KEYSPACE_END:
            raise CDCInvalidRangeError(
                f"directory {self.path!r} prefix lies in system key space"
            )

    @property
    def begin_key(self) -> bytes:
        """Inclusive start of the range: the raw prefix itself."""
        return self.prefix

    @property
    def end_key(self) -> bytes:
        """Exclusive end of the range: ``strinc(prefix)``."""
        return strinc(self.prefix)

    @property
    def key_range(self) -> KeyRange:
        """The ``(begin_key, end_key)`` pair for native registration."""
        return (self.begin_key, self.end_key)

    def contains(self, key: bytes) -> bool:
        """Whether ``key`` lies in ``[begin_key, end_key)``."""
        return self.begin_key <= key < self.end_key


def directory_fingerprint(
    root_path: DirectoryPath, recursive: bool, ranges: Iterable[DirectoryRange]
) -> str:
    """Return the deterministic fingerprint of a resolved configuration.

    The canonical representation is UTF-8 JSON with sorted object keys, no
    insignificant whitespace and ASCII escaping::

        {"format": 1, "kind": "fdb-kafka-bridge/directory-cdc",
         "ranges": [{"begin": <hex>, "end": <hex>, "path": [<str>, ...]}, ...],
         "recursive": <bool>, "root_path": [<str>, ...]}

    ``ranges`` is ordered by raw ``begin`` bytes and keys are lowercase hex, so
    the encoding is lossless and independent of discovery order.

    Args:
        root_path: Requested logical root.
        recursive: Whether descendants were discovered.
        ranges: Resolved directories, in any order.

    Returns:
        ``"v1:sha256:"`` followed by the lowercase hex SHA-256 of the
        canonical representation.
    """
    payload = {
        "format": FINGERPRINT_FORMAT_VERSION,
        "kind": _FINGERPRINT_KIND,
        "recursive": recursive,
        "root_path": list(root_path),
        "ranges": [
            {
                "begin": item.begin_key.hex(),
                "end": item.end_key.hex(),
                "path": list(item.path),
            }
            for item in sorted(ranges, key=lambda r: r.key_range)
        ],
    }
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return _FINGERPRINT_PREFIX + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ResolvedDirectoryTree:
    """An immutable, validated snapshot of a resolved directory tree.

    Construction validates the whole result: the root is present exactly once,
    every path is the root or (in recursive mode) a descendant of it, paths are
    unique, and the ranges are disjoint. ``ranges`` is stored sorted by raw
    prefix bytes.

    Attributes:
        root_path: Requested logical root.
        recursive: Whether descendants were discovered.
        ranges: Resolved directories sorted by ``begin_key``.
        fingerprint: :func:`directory_fingerprint` of this result.
    """

    root_path: DirectoryPath
    recursive: bool
    ranges: tuple[DirectoryRange, ...]
    fingerprint: str = field(init=False)
    _begins: tuple[bytes, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        """Validate and sort the ranges, then compute the fingerprint.

        Raises:
            TypeError: If a field has the wrong type.
            ValueError: If the root path is invalid.
            DirectoryResolutionError: If the root is missing or repeated, a
                path is outside the root (or below it in non-recursive mode),
                or paths repeat.
            CDCInvalidRangeError: If ranges overlap, duplicate, or exceed the
                native range limit.
        """
        validate_directory_path(self.root_path)
        if type(self.recursive) is not bool:
            raise TypeError("recursive must be a bool")
        if not isinstance(self.ranges, tuple) or not all(
            isinstance(item, DirectoryRange) for item in self.ranges
        ):
            raise TypeError("ranges must be a tuple of DirectoryRange")

        paths = [item.path for item in self.ranges]
        if len(set(paths)) != len(paths):
            raise DirectoryResolutionError("resolved directory paths must be unique")
        if paths.count(self.root_path) != 1:
            raise DirectoryResolutionError(
                f"resolved ranges must include the root {self.root_path!r} once"
            )
        depth = len(self.root_path)
        for path in paths:
            if path[:depth] != self.root_path:
                raise DirectoryResolutionError(
                    f"directory {path!r} is outside root {self.root_path!r}"
                )
            if not self.recursive and path != self.root_path:
                raise DirectoryResolutionError(
                    f"non-recursive resolution cannot include {path!r}"
                )

        validate_key_ranges(item.key_range for item in self.ranges)
        ordered = tuple(sorted(self.ranges, key=lambda r: r.key_range))
        object.__setattr__(self, "ranges", ordered)
        object.__setattr__(self, "_begins", tuple(r.begin_key for r in ordered))
        object.__setattr__(
            self,
            "fingerprint",
            directory_fingerprint(self.root_path, self.recursive, ordered),
        )

    @property
    def key_ranges(self) -> tuple[KeyRange, ...]:
        """The ``(begin_key, end_key)`` pairs to register, in ``ranges`` order."""
        return tuple(item.key_range for item in self.ranges)

    @property
    def paths(self) -> tuple[DirectoryPath, ...]:
        """The resolved logical paths, in ``ranges`` order."""
        return tuple(item.path for item in self.ranges)

    def lookup(self, key: bytes) -> DirectoryRange | None:
        """Return the directory whose own range contains ``key``.

        Ranges are disjoint, so at most one matches. The match is the most
        specific directory for the key: a key under a child's prefix belongs to
        the child, because a non-partition child's prefix is never inside its
        parent's. Boundaries are half-open, so a key equal to a prefix matches
        and a key equal to ``end_key`` does not.

        A ``CLEAR_RANGE`` mutation can span several directories (native CDC
        clips it to each intersecting registered range, not to directory
        boundaries), so its begin key alone does not identify one directory.

        Args:
            key: A raw mutation key.

        Returns:
            The matching :class:`DirectoryRange`, or ``None`` if ``key`` is
            outside every resolved range.

        Raises:
            TypeError: If ``key`` is not bytes.
        """
        if type(key) is not bytes:
            raise TypeError(f"key must be bytes, got {type(key).__name__}")
        index = bisect.bisect_right(self._begins, key) - 1
        if index < 0:
            return None
        candidate = self.ranges[index]
        return candidate if candidate.contains(key) else None

    def path_for_key(self, key: bytes) -> DirectoryPath | None:
        """Return the logical path owning ``key``, or ``None`` (see :meth:`lookup`).

        Args:
            key: A raw mutation key.

        Returns:
            The owning directory's path, or ``None`` outside every range.
        """
        match = self.lookup(key)
        return None if match is None else match.path


def _is_partition(directory: Any) -> bool:
    impl = getattr(fdb, "directory_impl", None)
    partition_type = getattr(impl, "DirectoryPartition", None)
    if partition_type is not None and isinstance(directory, partition_type):
        return True
    get_layer = getattr(directory, "get_layer", None)
    return callable(get_layer) and get_layer() == _PARTITION_LAYER


class DirectoryCDCResolver:
    """Resolve an existing Directory Layer tree for one multi-range CDC stream.

    Directories are opened, never created. Discovery runs in one read-only
    transaction, so every directory is read at a single read version; the
    transaction is never committed. A retryable FDB error (including
    ``transaction_too_old`` on a tree too large to read within the transaction
    time limit) restarts discovery from scratch, up to ``max_attempts``. A
    partial tree is never returned.

    Directory Layer partitions are rejected, as root or descendant: a
    partition's prefix range also holds its own node metadata, so registering
    it would capture keys that belong to no directory.
    """

    def __init__(
        self,
        db: Any,
        root_path: DirectoryPath,
        *,
        recursive: bool = False,
        directory_layer: Any | None = None,
        max_attempts: int = DEFAULT_MAX_DISCOVERY_ATTEMPTS,
    ) -> None:
        """Configure a resolver.

        Args:
            db: An open FoundationDB ``Database``.
            root_path: Logical path of the existing root directory.
            recursive: Whether to include every existing descendant.
            directory_layer: Directory Layer to read; defaults to
                ``fdb.directory``.
            max_attempts: Discovery attempts before giving up on retryable
                FDB errors.

        Raises:
            TypeError: If ``root_path`` is malformed, ``recursive`` is not a
                bool, or ``max_attempts`` is not an int.
            ValueError: If ``root_path`` is empty or ``max_attempts`` < 1.
            CDCNotSupportedError: If no ``directory_layer`` is given and the
                ``fdb`` bindings are not installed.
        """
        self._root_path = validate_directory_path(root_path)
        if type(recursive) is not bool:
            raise TypeError("recursive must be a bool")
        if type(max_attempts) is not int:
            raise TypeError("max_attempts must be an int")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if directory_layer is None:
            if fdb is None:
                raise CDCNotSupportedError(
                    "FoundationDB python bindings ('fdb') are not installed."
                )
            directory_layer = fdb.directory
        self._db = db
        self._recursive = recursive
        self._layer = directory_layer
        self._max_attempts = max_attempts

    @property
    def root_path(self) -> DirectoryPath:
        """The validated logical root path."""
        return self._root_path

    @property
    def recursive(self) -> bool:
        """Whether descendants are discovered."""
        return self._recursive

    def resolve(self) -> ResolvedDirectoryTree:
        """Read the tree once and return its validated, frozen ranges.

        Returns:
            The resolved tree.

        Raises:
            DirectoryNotFoundError: If the root directory does not exist.
            DirectoryPartitionError: If the root or a descendant is a partition.
            DirectoryResolutionError: If discovery cannot complete within
                ``max_attempts``, a child name is not ``str``, or the tree has
                more directories than native CDC accepts ranges.
            CDCInvalidRangeError: If resolved prefixes overlap or are invalid.
            Exception: A non-retryable FDB error from ``on_error``.
        """
        tr = self._db.create_transaction()
        attempt = 0
        while True:
            try:
                entries = self._discover(tr)
                break
            except DirectoryResolutionError:
                raise
            except Exception as exc:
                if _native_error_code(exc) is None:
                    raise
                attempt += 1
                if attempt >= self._max_attempts:
                    raise DirectoryResolutionError(
                        f"could not read a consistent snapshot of "
                        f"{self._root_path!r} in {self._max_attempts} attempt(s): "
                        f"{exc}"
                    ) from exc
                # Raises for errors that are not retryable; resets tr otherwise.
                tr.on_error(exc).wait()
        return ResolvedDirectoryTree(
            root_path=self._root_path,
            recursive=self._recursive,
            ranges=tuple(DirectoryRange(path, prefix) for path, prefix in entries),
        )

    def _discover(self, tr: Any) -> list[tuple[DirectoryPath, bytes]]:
        """Read every directory in the tree with ``tr``; iterative, depth-first."""
        layer = self._layer
        if not layer.exists(tr, self._root_path):
            raise DirectoryNotFoundError(
                f"directory {self._root_path!r} does not exist; it is opened, "
                "never created"
            )
        entries: list[tuple[DirectoryPath, bytes]] = []
        stack: list[tuple[DirectoryPath, Any]] = [
            (self._root_path, layer.open(tr, self._root_path))
        ]
        while stack:
            path, directory = stack.pop()
            if _is_partition(directory):
                raise DirectoryPartitionError(
                    f"directory {path!r} is a Directory Layer partition; "
                    "partitions are not supported by directory CDC"
                )
            entries.append((path, directory.key()))
            if len(entries) > MAX_CDC_RANGES:
                raise DirectoryResolutionError(
                    f"tree under {self._root_path!r} has more than "
                    f"{MAX_CDC_RANGES} directories, the native range limit"
                )
            if not self._recursive:
                continue
            for name in layer.list(tr, path):
                if type(name) is not str:
                    raise DirectoryResolutionError(
                        f"child of {path!r} has a non-str name "
                        f"{type(name).__name__}"
                    )
                child = path + (name,)
                stack.append((child, layer.open(tr, child)))
        return entries
