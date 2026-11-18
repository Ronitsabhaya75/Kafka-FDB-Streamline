"""FoundationDB client wrapper for API initialization and CDC stream operations."""

import inspect
from collections.abc import Iterable
from typing import Any, Final, NoReturn, Self

try:
    import fdb
except ImportError:  # pragma: no cover
    fdb = None  # type: ignore[assignment]

from fdb_kafka_bridge.cdc.errors import (
    CDCError,
    CDCInvalidCursorError,
    CDCInvalidRangeError,
    CDCNotSupportedError,
    CDCRetryableError,
    CDCStreamMismatchError,
    CDCTerminalError,
)
from fdb_kafka_bridge.cdc.ranges import (
    KeyRange,
    canonicalize_key_ranges,
    validate_key_ranges,
)

DEFAULT_CDC_API_VERSION: Final[int] = 800
MINIMUM_CDC_API_VERSION: Final[int] = 800

# Native FoundationDB error codes the consumer classifies explicitly.
# See CDC-DEEP-DIVE.md §3 "Errors we have to map".
FDB_TRANSACTION_TOO_OLD: Final[int] = 1007
FDB_SERVER_OVERLOADED: Final[int] = 1211
FDB_CLIENT_INVALID_OPERATION: Final[int] = 2000


def _native_error_code(exc: BaseException) -> int | None:
    """Return the FoundationDB error code carried by ``exc``, if any.

    Args:
        exc: An exception raised by a native consume or acknowledge call.

    Returns:
        The integer FDB error code, or ``None`` if ``exc`` is not a native
        ``FDBError``.
    """
    fdb_err_type = getattr(fdb, "FDBError", None) or getattr(
        getattr(fdb, "impl", None), "FDBError", None
    )
    if fdb_err_type is not None and isinstance(exc, fdb_err_type):
        return getattr(exc, "code", None)
    # Offline (no bindings) and in tests, duck-type on FDBError's int ``code``.
    code = getattr(exc, "code", None)
    return code if type(code) is int else None


def _raise_consume_failure(exc: BaseException, action: str) -> NoReturn:
    """Re-raise a native consume/acknowledge failure with the right policy.

    The three CDC error codes become terminal or retryable ``CDCConsumeError``
    subclasses that keep ``.code`` on the surface, so a retry loop can act without
    unwrapping ``__cause__``. Any other native ``FDBError`` propagates unwrapped so
    callers keep normal FDB retry and diagnostics. A non-native failure is wrapped
    as ``CDCError``.

    Args:
        exc: The exception raised by the native call.
        action: Short description of the failed action for the message.

    Raises:
        CDCRetryableError: On ``server_overloaded`` (1211).
        CDCTerminalError: On ``transaction_too_old`` (1007) or
            ``client_invalid_operation`` (2000).
        CDCError: If the failure did not originate from the native binding.
        BaseException: The original ``exc`` for any other native FDB error.
    """
    code = _native_error_code(exc)
    if code == FDB_SERVER_OVERLOADED:
        raise CDCRetryableError(f"Failed to {action}: {exc}", code=code) from exc
    if code in (FDB_TRANSACTION_TOO_OLD, FDB_CLIENT_INVALID_OPERATION):
        raise CDCTerminalError(f"Failed to {action}: {exc}", code=code) from exc
    if code is not None:
        raise exc
    raise CDCError(f"Failed to {action}: {exc}") from exc


def _to_bytes(name: bytes | str) -> bytes:
    """Return a stream name as bytes.

    Args:
        name: UTF-8 string or bytes stream name.

    Returns:
        The byte representation expected by the FDB binding.

    Raises:
        TypeError: If name is neither bytes nor str.
    """
    if isinstance(name, bytes):
        return name
    if isinstance(name, str):
        return name.encode("utf-8")
    raise TypeError(f"name must be bytes or str, got {type(name).__name__}")


def _translate_cdc_runtime_error(exc: RuntimeError) -> NoReturn:
    """Raise the package error for an unavailable native CDC implementation.

    Args:
        exc: Runtime error raised by the FoundationDB binding.

    Raises:
        CDCNotSupportedError: If the binding reports missing CDC support.
        RuntimeError: If the runtime error is unrelated to CDC availability.
    """
    message = str(exc).lower()
    if "native cdc" in message and (
        "does not support" in message or "requires api version" in message
    ):
        raise CDCNotSupportedError(str(exc)) from exc
    raise exc


def multi_range_cdc_unsupported_reason() -> str | None:
    """Explain why the loaded binding cannot register multi-range streams.

    Evidence-based, with no native call: the ``foundationdb`` 8.0.0 binding
    adds the keyword-only ``ranges`` parameter to
    ``Database.register_cdc_stream`` and the ``CdcKeyRange`` type together
    with the five-argument ``fdb_database_register_cdc_stream(db, name,
    name_length, ranges, range_count)`` it calls. A binding without them
    predates multi-range CDC, and probing its native symbol would risk the
    signature mismatch described in CDC-DEEP-DIVE §2.6.

    The binding only loads ``fdb.Database`` once an API version is selected,
    so call this after :func:`init_fdb` (``FDBClient.open``).

    Returns:
        ``None`` if supported, otherwise the missing capability.
    """
    if fdb is None:
        return "FoundationDB python bindings ('fdb') are not installed"
    if not fdb.is_api_version_selected():
        return "no FDB API version is selected yet; open the client first"
    database_type = getattr(fdb, "Database", None)
    register = getattr(database_type, "register_cdc_stream", None)
    if register is None:
        return "the loaded fdb binding has no Database.register_cdc_stream"
    try:
        parameters = inspect.signature(register).parameters
    except (TypeError, ValueError):
        return "Database.register_cdc_stream has no inspectable signature"
    ranges = parameters.get("ranges")
    if ranges is None or ranges.kind is not inspect.Parameter.KEYWORD_ONLY:
        return "Database.register_cdc_stream has no keyword-only 'ranges' parameter"
    if not hasattr(fdb, "CdcKeyRange"):
        return "the loaded fdb binding does not export CdcKeyRange"
    return None


def _stream_ranges(info: Any) -> tuple[KeyRange, ...]:
    """Return a listed stream's canonical ranges as ``(begin, end)`` pairs."""
    ranges = getattr(info, "ranges", None)
    if ranges is None:
        return ((bytes(info.begin_key), bytes(info.end_key)),)
    return tuple((bytes(r.begin_key), bytes(r.end_key)) for r in ranges)


def init_fdb(api_version: int = DEFAULT_CDC_API_VERSION) -> None:
    """Initialize the FoundationDB API version.

    FoundationDB requires setting the API version before any database operations.
    Native CDC strictly requires API version 800 or later.

    Args:
        api_version: FDB API version integer to select. Defaults to 800.

    Raises:
        CDCNotSupportedError: If 'fdb' is not installed or api_version < 800.
        RuntimeError: If FDB was already initialized with a conflicting version.
    """
    if api_version < MINIMUM_CDC_API_VERSION:
        raise CDCNotSupportedError(
            f"Native CDC requires API version {MINIMUM_CDC_API_VERSION} or later; "
            f"requested {api_version}."
        )

    if fdb is None:
        raise CDCNotSupportedError(
            "FoundationDB python bindings ('fdb') are not installed."
        )

    # Check if API version is already initialized
    try:
        current_version = fdb.get_api_version()
        if current_version == api_version:
            return
        raise RuntimeError(
            f"FDB API version already initialized to {current_version}; "
            f"cannot reinitialize to {api_version}."
        )
    except RuntimeError as exc:
        # FDB raises RuntimeError if not initialized yet or if conflicting version
        if "API version is not set" in str(exc) or "not set" in str(exc):
            fdb.api_version(api_version)
        else:
            raise


class FDBClient:
    """FoundationDB client connection manager and CDC stream factory.

    Wraps FDB database handle, manages API initialization, and provides
    high-level methods for CDC stream registration, consumer creation,
    and cursor resumption.
    """

    def __init__(
        self,
        cluster_file: str | None = None,
        api_version: int = DEFAULT_CDC_API_VERSION,
    ) -> None:
        """Initialize FDBClient configuration.

        Args:
            cluster_file: Path to FoundationDB cluster file (fdb.cluster).
                If None, uses the default cluster file or FDB_CLUSTER_FILE env.
            api_version: FoundationDB API version. Defaults to 800.
        """
        self.cluster_file = cluster_file
        self.api_version = api_version
        self._db: Any | None = None

    def open(self) -> Any:
        """Initialize FDB API version and open the database connection.

        Returns:
            Underlying FoundationDB Database instance.

        Raises:
            CDCNotSupportedError: If 'fdb' is unavailable or api_version is unsupported.
        """
        if self._db is None:
            init_fdb(self.api_version)
            self._db = fdb.open(self.cluster_file)
        return self._db

    @property
    def db(self) -> Any:
        """Get the active FoundationDB database handle, opening it if necessary."""
        return self.open()

    def close(self) -> None:
        """Close client resources and release database handle."""
        self._db = None

    def __enter__(self) -> Self:
        """Enter context manager, opening database connection."""
        self.open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit context manager, releasing database handle."""
        self.close()

    def register_cdc_stream(
        self,
        name: bytes | str,
        begin_key: bytes | None = None,
        end_key: bytes | None = None,
        *,
        ranges: Iterable[KeyRange] | None = None,
    ) -> int:
        """Register a CDC stream over one range, or over a union of ranges.

        Pass ``begin_key`` and ``end_key`` for one ``[begin_key, end_key)``
        range, or ``ranges`` for a multi-range stream. The whole ``ranges``
        collection is validated (``validate_key_ranges``) and the binding's
        multi-range capability checked before the single native registration
        call.

        Native registration is idempotent for the same name and canonical union
        and fails for a different union. Prefer :meth:`ensure_cdc_stream` for
        multi-range streams; it reports a mismatch before registering.

        Args:
            name: Unique stream identifier name.
            begin_key: Inclusive start key of a single monitored range.
            end_key: Exclusive end key of a single monitored range.
            ranges: ``(begin_key, end_key)`` pairs for a multi-range stream.

        Returns:
            64-bit integer stream ID assigned by FoundationDB.

        Raises:
            TypeError: If ``ranges`` is combined with ``begin_key`` or
                ``end_key``.
            CDCInvalidRangeError: If a range is empty, reversed, not bytes, or
                (for ``ranges``) duplicated, overlapping, outside user key
                space, or more than the native limit.
            CDCNotSupportedError: If the loaded binding lacks native CDC
                support, or multi-range support when ``ranges`` is given.
        """
        if ranges is not None:
            if begin_key is not None or end_key is not None:
                raise TypeError("ranges cannot be combined with begin_key or end_key")
            return self._register_ranges(name, validate_key_ranges(ranges))
        if not isinstance(begin_key, bytes) or not isinstance(end_key, bytes):
            raise CDCInvalidRangeError("CDC range boundaries must be bytes")
        if begin_key >= end_key:
            raise CDCInvalidRangeError("CDC range must satisfy begin_key < end_key")
        try:
            return int(
                self.db.register_cdc_stream(_to_bytes(name), begin_key, end_key).wait()
            )
        except RuntimeError as exc:
            _translate_cdc_runtime_error(exc)

    def _register_ranges(self, name: bytes | str, ranges: tuple[KeyRange, ...]) -> int:
        """Register already validated ranges with one native call.

        Raises:
            CDCNotSupportedError: If the binding cannot register multi-range
                streams.
        """
        self.open()
        reason = multi_range_cdc_unsupported_reason()
        if reason is not None:
            raise CDCNotSupportedError(
                f"Multi-range CDC registration is unavailable: {reason}. It needs "
                "the foundationdb>=8.0.0 Python binding with a matching 8.0 "
                "libfdb_c (see the Dockerfile's FDB_VERSION)."
            )
        try:
            return int(
                self.db.register_cdc_stream(_to_bytes(name), ranges=list(ranges)).wait()
            )
        except RuntimeError as exc:
            _translate_cdc_runtime_error(exc)

    def get_cdc_stream(self, name: bytes | str) -> Any | None:
        """Return the listed stream registered under ``name``, if any.

        Args:
            name: Stream name.

        Returns:
            The ``CdcStreamInfo`` for ``name``, or ``None`` if not registered.

        Raises:
            CDCNotSupportedError: If the loaded binding lacks native CDC support.
        """
        raw = _to_bytes(name)
        for info in self.list_cdc_streams():
            if bytes(info.name) == raw:
                return info
        return None

    def ensure_cdc_stream(self, name: bytes | str, ranges: Iterable[KeyRange]) -> int:
        """Register a multi-range stream, or verify an existing one matches.

        Every check runs before the single native registration call: the
        ranges are validated, multi-range support is confirmed, and an existing
        stream's listed canonical union must equal the requested one. A
        mismatched stream is refused, never removed or re-registered.

        Args:
            name: Stream name.
            ranges: ``(begin_key, end_key)`` pairs, in any order.

        Returns:
            The stream ID; for an existing matching stream, its existing ID.

        Raises:
            CDCInvalidRangeError: If the ranges are invalid.
            CDCNotSupportedError: If multi-range CDC is unavailable.
            CDCStreamMismatchError: If ``name`` is registered over a different
                union, or its ID changed during registration.
        """
        validated = validate_key_ranges(ranges)
        self.open()
        reason = multi_range_cdc_unsupported_reason()
        if reason is not None:
            raise CDCNotSupportedError(
                f"Multi-range CDC registration is unavailable: {reason}."
            )
        existing = self.get_cdc_stream(name)
        if existing is not None:
            expected = canonicalize_key_ranges(validated)
            actual = _stream_ranges(existing)
            if actual != expected:
                raise CDCStreamMismatchError(
                    f"CDC stream {_to_bytes(name)!r} (id {existing.stream_id}) is "
                    f"registered over {len(actual)} canonical range(s) that differ "
                    f"from the {len(expected)} requested; stream ranges are "
                    "immutable, so use a new stream name or remove the stream "
                    "deliberately"
                )
        stream_id = self._register_ranges(name, validated)
        if existing is not None and stream_id != existing.stream_id:
            raise CDCStreamMismatchError(
                f"CDC stream {_to_bytes(name)!r} changed id from "
                f"{existing.stream_id} to {stream_id} during registration"
            )
        return stream_id

    def create_cdc_consumer(self, name: bytes | str) -> Any:
        """Create a new CDC consumer for a registered stream.

        Begins streaming at the current cluster min_version.

        Args:
            name: Identifier of the registered CDC stream.

        Returns:
            CdcConsumer handle.

        Raises:
            CDCNotSupportedError: If the loaded binding lacks native CDC support.
        """
        try:
            return self.db.create_cdc_consumer(_to_bytes(name)).wait()
        except RuntimeError as exc:
            _translate_cdc_runtime_error(exc)

    def resume_cdc_consumer(self, cursor: Any) -> Any:
        """Resume an existing CDC consumer from a persisted cursor.

        Args:
            cursor: CdcCursor (stream_id, last_consumed_version).

        Returns:
            Resumed CdcConsumer handle.

        Raises:
            CDCInvalidCursorError: If cursor coordinates are absent or out of range.
            CDCNotSupportedError: If the loaded binding lacks native CDC support.
        """
        try:
            stream_id = cursor.stream_id
            last_consumed_version = cursor.last_consumed_version
        except AttributeError as exc:
            raise CDCInvalidCursorError(
                "cursor must expose stream_id and last_consumed_version"
            ) from exc
        if type(stream_id) is not int or not 0 <= stream_id < 2**64:
            raise CDCInvalidCursorError("cursor.stream_id must be a uint64 integer")
        if (
            type(last_consumed_version) is not int
            or not -(2**63) <= last_consumed_version < 2**63
        ):
            raise CDCInvalidCursorError(
                "cursor.last_consumed_version must be an int64 integer"
            )
        try:
            return self.db.resume_cdc_consumer(cursor).wait()
        except RuntimeError as exc:
            _translate_cdc_runtime_error(exc)

    def remove_cdc_stream(self, name: bytes | str) -> None:
        """Remove a registered CDC stream by name.

        Removal is terminal; re-registering the same name will allocate a new stream ID.

        Args:
            name: Identifier of the registered CDC stream to remove.

        Raises:
            CDCNotSupportedError: If the loaded binding lacks native CDC support.
        """
        try:
            self.db.remove_cdc_stream(_to_bytes(name)).wait()
        except RuntimeError as exc:
            _translate_cdc_runtime_error(exc)

    def list_cdc_streams(self) -> list[Any]:
        """List all active CDC streams in the cluster.

        Returns:
            List of CdcStreamInfo objects.

        Raises:
            CDCNotSupportedError: If the loaded binding lacks native CDC support.
        """
        try:
            return list(self.db.list_cdc_streams().wait())
        except RuntimeError as exc:
            _translate_cdc_runtime_error(exc)

    def get_read_version(self) -> int:
        """Return a fresh database read version.

        Returns:
            Current cluster read version.
        """
        if hasattr(self.db, "get_read_version"):
            rv = self.db.get_read_version()
            if hasattr(rv, "wait"):
                return int(rv.wait())
            return int(rv)
        tr = self.db.create_transaction()
        return int(tr.get_read_version().wait())
