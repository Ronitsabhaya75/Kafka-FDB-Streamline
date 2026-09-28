"""FoundationDB client wrapper for API initialization and CDC stream operations."""

from typing import Any, Final, NoReturn, Self

try:
    import fdb
except ImportError:  # pragma: no cover
    fdb = None  # type: ignore[assignment]

from src.cdc.errors import (
    CDCError,
    CDCInvalidCursorError,
    CDCInvalidRangeError,
    CDCNotSupportedError,
    CDCRetryableError,
    CDCTerminalError,
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
    if fdb is not None and isinstance(exc, fdb.FDBError):
        return exc.code
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
        begin_key: bytes,
        end_key: bytes,
    ) -> int:
        """Register a CDC stream over a specified key range [begin_key, end_key).

        Args:
            name: Unique stream identifier name.
            begin_key: Inclusive start key of the monitored range.
            end_key: Exclusive end key of the monitored range.

        Returns:
            64-bit integer stream ID assigned by FoundationDB.

        Raises:
            CDCInvalidRangeError: If the range is empty, reversed, or not bytes.
            CDCNotSupportedError: If the loaded binding lacks native CDC support.
        """
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
