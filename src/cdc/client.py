"""FoundationDB client wrapper for API initialization and CDC stream operations."""

from typing import Any, Final, Self

try:
    import fdb
except ImportError:  # pragma: no cover
    fdb = None  # type: ignore[assignment]

from src.cdc.errors import CDCNotSupportedError

DEFAULT_CDC_API_VERSION: Final[int] = 800
MINIMUM_CDC_API_VERSION: Final[int] = 800


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
        """
        raw_name = name.encode("utf-8") if isinstance(name, str) else name
        return int(self.db.register_cdc_stream(raw_name, begin_key, end_key).wait())

    def create_cdc_consumer(self, name: bytes | str) -> Any:
        """Create a new CDC consumer for a registered stream.

        Begins streaming at the current cluster min_version.

        Args:
            name: Identifier of the registered CDC stream.

        Returns:
            CdcConsumer handle.
        """
        raw_name = name.encode("utf-8") if isinstance(name, str) else name
        return self.db.create_cdc_consumer(raw_name).wait()

    def resume_cdc_consumer(self, cursor: Any) -> Any:
        """Resume an existing CDC consumer from a persisted cursor.

        Args:
            cursor: CdcCursor (stream_id, last_consumed_version).

        Returns:
            Resumed CdcConsumer handle.
        """
        return self.db.resume_cdc_consumer(cursor).wait()

    def remove_cdc_stream(self, name: bytes | str) -> None:
        """Remove a registered CDC stream by name.

        Removal is terminal; re-registering the same name will allocate a new stream ID.

        Args:
            name: Identifier of the registered CDC stream to remove.
        """
        raw_name = name.encode("utf-8") if isinstance(name, str) else name
        self.db.remove_cdc_stream(raw_name).wait()

    def list_cdc_streams(self) -> list[Any]:
        """List all active CDC streams in the cluster.

        Returns:
            List of CdcStreamInfo objects.
        """
        return list(self.db.list_cdc_streams().wait())
