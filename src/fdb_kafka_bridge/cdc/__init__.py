"""FoundationDB native Change Data Capture (CDC) consumer package.

Provides high-level client connection management, bounded Protobuf record
polling, and Directory Layer resolution into multi-range streams for
FoundationDB 8.0 native CDC.
"""

from fdb_kafka_bridge.cdc.client import (
    DEFAULT_CDC_API_VERSION,
    MINIMUM_CDC_API_VERSION,
    FDBClient,
    init_fdb,
    multi_range_cdc_unsupported_reason,
)
from fdb_kafka_bridge.cdc.directory import (
    FINGERPRINT_FORMAT_VERSION,
    DirectoryCDCResolver,
    DirectoryPath,
    DirectoryRange,
    ResolvedDirectoryTree,
    directory_fingerprint,
    validate_directory_path,
)
from fdb_kafka_bridge.cdc.errors import (
    CDCConsumeError,
    CDCConsumerClosedError,
    CDCError,
    CDCInvalidCursorError,
    CDCInvalidRangeError,
    CDCNotSupportedError,
    CDCRetryableError,
    CDCStreamMismatchError,
    CDCTerminalError,
    DirectoryNotFoundError,
    DirectoryPartitionError,
    DirectoryResolutionError,
)
from fdb_kafka_bridge.cdc.listener import (
    FDBMutationListener,
    strinc,
    subspace_to_key_range,
)
from fdb_kafka_bridge.cdc.ranges import (
    MAX_CDC_RANGES,
    KeyRange,
    canonicalize_key_ranges,
    validate_key_ranges,
)

__all__ = [
    "CDCConsumeError",
    "CDCConsumerClosedError",
    "CDCError",
    "CDCInvalidCursorError",
    "CDCInvalidRangeError",
    "CDCNotSupportedError",
    "CDCRetryableError",
    "CDCStreamMismatchError",
    "CDCTerminalError",
    "DEFAULT_CDC_API_VERSION",
    "DirectoryCDCResolver",
    "DirectoryNotFoundError",
    "DirectoryPartitionError",
    "DirectoryPath",
    "DirectoryRange",
    "DirectoryResolutionError",
    "FDBClient",
    "FDBMutationListener",
    "FINGERPRINT_FORMAT_VERSION",
    "KeyRange",
    "MAX_CDC_RANGES",
    "MINIMUM_CDC_API_VERSION",
    "ResolvedDirectoryTree",
    "canonicalize_key_ranges",
    "directory_fingerprint",
    "init_fdb",
    "multi_range_cdc_unsupported_reason",
    "strinc",
    "subspace_to_key_range",
    "validate_directory_path",
    "validate_key_ranges",
]
