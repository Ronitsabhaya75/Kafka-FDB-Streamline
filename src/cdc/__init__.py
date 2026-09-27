"""FoundationDB native Change Data Capture (CDC) consumer package.

Provides high-level client connection management, mutation stream listening,
and Protobuf mapping for FoundationDB 8.0 native CDC.
"""

from src.cdc.client import (
    DEFAULT_CDC_API_VERSION,
    MINIMUM_CDC_API_VERSION,
    FDBClient,
    init_fdb,
)
from src.cdc.errors import (
    CDCConsumerClosedError,
    CDCError,
    CDCInvalidCursorError,
    CDCInvalidRangeError,
    CDCNotSupportedError,
    CDCStreamNotFoundError,
)
from src.cdc.listener import (
    FDBMutationListener,
    strinc,
    subspace_to_key_range,
)
from src.cdc.mapper import (
    RawMutation,
    to_clear_range,
    to_fdb_mutation,
    to_fdb_mutation_batch,
    to_mutation_record,
    to_single_key_mutation,
    to_timestamp,
    to_version_end,
    to_version_index,
)

__all__ = [
    "CDCConsumerClosedError",
    "CDCError",
    "CDCInvalidCursorError",
    "CDCInvalidRangeError",
    "CDCNotSupportedError",
    "CDCStreamNotFoundError",
    "DEFAULT_CDC_API_VERSION",
    "FDBClient",
    "FDBMutationListener",
    "MINIMUM_CDC_API_VERSION",
    "RawMutation",
    "init_fdb",
    "strinc",
    "subspace_to_key_range",
    "to_clear_range",
    "to_fdb_mutation",
    "to_fdb_mutation_batch",
    "to_mutation_record",
    "to_single_key_mutation",
    "to_timestamp",
    "to_version_end",
    "to_version_index",
]
