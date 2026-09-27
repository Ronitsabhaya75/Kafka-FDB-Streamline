"""FoundationDB CDC mutation stream listener and polling loop."""

import time
from collections import deque
from collections.abc import Iterator
from typing import Any, Self

from fdbkafka.cdc.v1 import mutations_pb2
from src.cdc.client import FDBClient
from src.cdc.errors import CDCConsumerClosedError, CDCError
from src.cdc.mapper import (
    to_fdb_mutation,
    to_fdb_mutation_batch,
    to_mutation_record,
    to_version_end,
)


def strinc(key: bytes) -> bytes:
    """Return the next prefix after `key` in lexicographical byte order.

    Used to compute the exclusive upper bound for a prefix key range.

    Args:
        key: Input byte string.

    Returns:
        The next prefix byte string.

    Raises:
        ValueError: If key is empty or consists entirely of 0xFF bytes.
    """
    stripped = key.rstrip(b"\xff")
    if not stripped:
        raise ValueError("Key must contain at least one byte not equal to 0xFF")
    return stripped[:-1] + bytes([stripped[-1] + 1])


def subspace_to_key_range(subspace: Any) -> tuple[bytes, bytes]:
    """Compute the strict [begin, end) key range covering all keys in a subspace.

    Uses raw prefix and strinc(prefix) rather than subspace.range(), ensuring
    that both the bare prefix and any escaped suffix keys are included.

    Args:
        subspace: FoundationDB Subspace, DirectorySubspace, or bytes prefix.

    Returns:
        Tuple of (begin_key, end_key).

    Raises:
        TypeError: If subspace is neither an FDB Subspace nor bytes.
    """
    if hasattr(subspace, "key") and callable(subspace.key):
        prefix = subspace.key()
    elif isinstance(subspace, bytes):
        prefix = subspace
    else:
        raise TypeError(
            f"Expected FDB Subspace or bytes prefix, got {type(subspace).__name__}"
        )
    return prefix, strinc(prefix)


class FDBMutationListener:
    """Listener and polling loop for consuming CDC mutations from FoundationDB.

    Monitors a designated key range or subspace, polls change batches via
    native CDC, and maps raw mutations to generated Protobuf messages.
    """

    def __init__(
        self,
        client: FDBClient,
        stream_name: str | bytes,
        *,
        key_range: tuple[bytes, bytes] | None = None,
        subspace: Any | None = None,
        cursor: Any | None = None,
        auto_register: bool = True,
    ) -> None:
        """Initialize the CDC mutation listener.

        Args:
            client: Connected FDBClient instance.
            stream_name: Name of the CDC stream to consume from.
            key_range: Optional explicit (begin_key, end_key) tuple.
            subspace: Optional FDB Subspace to monitor. Computed via
                subspace_to_key_range.
            cursor: Optional persisted CdcCursor to resume from.
            auto_register: If True and key_range/subspace is provided, registers
                the stream if not already registered.
        """
        self.client = client
        if isinstance(stream_name, bytes):
            self.raw_stream_name = stream_name
            self.stream_name = stream_name.decode("utf-8", errors="replace")
        else:
            self.stream_name = stream_name
            self.raw_stream_name = stream_name.encode("utf-8")

        if subspace is not None:
            self.key_range = subspace_to_key_range(subspace)
        else:
            self.key_range = key_range

        self.initial_cursor = cursor
        self.auto_register = auto_register
        self._consumer: Any | None = None
        self._is_closed = False
        self._awaiting_ack = False
        self._pending_mutations: deque[mutations_pb2.FDBMutation] = deque()
        self._pending_records: deque[mutations_pb2.FDBMutationRecord] = deque()
        self._pending_auto_ack = False

    def start(self) -> None:
        """Register stream if needed and create or resume CDC consumer handle."""
        if self._consumer is not None:
            return

        if self._is_closed:
            raise CDCConsumerClosedError("Cannot start a closed FDBMutationListener.")

        self.client.open()

        if self.auto_register and self.key_range is not None:
            begin_key, end_key = self.key_range
            self.client.register_cdc_stream(self.raw_stream_name, begin_key, end_key)

        if self.initial_cursor is not None:
            self._consumer = self.client.resume_cdc_consumer(self.initial_cursor)
        else:
            self._consumer = self.client.create_cdc_consumer(self.raw_stream_name)

    @property
    def consumer(self) -> Any:
        """Get the active CDC consumer handle, starting the listener if needed."""
        if self._is_closed:
            raise CDCConsumerClosedError("CDC consumer is closed.")
        if self._consumer is None:
            self.start()
        return self._consumer

    def consume_batch(self) -> Any:
        """Poll the native CDC consumer for the next batch of versioned mutations.

        Returns:
            CdcConsumeResult object containing commit version groups and watermark.

        Raises:
            CDCConsumerClosedError: If the listener is closed.
            CDCError: If the native consume operation fails.
        """
        if self._awaiting_ack:
            raise CDCError(
                "The previous CDC batch must be fully delivered and acknowledged "
                "before consuming another batch."
            )

        try:
            batch = self.consumer.consume().wait()
        except Exception as exc:
            if self._is_closed:
                raise CDCConsumerClosedError("CDC consumer is closed.") from exc
            raise CDCError(f"Failed to consume CDC batch: {exc}") from exc

        self._awaiting_ack = any(
            version_group.mutations for version_group in getattr(batch, "mutations", ())
        )
        return batch

    def poll(self) -> list[mutations_pb2.FDBMutation]:
        """Poll for change events and map them into Protobuf FDBMutation messages.

        Returns:
            List of FDBMutation messages for all version groups in the batch.
        """
        batch = self.consume_batch()
        if not hasattr(batch, "mutations") or not batch.mutations:
            return []

        mapped_mutations: list[mutations_pb2.FDBMutation] = []
        for version_group in batch.mutations:
            commit_version = version_group.version
            for seq_no, raw_m in enumerate(version_group.mutations):
                mapped = to_fdb_mutation(raw_m, commit_version, seq_no)
                mapped_mutations.append(mapped)

        return mapped_mutations

    def poll_records(
        self, bridge_timestamp_ns: int | None = None
    ) -> list[mutations_pb2.FDBMutationRecord]:
        """Poll for change events and map each version group into a batch record.

        Args:
            bridge_timestamp_ns: Optional timestamp in nanoseconds for the records.

        Returns:
            List of FDBMutationRecord envelopes containing FDBMutationBatch payloads.
        """
        batch = self.consume_batch()
        timestamp_ns = (
            bridge_timestamp_ns if bridge_timestamp_ns is not None else time.time_ns()
        )

        records: list[mutations_pb2.FDBMutationRecord] = []
        for version_group in getattr(batch, "mutations", ()):
            commit_version = version_group.version
            if version_group.mutations:
                mutation_batch = to_fdb_mutation_batch(
                    version_group.mutations, commit_version
                )
                records.append(
                    to_mutation_record(
                        stream_name=self.stream_name,
                        batch=mutation_batch,
                        bridge_timestamp_ns=timestamp_ns,
                    )
                )
            records.append(
                to_mutation_record(
                    stream_name=self.stream_name,
                    version_end=to_version_end(
                        commit_version,
                        len(version_group.mutations),
                        timestamp_ns,
                    ),
                    bridge_timestamp_ns=timestamp_ns,
                )
            )

        if not records:
            last_consumed_version = getattr(batch, "last_consumed_version", -1)
            if last_consumed_version >= 0:
                records.append(
                    to_mutation_record(
                        stream_name=self.stream_name,
                        version_end=to_version_end(
                            last_consumed_version,
                            0,
                            timestamp_ns,
                        ),
                        bridge_timestamp_ns=timestamp_ns,
                    )
                )

        if records:
            self._awaiting_ack = True

        return records

    def acknowledge(self) -> None:
        """Acknowledge consumed mutations up to the latest delivered position.

        Advances the cluster-side retention watermark (minVersion) to
        last_consumed_version + 1. Must be called only after records are
        durably processed/checkpointed.
        """
        if self._pending_mutations or self._pending_records:
            raise CDCError(
                "Cannot acknowledge while consumed CDC records remain undelivered."
            )

        try:
            self.consumer.acknowledge().wait()
        except Exception as exc:
            if self._is_closed:
                raise CDCConsumerClosedError("CDC consumer is closed.") from exc
            raise CDCError(f"Failed to acknowledge CDC position: {exc}") from exc
        self._awaiting_ack = False
        self._pending_auto_ack = False

    def get_position(self) -> Any:
        """Get the current consumer position (CdcCursor: stream_id, version).

        Returns:
            CdcCursor representing current position.
        """
        return self.consumer.get_position()

    def close(self) -> None:
        """Close the consumer handle.

        Idempotent. Does NOT acknowledge uncommitted mutations or remove the stream.
        """
        self._is_closed = True
        self._pending_mutations.clear()
        self._pending_records.clear()
        self._pending_auto_ack = False
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:
                pass
            self._consumer = None

    def __enter__(self) -> Self:
        """Enter context manager, starting consumer."""
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit context manager, closing consumer handle."""
        self.close()

    def stream_mutations(
        self,
        max_mutations: int | None = None,
        auto_ack: bool = False,
    ) -> Iterator[mutations_pb2.FDBMutation]:
        """Continuously stream individual FDBMutation messages.

        Args:
            max_mutations: Optional limit on the number of mutations to yield.
            auto_ack: If True, acknowledges each consumed batch and continues polling.
                If False, returns after one complete native reply so the caller can
                checkpoint and acknowledge before requesting another reply.

        Yields:
            FDBMutation messages in commit and sequence order.

        Raises:
            ValueError: If max_mutations is not positive.
            CDCError: If record envelopes from a previous reply remain undelivered.
        """
        if max_mutations is not None and max_mutations < 1:
            raise ValueError("max_mutations must be positive")

        yielded_count = 0
        while not self._is_closed:
            if self._pending_records:
                raise CDCError(
                    "Pending record envelopes must be drained with stream_records()."
                )

            if not self._pending_mutations:
                self._pending_mutations.extend(self.poll())
                self._pending_auto_ack = auto_ack

            if not self._pending_mutations:
                continue

            self._pending_auto_ack = self._pending_auto_ack or auto_ack
            while self._pending_mutations:
                mutation = self._pending_mutations.popleft()
                yield mutation
                yielded_count += 1
                if (
                    max_mutations is not None
                    and yielded_count >= max_mutations
                    and self._pending_mutations
                ):
                    return

            if self._pending_auto_ack:
                self.acknowledge()
            else:
                return
            if max_mutations is not None and yielded_count >= max_mutations:
                return

    def stream_records(
        self,
        max_records: int | None = None,
        auto_ack: bool = False,
    ) -> Iterator[mutations_pb2.FDBMutationRecord]:
        """Continuously stream FDBMutationRecord envelopes.

        Args:
            max_records: Optional limit on the number of records to yield.
            auto_ack: If True, acknowledges each consumed batch and continues polling.
                If False, returns after one complete native reply so the caller can
                checkpoint and acknowledge before requesting another reply.

        Yields:
            FDBMutationRecord envelopes containing batched mutations.

        Raises:
            ValueError: If max_records is not positive.
            CDCError: If mutations from a previous reply remain undelivered.
        """
        if max_records is not None and max_records < 1:
            raise ValueError("max_records must be positive")

        yielded_count = 0
        while not self._is_closed:
            if self._pending_mutations:
                raise CDCError(
                    "Pending mutations must be drained with stream_mutations()."
                )

            if not self._pending_records:
                self._pending_records.extend(self.poll_records())
                self._pending_auto_ack = auto_ack

            if not self._pending_records:
                continue

            self._pending_auto_ack = self._pending_auto_ack or auto_ack
            while self._pending_records:
                record = self._pending_records.popleft()
                yield record
                yielded_count += 1
                if (
                    max_records is not None
                    and yielded_count >= max_records
                    and self._pending_records
                ):
                    return

            if self._pending_auto_ack:
                self.acknowledge()
            else:
                return
            if max_records is not None and yielded_count >= max_records:
                return
