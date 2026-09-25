"""Shared fixtures and mock factories for Protobuf testing."""

import os
import sys
from typing import Any, NamedTuple

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

_GEN_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "gen"))
if _GEN_PATH not in sys.path:
    sys.path.insert(0, _GEN_PATH)

from fdbkafka.cdc.v1 import mutations_pb2


class MockCdcMutation(NamedTuple):
    """Mimics FoundationDB's native CdcMutation structure."""

    type: int
    param1: bytes  # key or begin_key
    param2: bytes  # value or end_key


class MockCdcVersionedMutations(NamedTuple):
    """Mimics a group of FoundationDB mutations sharing a commit version."""

    version: int
    mutations: tuple[MockCdcMutation, ...]


@pytest.fixture
def sample_timestamp() -> Timestamp:
    """Fixture providing a populated protobuf Timestamp."""
    ts = Timestamp()
    ts.GetCurrentTime()
    return ts


@pytest.fixture
def make_version_index() -> Any:
    """Factory fixture to create FDBVersionIndex messages."""

    def _factory(
        version: int = 100_000,
        sequence_no: int = 0,
    ) -> mutations_pb2.FDBVersionIndex:
        return mutations_pb2.FDBVersionIndex(
            fdb_version=version,
            sequence_no=sequence_no,
        )

    return _factory


@pytest.fixture
def sample_cdc_batch() -> tuple[MockCdcVersionedMutations, ...]:
    """Provides sample CDC versioned mutations representing typical FDB CDC output."""
    return (
        MockCdcVersionedMutations(
            version=1000,
            mutations=(
                MockCdcMutation(
                    type=0,
                    param1=b"users:1001",
                    param2=b'{"name": "Alice"}',
                ),
                MockCdcMutation(
                    type=0,
                    param1=b"users:1002",
                    param2=b'{"name": "Bob"}',
                ),
                MockCdcMutation(
                    type=2,
                    param1=b"counters:visits",
                    param2=(1).to_bytes(8, "little"),
                ),
            ),
        ),
        MockCdcVersionedMutations(
            version=1001,
            mutations=(
                MockCdcMutation(
                    type=1,
                    param1=b"sessions:0000",
                    param2=b"sessions:\xff",
                ),
                MockCdcMutation(
                    type=9,
                    param1=b"logs:audit",
                    param2=b"login_event\n",
                ),
            ),
        ),
    )
