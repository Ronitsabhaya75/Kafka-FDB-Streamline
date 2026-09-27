"""Live integration tests for FDBClient and FDBMutationListener against FoundationDB."""

import os
import time

import pytest

from fdbkafka.cdc.v1 import mutations_pb2
from src.cdc.client import FDBClient
from src.cdc.listener import FDBMutationListener, strinc

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("FDB_CDC_INTEGRATION") != "1",
        reason="set FDB_CDC_INTEGRATION=1 to run live FoundationDB tests",
    ),
]


def test_live_cdc_mutation_listener() -> None:
    """Test full CDC listener loop against a live FoundationDB cluster."""
    client = FDBClient(api_version=800)
    db = client.open()

    stream_name = b"integ_listener_stream"
    prefix = b"integ_cdc_key:"
    key_range = (prefix, strinc(prefix))

    # Clean up any existing stream from a previous run
    try:
        client.remove_cdc_stream(stream_name)
    except Exception:
        pass

    with FDBMutationListener(
        client=client,
        stream_name=stream_name,
        key_range=key_range,
        auto_register=True,
    ) as listener:
        # Commit a set mutation
        k1 = prefix + b"alpha"
        v1 = b"value_alpha"
        db[k1] = v1

        # Poll for mutations with bounded retry
        found_mutations: list[mutations_pb2.FDBMutation] = []
        for _ in range(10):
            batch = listener.poll()
            if batch:
                found_mutations.extend(batch)
                break
            time.sleep(0.3)

        assert len(found_mutations) > 0
        first_m = found_mutations[0]
        assert first_m.WhichOneof("mutation") == "single_key_mutation"
        assert first_m.single_key_mutation.key == k1
        assert first_m.single_key_mutation.value == v1
        assert (
            first_m.single_key_mutation.mutation_type
            == mutations_pb2.FDBSingleKeyMutation.MUTATION_TYPE_SET_VALUE
        )
        assert first_m.version_index.fdb_version > 0

        # Acknowledge first batch before polling for next batch
        listener.acknowledge()

        # Commit a range clear
        k2 = prefix + b"beta"
        db[k2] = b"value_beta"
        del db[k1 : prefix + b"\xff"]

        found_clear = False
        for _ in range(10):
            batch = listener.poll()
            for m in batch:
                if m.WhichOneof("mutation") == "clear_range":
                    found_clear = True
                    break
            if found_clear:
                break
            time.sleep(0.3)

        assert found_clear

        # Acknowledge and verify position
        listener.acknowledge()
        pos = listener.get_position()
        assert pos is not None
        assert pos.last_consumed_version > 0

    # Clean up test keys and stream
    del db[prefix : strinc(prefix)]
    try:
        client.remove_cdc_stream(stream_name)
    except Exception:
        pass
