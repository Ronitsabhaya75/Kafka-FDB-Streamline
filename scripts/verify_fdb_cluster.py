"""Verify FDB read/write correctness in a live cluster.

Writes test key-value pairs into the 'test:' keyspace of a running FDB
cluster, reads them back, and confirms data integrity.
"""

from typing import Any

import fdb

fdb.api_version(800)


@fdb.transactional
def write_test_data(tr: Any) -> None:
    """Clear test range and write 20 test key-value pairs into 'test:' keyspace."""
    tr.options.set_timeout(5000)
    tr.clear_range(b"test:\x00", b"test:\xff")
    for i in range(20):
        tr[f"test:{i:04d}".encode()] = f"value_{i}".encode()


@fdb.transactional
def read_and_verify_test_data(tr: Any) -> int:
    """Read and verify all keys and values in the 'test:' keyspace."""
    tr.options.set_timeout(5000)
    items = list(tr.get_range(b"test:\x00", b"test:\xff"))
    for i, (k, v) in enumerate(items):
        expected_key = f"test:{i:04d}".encode()
        expected_val = f"value_{i}".encode()
        assert k == expected_key, f"Expected key {expected_key!r}, got {k!r}"
        assert v == expected_val, f"Expected value {expected_val!r}, got {v!r}"
    return len(items)


def main() -> None:
    """Open FDB database, write test data, verify contents, and assert correctness."""
    db = fdb.open()

    write_test_data(db)
    print("Cleared test range and wrote 20 test keys to FDB")

    count = read_and_verify_test_data(db)
    print(f"Verified {count} keys and values match expected test data in FDB")
    assert count == 20, f"Expected 20 keys, got {count}"
    print("FDB read/write verification passed")


if __name__ == "__main__":
    main()
