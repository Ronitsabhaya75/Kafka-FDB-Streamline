"""Verify FDB read/write correctness in a live cluster.

Used by the live-cluster-tests workflow to write test mutations into
a running FDB cluster, read them back, and confirm data integrity.
"""

import fdb

fdb.api_version(800)

db = fdb.open()


@fdb.transactional
def write_test_data(tr: fdb.Transaction) -> None:
    """Write 20 test key-value pairs into the 'test:' keyspace."""
    for i in range(20):
        tr[f"test:{i:04d}".encode()] = f"value_{i}".encode()


@fdb.transactional
def read_test_data(tr: fdb.Transaction) -> int:
    """Read all keys in the 'test:' keyspace and return count."""
    count = 0
    for _k, _v in tr.get_range(b"test:\x00", b"test:\xff"):
        count += 1
    return count


def main() -> None:
    """Write test data, read it back, and assert correctness."""
    write_test_data(db)
    print("Wrote 20 test keys to FDB")

    count = read_test_data(db)
    print(f"Read back {count} keys from FDB")
    assert count == 20, f"Expected 20 keys, got {count}"
    print("FDB read/write verification passed")


if __name__ == "__main__":
    main()
