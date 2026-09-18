"""Change detectors, not contract.

These assert byte stability upstream does not guarantee. A failure after a
protobuf or gencode upgrade means: review the diff, then re-bless. A failure at
any other time is a real regression. The literals are the bytes of the equivalent
hand-built `mutations_pb2` messages.
"""

from src.serialization import serialize_batch, serialize_mutation, serialize_version_end
from tests.serialization.doubles import ADD, CLEAR, ENV, V

G1 = bytes.fromhex(
    "0a066f7264657273120b0880e2cfaa0610959aef3a1a1a0a09088780808080201003"
    "120d0a0415016b001203ff00761802"
)
G2 = bytes.fromhex(
    "0a066f7264657273120b0880e2cfaa0610959aef3a1a140a09088780808080201004"
    "1a070a0161120262ff"
)
G3 = bytes.fromhex(
    "0a066f7264657273120b0880e2cfaa0610959aef3a2216088780808080201005"
    "1a0b0880e2cfaa0610959aef3a"
)
G4 = bytes.fromhex(
    "0a066f7264657273120b0880e2cfaa0610959aef3a2a320a1a0a0908878080808020"
    "1003120d0a0415016b001203ff007618020a140a090887808080802010041a070a01"
    "61120262ff"
)
GOLDEN = (G1, G2, G3, G4)


def test_single_key_mutation_record_bytes() -> None:
    assert serialize_mutation(ADD, fdb_version=V, sequence_no=3, **ENV) == G1


def test_clear_range_mutation_record_bytes() -> None:
    assert serialize_mutation(CLEAR, fdb_version=V, sequence_no=4, **ENV) == G2


def test_version_end_record_bytes() -> None:
    assert serialize_version_end(fdb_version=V, total_mutations=5, **ENV) == G3


def test_batch_record_bytes() -> None:
    data = serialize_batch([ADD, CLEAR], fdb_version=V, first_sequence_no=3, **ENV)

    assert data == G4
