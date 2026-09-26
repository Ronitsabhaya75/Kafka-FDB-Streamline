from hypothesis import strategies as st

from tests.serialization import doubles

# Bounds are literals on purpose. Importing the package's constants would make
# these tests agree with whatever value the constants held.
type_codes = st.integers(min_value=0, max_value=255)
keys = st.binary(max_size=64)
values = st.binary(max_size=256)
native_mutations = st.builds(doubles.NativeMutation, type_codes, keys, values)
groups = st.lists(native_mutations, max_size=20)

fdb_versions = st.integers(min_value=0, max_value=2**63 - 1)
sequence_nos = st.integers(min_value=0, max_value=2**32 - 1)

# Surrogates do not encode as UTF-8 and "" is rejected. Every other str is legal.
stream_names = st.text(
    alphabet=st.characters(exclude_categories=("Cs",)), min_size=1, max_size=40
)
bridge_timestamps_ns = st.integers(min_value=0, max_value=253_402_300_799_999_999_999)
