from hypothesis import strategies as st

from tests.serialization import doubles

# Bounds are literals on purpose: an independent statement of the accepted
# domains, not a re-read of the package's own constants.
type_codes = st.integers(min_value=0, max_value=255)
keys = st.binary(max_size=64)
values = st.binary(max_size=256)
native_mutations = st.builds(doubles.NativeMutation, type_codes, keys, values)
groups = st.lists(native_mutations, max_size=20)

fdb_versions = st.integers(min_value=0, max_value=2**63 - 1)
sequence_nos = st.integers(min_value=0, max_value=2**32 - 1)

# Surrogates are not UTF-8-encodable and "" is rejected; everything else is legal.
stream_names = st.text(
    alphabet=st.characters(exclude_categories=("Cs",)), min_size=1, max_size=40
)
bridge_timestamps_ns = st.integers(min_value=0, max_value=253_402_300_799_999_999_999)
