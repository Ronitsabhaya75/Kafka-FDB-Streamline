import os

from hypothesis import settings

settings.register_profile("dev", deadline=None)
settings.register_profile(
    "ci", deadline=None, derandomize=True, max_examples=200, print_blob=True
)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))
