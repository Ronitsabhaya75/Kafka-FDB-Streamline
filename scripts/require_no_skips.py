"""Fail unless a pytest JUnit XML report shows tests ran and none were skipped.

Pytest exits 0 when every test skips, so its exit code alone cannot prove a
self-skipping suite actually ran.

Usage: python scripts/require_no_skips.py REPORT.xml
"""

import sys
import xml.etree.ElementTree as ET


def check_report(path: str) -> str | None:
    """Check one JUnit XML report.

    Args:
        path: Path to the report written by `pytest --junitxml`.

    Returns:
        A failure message, or None if at least one test ran and none skipped.
    """
    suites = ET.parse(path).getroot().iter("testsuite")
    # Direct indexing: a report without these attributes must crash, not pass.
    totals = [(int(s.attrib["tests"]), int(s.attrib["skipped"])) for s in suites]
    collected = sum(tests for tests, _ in totals)
    skipped = sum(skips for _, skips in totals)
    if collected == 0:
        return "no tests collected"
    if skipped:
        return f"{skipped} of {collected} tests skipped"
    return None


def main() -> int:
    """Check the report named on the command line.

    Returns:
        Process exit code: 0 on success, 1 on failure, 2 on bad usage.
    """
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    failure = check_report(sys.argv[1])
    if failure:
        print(f"::error::{failure}: this job must actually run them")
        return 1
    print("No tests skipped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
