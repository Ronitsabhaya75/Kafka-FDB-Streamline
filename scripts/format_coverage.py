"""Format coverage JSON reports into a GitHub Markdown summary table."""

import json
from pathlib import Path


def main() -> None:
    """Read coverage JSONs and generate Markdown summary table."""
    unit_pct = json.loads(Path("cov_unit.json").read_text())["totals"][
        "percent_covered"
    ]
    int_pct = json.loads(Path("cov_int.json").read_text())["totals"]["percent_covered"]
    comb_pct = json.loads(Path("cov_comb.json").read_text())["totals"][
        "percent_covered"
    ]

    table = (
        "## Code Coverage\n\n"
        "| Tier | Line Coverage |\n"
        "| :--- | :--- |\n"
        f"| Unit | {unit_pct:.2f}% |\n"
        f"| Integration | {int_pct:.2f}% |\n"
        f"| Combined | {comb_pct:.2f}% |\n"
    )
    Path("coverage_summary.md").write_text(table)


if __name__ == "__main__":
    main()
