"""Generate small, wide .tab fixtures for MX schema pipeline tests.

The generated files simulate deliberately untidy raw input before cleanup and
ZSTD compression. Each file contains exactly 10 data rows by default. Its
logical column count matches the MX tier: the first two columns are
``timestamp`` and ``aircraft_type``; all remaining columns are deterministic
random numeric features.

Cleanup fixtures intentionally contain:

* one extra trailing tab on the header and every data row,
* one accidental repeated tab inside every fourth data row,
* surrounding spaces in ``aircraft_type`` on alternating rows,
* surrounding spaces in the first numeric feature on every third row.

No null or invalid numeric value is injected; these files test structural
cleanup only.

Outputs are written under ``data/raw/``, which is ignored by Git.
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "mx_small_fixtures"
MX_COLUMN_COUNTS = (10_000, 20_000, 30_000, 40_000, 50_000)
BASE_TIMESTAMP = datetime(2026, 1, 1, tzinfo=timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate 10-row MX10000-MX50000 raw .tab fixtures."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=10,
        help="Number of data rows per file (default: 10)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Base random seed for reproducible fixtures (default: 42)",
    )
    return parser.parse_args()


def format_timestamp(row_index: int) -> str:
    timestamp = BASE_TIMESTAMP + timedelta(seconds=row_index)
    return timestamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def generate_fixture(
    output_dir: Path,
    total_columns: int,
    row_count: int,
    base_seed: int,
) -> Path:
    if total_columns < 3:
        raise ValueError("A fixture needs at least three columns.")
    if row_count < 1:
        raise ValueError("Row count must be positive.")

    aircraft_type = f"MX{total_columns}"
    feature_count = total_columns - 2
    feature_names = [f"feature_{index:05d}" for index in range(1, feature_count + 1)]
    output_path = output_dir / f"{aircraft_type.lower()}_{row_count}rows.tab"
    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")
    rng = random.Random(base_seed + total_columns)

    with temporary_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.writer(output_file, delimiter="\t", lineterminator="\n")
        # The final empty field intentionally creates one trailing tab.
        writer.writerow(["timestamp", "aircraft_type", *feature_names, ""])

        for row_index in range(row_count):
            feature_values = [f"{rng.random():.6f}" for _ in range(feature_count)]
            if row_index % 3 == 0:
                feature_values[0] = f"  {feature_values[0]}  "

            aircraft_value = aircraft_type
            if row_index % 2 == 0:
                aircraft_value = f"  {aircraft_type}  "

            row = [format_timestamp(row_index), aircraft_value, *feature_values]
            if row_index % 4 == 0:
                # Insert an accidental empty field: feature_00001\t\tfeature_00002.
                row.insert(3, "")

            # The final empty field intentionally creates one trailing tab.
            writer.writerow([*row, ""])

    temporary_path.replace(output_path)
    return output_path


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Output directory: {output_dir}")
    for total_columns in MX_COLUMN_COUNTS:
        output_path = generate_fixture(
            output_dir=output_dir,
            total_columns=total_columns,
            row_count=args.rows,
            base_seed=args.seed,
        )
        size_mib = output_path.stat().st_size / (1024 * 1024)
        print(
            f"{output_path.name}: {args.rows} rows, "
            f"{total_columns:,} columns, {size_mib:.2f} MiB"
        )


if __name__ == "__main__":
    main()
