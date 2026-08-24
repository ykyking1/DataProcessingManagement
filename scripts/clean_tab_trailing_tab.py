"""Stream-clean whitespace artifacts from a .tab file.

The cleaner is independent of row and column counts. It derives the logical
column count from the header, processes one physical line at a time, and writes
another uncompressed .tab file.

Cleanup rules:

* trim surrounding ASCII whitespace from every field,
* remove extra empty fields caused by trailing tab characters,
* remove surplus empty fields caused by repeated internal tabs,
* skip completely blank physical lines,
* preserve internal empty fields when the row already has the expected width,
* reject rows whose column count still differs from the header after cleanup.

Example:

    python scripts/clean_tab_trailing_tab.py input.tab output.tab
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TabCleanupResult:
    """Summary returned after a successful cleanup."""

    output_path: Path
    column_count: int
    rows_written: int
    fields_trimmed: int
    trailing_fields_removed: int
    surplus_empty_fields_removed: int
    blank_lines_skipped: int


def _split_and_trim(raw_line: bytes) -> tuple[list[bytes], int]:
    """Split one physical line and trim its fields without decoding values."""

    fields = raw_line.rstrip(b"\r\n").split(b"\t")
    cleaned_fields = [field.strip() for field in fields]
    trimmed_count = sum(
        original != cleaned for original, cleaned in zip(fields, cleaned_fields)
    )
    return cleaned_fields, trimmed_count


def _remove_extra_trailing_fields(
    fields: list[bytes], expected_columns: int | None = None
) -> int:
    """Remove empty fields added by trailing tabs and return their count."""

    original_count = len(fields)
    minimum_count = expected_columns if expected_columns is not None else 0
    while len(fields) > minimum_count and fields[-1] == b"":
        fields.pop()
    return original_count - len(fields)


def _remove_surplus_empty_fields(fields: list[bytes], expected_columns: int) -> int:
    """Remove only enough empty fields to restore the expected row width.

    An empty field is preserved when a row already matches the header width,
    because it can represent a real missing value. If the row is wider than the
    header, empty fields created by repeated delimiters are removed until the
    expected width is reached.
    """

    surplus = len(fields) - expected_columns
    if surplus <= 0:
        return 0

    cleaned_fields: list[bytes] = []
    removed = 0
    for field in fields:
        if field == b"" and removed < surplus:
            removed += 1
            continue
        cleaned_fields.append(field)

    fields[:] = cleaned_fields
    return removed


def clean_tab_file(input_path: Path | str, output_path: Path | str) -> TabCleanupResult:
    """Clean one .tab file and write the normalized result to a new .tab file.

    The function uses memory proportional to a single row, rather than the full
    dataset. Input and output must be different paths so the source remains
    available if cleanup or structural validation fails.
    """

    source = Path(input_path).resolve()
    destination = Path(output_path).resolve()

    if not source.is_file():
        raise FileNotFoundError(f"Input .tab file not found: {source}")
    if source == destination:
        raise ValueError("Input and output paths must be different.")

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = destination.with_suffix(destination.suffix + ".tmp")

    rows_written = 0
    fields_trimmed = 0
    trailing_fields_removed = 0
    surplus_empty_fields_removed = 0
    blank_lines_skipped = 0

    try:
        with source.open("rb") as input_file, temporary_output.open("wb") as output_file:
            raw_header = input_file.readline()
            if not raw_header:
                raise ValueError("Input .tab file is empty.")

            header, trimmed = _split_and_trim(raw_header)
            fields_trimmed += trimmed
            trailing_fields_removed += _remove_extra_trailing_fields(header)
            header_empty_count = sum(field == b"" for field in header)
            if header_empty_count:
                header[:] = [field for field in header if field != b""]
                surplus_empty_fields_removed += header_empty_count
            if not header:
                raise ValueError("Header has no named columns after cleanup.")

            expected_columns = len(header)
            output_file.write(b"\t".join(header) + b"\n")

            for line_number, raw_line in enumerate(input_file, start=2):
                if not raw_line.strip():
                    blank_lines_skipped += 1
                    continue

                fields, trimmed = _split_and_trim(raw_line)
                fields_trimmed += trimmed
                trailing_fields_removed += _remove_extra_trailing_fields(
                    fields, expected_columns
                )
                surplus_empty_fields_removed += _remove_surplus_empty_fields(
                    fields, expected_columns
                )

                if len(fields) != expected_columns:
                    raise ValueError(
                        f"Line {line_number} has {len(fields):,} columns after "
                        f"cleanup; expected {expected_columns:,}."
                    )

                output_file.write(b"\t".join(fields) + b"\n")
                rows_written += 1

        temporary_output.replace(destination)
    except Exception:
        temporary_output.unlink(missing_ok=True)
        raise

    return TabCleanupResult(
        output_path=destination,
        column_count=expected_columns,
        rows_written=rows_written,
        fields_trimmed=fields_trimmed,
        trailing_fields_removed=trailing_fields_removed,
        surplus_empty_fields_removed=surplus_empty_fields_removed,
        blank_lines_skipped=blank_lines_skipped,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove surrounding whitespace and trailing tabs from a .tab file."
    )
    parser.add_argument("input", type=Path, help="Source .tab file")
    parser.add_argument("output", type=Path, help="Cleaned .tab output file")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = clean_tab_file(args.input, args.output)
    print(f"Output: {result.output_path}")
    print(f"Rows: {result.rows_written:,}")
    print(f"Columns: {result.column_count:,}")
    print(f"Trimmed fields: {result.fields_trimmed:,}")
    print(f"Removed trailing fields: {result.trailing_fields_removed:,}")
    print(
        "Removed surplus empty fields: "
        f"{result.surplus_empty_fields_removed:,}"
    )
    print(f"Skipped blank lines: {result.blank_lines_skipped:,}")


if __name__ == "__main__":
    main()
