"""Polars preprocessing for dynamic-column AU-AIR synthetic TAB datasets."""

from __future__ import annotations

import argparse
import re
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path

import polars as pl
import zstandard as zstd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FLIGHT_ID_COLUMN = "flight_id"
TIME_COLUMN = "time"
REQUIRED_PREFIX = [FLIGHT_ID_COLUMN, TIME_COLUMN]
DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"
DEFAULT_MAX_COLUMNS = 100_000
DEFAULT_ZSTD_LEVEL = 12
MIN_COLUMN_COUNT = 17
ZSTD_SUFFIXES = {".zst", ".zstd"}

STRING_BASE_COLUMNS = {"image_name", "platform"}
INTEGER_BASE_COLUMNS = {
    "image_width",
    "image_height",
    "num_objects",
    "obj_human",
    "obj_car",
    "obj_truck",
    "obj_van",
    "obj_motorbike",
    "obj_bicycle",
    "obj_bus",
    "obj_trailer",
}
DOUBLE_BASE_COLUMNS = {
    "longtitude",
    "latitude",
    "altitude",
    "linear_x",
    "linear_y",
    "linear_z",
    "angle_phi",
    "angle_theta",
    "angle_psi",
}
KNOWN_BASE_COLUMNS = (
    STRING_BASE_COLUMNS | INTEGER_BASE_COLUMNS | DOUBLE_BASE_COLUMNS
)


def _base_column_name(column_name: str) -> str:
    """Strip the numeric suffix used by repeated AU-AIR column blocks."""

    return re.sub(r"\d+$", "", column_name)


def _chrono_timestamp_format(java_format: str) -> tuple[str, bool]:
    """Translate the supported Spark timestamp pattern to Polars/Chrono."""

    normalized = java_format.replace("'", "")
    has_timezone = False
    for timezone_suffix in ("XXX", "XX", "X", "Z"):
        if normalized.endswith(timezone_suffix):
            normalized = normalized[: -len(timezone_suffix)]
            has_timezone = True
            break

    replacements = (
        ("yyyy", "%Y"),
        (".SSS", "%.3f"),
        ("SSS", "%3f"),
        ("MM", "%m"),
        ("dd", "%d"),
        ("HH", "%H"),
        ("mm", "%M"),
        ("ss", "%S"),
    )
    for java_token, chrono_token in replacements:
        normalized = normalized.replace(java_token, chrono_token)

    format_probe = re.sub(r"%\.\d+f|%\d+f|%[A-Za-z]", "", normalized)
    unsupported = re.findall(r"[A-Za-z]+", format_probe.replace("T", ""))
    if unsupported:
        raise ValueError(
            "Unsupported timestamp format token(s) for Polars: "
            f"{unsupported}."
        )
    return normalized, has_timezone


def _decompress_zstd_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    decompressor = zstd.ZstdDecompressor()
    with source.open("rb") as input_file, destination.open("wb") as output_file:
        decompressor.copy_stream(input_file, output_file)


@contextmanager
def polars_readable_tab_inputs(input_path: Path | str):
    """Yield plain TAB inputs, expanding local ZSTD files when necessary."""

    path_text = str(input_path)
    if "://" in path_text:
        yield [path_text]
        return

    source = Path(path_text).resolve()
    if source.is_file():
        zstd_files = [source] if source.suffix.lower() in ZSTD_SUFFIXES else []
        plain_files = [source] if not zstd_files else []
    elif source.is_dir():
        zstd_files = sorted(
            path
            for path in source.rglob("*")
            if path.is_file() and path.suffix.lower() in ZSTD_SUFFIXES
        )
        plain_files = (
            []
            if zstd_files
            else sorted(
                path
                for path in source.rglob("*")
                if path.is_file()
                and not path.name.startswith((".", "_"))
            )
        )
    else:
        raise FileNotFoundError(f"Input path not found: {source}")

    if not zstd_files:
        if not plain_files:
            raise FileNotFoundError(f"No readable TAB files found: {source}")
        yield plain_files
        return

    with tempfile.TemporaryDirectory(prefix="dvc_tab_zstd_") as temporary_dir:
        staging_directory = Path(temporary_dir)
        staged_files = []
        for index, zstd_file in enumerate(zstd_files):
            staged_file = staging_directory / f"part-{index:05d}.tab"
            _decompress_zstd_file(zstd_file, staged_file)
            staged_files.append(staged_file)
        yield staged_files


def read_auair_lazyframe(
    input_paths: list[Path | str],
    *,
    max_columns: int = DEFAULT_MAX_COLUMNS,
) -> pl.LazyFrame:
    """Read header-bearing TAB inputs as strings for explicit conversion."""

    if max_columns < MIN_COLUMN_COUNT:
        raise ValueError(
            f"max_columns must be at least {MIN_COLUMN_COUNT}; "
            f"received {max_columns}."
        )
    source = pl.scan_csv(
        input_paths,
        separator="\t",
        has_header=True,
        infer_schema=False,
        encoding="utf8",
        ignore_errors=False,
        truncate_ragged_lines=False,
    )
    column_count = len(source.collect_schema())
    if column_count > max_columns:
        raise ValueError(
            f"AU-AIR data exceeds max_columns={max_columns}; "
            f"received {column_count}."
        )
    return source


def preprocess_auair_lazyframe(
    dataframe: pl.LazyFrame,
    *,
    timestamp_format: str = DEFAULT_TIMESTAMP_FORMAT,
    min_columns: int = MIN_COLUMN_COUNT,
) -> pl.LazyFrame:
    """Apply the existing AU-AIR trim, parse, cast, and null rules."""

    columns = dataframe.collect_schema().names()
    if columns[:2] != REQUIRED_PREFIX:
        raise ValueError(
            f"AU-AIR columns must start with {REQUIRED_PREFIX}; "
            f"received {columns[:2]}."
        )
    if len(columns) < min_columns:
        raise ValueError(
            f"AU-AIR data must contain at least {min_columns} columns; "
            f"received {len(columns)}."
        )
    if len(columns) != len(set(columns)):
        raise ValueError("Duplicate AU-AIR column names are not supported.")

    unknown_columns = [
        column_name
        for column_name in columns[2:]
        if _base_column_name(column_name) not in KNOWN_BASE_COLUMNS
    ]
    if unknown_columns:
        raise ValueError(f"Unknown AU-AIR columns: {unknown_columns[:10]}")

    chrono_format, requires_timezone = _chrono_timestamp_format(timestamp_format)
    time_text = pl.col(TIME_COLUMN).str.strip_chars()
    timezone_timestamp = time_text.str.to_datetime(
        format=f"{chrono_format}%#z",
        strict=False,
    ).dt.replace_time_zone(None)
    if requires_timezone:
        timestamp_expression = timezone_timestamp
    else:
        local_timestamp = time_text.str.to_datetime(
            format=chrono_format,
            strict=False,
        )
        timestamp_expression = pl.coalesce(local_timestamp, timezone_timestamp)

    expressions = [
        pl.col(FLIGHT_ID_COLUMN).str.strip_chars().alias(FLIGHT_ID_COLUMN),
        timestamp_expression.alias(TIME_COLUMN),
    ]
    for column_name in columns[2:]:
        trimmed = pl.col(column_name).str.strip_chars()
        base_name = _base_column_name(column_name)
        if base_name in STRING_BASE_COLUMNS:
            expression = trimmed
        elif base_name in INTEGER_BASE_COLUMNS:
            expression = trimmed.cast(pl.Int64, strict=False)
        else:
            expression = trimmed.cast(pl.Float64, strict=False)
        expressions.append(expression.alias(column_name))
    return dataframe.select(expressions)


def _remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def write_processed_tab_zstd(
    dataframe: pl.LazyFrame,
    output_path: Path | str,
    *,
    compression_level: int = DEFAULT_ZSTD_LEVEL,
) -> int:
    """Stream one tab-separated output part and compress it with ZSTD."""

    path_text = str(output_path)
    if "://" in path_text:
        raise ValueError(
            "The standalone DVC repro writer requires a local output path."
        )

    destination = Path(path_text).resolve()
    try:
        destination.relative_to(PROJECT_ROOT)
    except ValueError as error:
        raise ValueError("DVC repro output must be inside the project.") from error
    if destination == PROJECT_ROOT:
        raise ValueError("DVC repro output cannot be the project root.")

    staging_directory = destination.with_name(destination.name + ".polars-tmp")
    compressed_directory = destination.with_name(destination.name + ".zstd-tmp")
    for temporary_path in (staging_directory, compressed_directory):
        _remove_path(temporary_path)

    staging_directory.mkdir(parents=True)
    staged_file = staging_directory / "part-00000.tab"
    try:
        dataframe.sink_csv(
            staged_file,
            include_header=True,
            separator="\t",
            line_terminator="\n",
            datetime_format="%Y-%m-%dT%H:%M:%S%.3f",
            null_value="",
            maintain_order=True,
        )

        compressed_directory.mkdir(parents=True)
        compressed_file = compressed_directory / "part-00000.tab.zst"
        compressor = zstd.ZstdCompressor(level=compression_level)
        with staged_file.open("rb") as input_file, compressed_file.open(
            "wb"
        ) as output_file:
            compressor.copy_stream(input_file, output_file)

        _remove_path(destination)
        compressed_directory.replace(destination)
    finally:
        _remove_path(staging_directory)
        if compressed_directory.exists():
            _remove_path(compressed_directory)

    return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preprocess dynamic-column AU-AIR TAB data with Polars."
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--max-columns", type=int, default=DEFAULT_MAX_COLUMNS)
    parser.add_argument("--min-columns", type=int, default=MIN_COLUMN_COUNT)
    parser.add_argument("--timestamp-format", default=DEFAULT_TIMESTAMP_FORMAT)
    parser.add_argument("--zstd-level", type=int, default=DEFAULT_ZSTD_LEVEL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with polars_readable_tab_inputs(args.input) as inputs:
        source = read_auair_lazyframe(inputs, max_columns=args.max_columns)
        processed = preprocess_auair_lazyframe(
            source,
            timestamp_format=args.timestamp_format,
            min_columns=args.min_columns,
        )
        column_count = len(processed.collect_schema())
        part_count = write_processed_tab_zstd(
            processed,
            args.output,
            compression_level=args.zstd_level,
        )
    print(
        f"AU-AIR preprocessing completed: {args.output} "
        f"({column_count} columns, {part_count} ZSTD part files)"
    )


if __name__ == "__main__":
    main()
