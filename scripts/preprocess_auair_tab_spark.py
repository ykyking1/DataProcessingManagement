"""Spark preprocessing for dynamic-column AU-AIR synthetic TAB datasets."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import TYPE_CHECKING

try:
    from scripts.preprocess_tab_spark import (
        DEFAULT_MAX_COLUMNS,
        DEFAULT_SPARK_MASTER,
        DEFAULT_ZSTD_LEVEL,
        create_spark_session,
        read_tab_dataframe,
        spark_readable_tab_input,
        write_processed_tab_zstd,
    )
except ModuleNotFoundError:
    from preprocess_tab_spark import (
        DEFAULT_MAX_COLUMNS,
        DEFAULT_SPARK_MASTER,
        DEFAULT_ZSTD_LEVEL,
        create_spark_session,
        read_tab_dataframe,
        spark_readable_tab_input,
        write_processed_tab_zstd,
    )

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


FLIGHT_ID_COLUMN = "flight_id"
TIME_COLUMN = "time"
REQUIRED_PREFIX = [FLIGHT_ID_COLUMN, TIME_COLUMN]
DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"
MIN_COLUMN_COUNT = 17

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


def _quote_identifier(column_name: str) -> str:
    return f"`{column_name.replace('`', '``')}`"


def _base_column_name(column_name: str) -> str:
    """Strip the numeric suffix used by repeated AU-AIR column blocks."""

    return re.sub(r"\d+$", "", column_name)


def preprocess_auair_dataframe(
    dataframe: "DataFrame",
    *,
    timestamp_format: str = DEFAULT_TIMESTAMP_FORMAT,
    min_columns: int = MIN_COLUMN_COUNT,
) -> "DataFrame":
    """Cast AU-AIR columns while preserving its generated dynamic schema."""

    columns = dataframe.columns
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

    expressions = [
        f"trim({_quote_identifier(FLIGHT_ID_COLUMN)}) AS `{FLIGHT_ID_COLUMN}`",
        f"trim({_quote_identifier(TIME_COLUMN)}) AS `{TIME_COLUMN}`",
    ]
    for column_name in columns[2:]:
        quoted = _quote_identifier(column_name)
        base_name = _base_column_name(column_name)
        if base_name in STRING_BASE_COLUMNS:
            expressions.append(f"trim({quoted}) AS {quoted}")
        elif base_name in INTEGER_BASE_COLUMNS:
            expressions.append(f"cast(trim({quoted}) AS bigint) AS {quoted}")
        else:
            expressions.append(f"cast(trim({quoted}) AS double) AS {quoted}")

    # Spark SQL string escaping can alter the literal quotes around the ISO
    # ``T``. The column API keeps the datetime pattern intact.
    from pyspark.sql import functions as spark_functions

    processed = dataframe.selectExpr(*expressions)
    timestamp_patterns = [timestamp_format]
    if not timestamp_format.endswith(("X", "XX", "XXX", "Z")):
        timestamp_patterns.append(f"{timestamp_format}X")
    parsed_timestamps = [
        spark_functions.try_to_timestamp(
            spark_functions.col(TIME_COLUMN),
            spark_functions.lit(pattern),
        )
        for pattern in timestamp_patterns
    ]
    return processed.withColumn(
        TIME_COLUMN,
        spark_functions.coalesce(*parsed_timestamps),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preprocess dynamic-column AU-AIR TAB data with Spark."
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--max-columns", type=int, default=DEFAULT_MAX_COLUMNS)
    parser.add_argument("--min-columns", type=int, default=MIN_COLUMN_COUNT)
    parser.add_argument("--timestamp-format", default=DEFAULT_TIMESTAMP_FORMAT)
    parser.add_argument("--spark-master", default=DEFAULT_SPARK_MASTER)
    parser.add_argument("--zstd-level", type=int, default=DEFAULT_ZSTD_LEVEL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session(
        app_name="auair-telemetry-preprocessing",
        master=args.spark_master,
    )
    try:
        with spark_readable_tab_input(args.input) as readable_input:
            source = read_tab_dataframe(
                spark,
                readable_input,
                max_columns=args.max_columns,
            )
            processed = preprocess_auair_dataframe(
                source,
                timestamp_format=args.timestamp_format,
                min_columns=args.min_columns,
            )
            part_count = write_processed_tab_zstd(
                processed,
                args.output,
                compression_level=args.zstd_level,
            )
        print(
            f"AU-AIR preprocessing completed: {args.output} "
            f"({len(processed.columns)} columns, {part_count} ZSTD part files)"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
