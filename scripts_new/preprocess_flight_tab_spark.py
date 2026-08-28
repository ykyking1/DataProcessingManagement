"""Spark preprocessing for dashboard-ready flight telemetry TAB datasets."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import TYPE_CHECKING

try:
    from scripts_new.preprocess_tab_spark import (
        DEFAULT_MAX_COLUMNS,
        DEFAULT_SPARK_MASTER,
        DEFAULT_TIMESTAMP_FORMAT,
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
        DEFAULT_TIMESTAMP_FORMAT,
        DEFAULT_ZSTD_LEVEL,
        create_spark_session,
        read_tab_dataframe,
        spark_readable_tab_input,
        write_processed_tab_zstd,
    )

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


TIME_COLUMN = "time"
STRING_COLUMNS = ("image_name", "class", "flight_id")
NUMERIC_COLUMNS = (
    "latitude",
    "longitude",
    "altitude",
    "velocity_x",
    "velocity_y",
    "velocity_z",
    "roll",
    "pitch",
    "yaw",
    "box_x",
    "box_y",
    "box_w",
    "box_h",
)
FLIGHT_COLUMNS = [
    TIME_COLUMN,
    "latitude",
    "longitude",
    "altitude",
    "velocity_x",
    "velocity_y",
    "velocity_z",
    "roll",
    "pitch",
    "yaw",
    "image_name",
    "box_x",
    "box_y",
    "box_w",
    "box_h",
    "class",
    "flight_id",
]


def _quote_identifier(column_name: str) -> str:
    return f"`{column_name.replace('`', '``')}`"


def preprocess_flight_dataframe(
    dataframe: "DataFrame",
    *,
    timestamp_format: str = DEFAULT_TIMESTAMP_FORMAT,
) -> "DataFrame":
    """Validate the ordered schema and cast telemetry columns in one projection."""

    if dataframe.columns != FLIGHT_COLUMNS:
        raise ValueError(
            "Flight TAB columns must exactly match the dashboard contract. "
            f"Expected {FLIGHT_COLUMNS}; received {dataframe.columns}."
        )
    if len(dataframe.columns) != len(set(dataframe.columns)):
        raise ValueError("Duplicate column names are not supported.")

    escaped_format = timestamp_format.replace("'", "''")
    expressions: list[str] = []
    for column_name in FLIGHT_COLUMNS:
        quoted = _quote_identifier(column_name)
        if column_name == TIME_COLUMN:
            expressions.append(
                f"try_to_timestamp(trim({quoted}), '{escaped_format}') AS {quoted}"
            )
        elif column_name in NUMERIC_COLUMNS:
            expressions.append(f"cast(trim({quoted}) AS double) AS {quoted}")
        elif column_name == "class":
            expressions.append(f"lower(trim({quoted})) AS {quoted}")
        else:
            expressions.append(f"trim({quoted}) AS {quoted}")
    return dataframe.selectExpr(*expressions)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preprocess dashboard-ready flight TAB data with Spark."
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--max-columns", type=int, default=DEFAULT_MAX_COLUMNS)
    parser.add_argument("--timestamp-format", default=DEFAULT_TIMESTAMP_FORMAT)
    parser.add_argument("--spark-master", default=DEFAULT_SPARK_MASTER)
    parser.add_argument("--zstd-level", type=int, default=DEFAULT_ZSTD_LEVEL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session(
        app_name="flight-telemetry-preprocessing",
        master=args.spark_master,
    )
    try:
        with spark_readable_tab_input(args.input) as readable_input:
            source = read_tab_dataframe(
                spark,
                readable_input,
                max_columns=args.max_columns,
            )
            processed = preprocess_flight_dataframe(
                source,
                timestamp_format=args.timestamp_format,
            )
            part_count = write_processed_tab_zstd(
                processed,
                args.output,
                compression_level=args.zstd_level,
            )
        print(
            f"Flight preprocessing completed: {args.output} "
            f"({part_count} ZSTD part files)"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
