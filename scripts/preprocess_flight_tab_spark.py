"""Spark preprocessing for dashboard-ready flight telemetry TAB datasets."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import TYPE_CHECKING

try:
    from scripts.preprocess_tab_spark import (
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

# Dashboard-serving flight telemetry now follows the native AU-AIR frame
# schema (the only deviation is fixing the ``longtitude`` header typo).
# The generator (generate_and_upload_flight_tab_to_minio.py) and the AU-AIR
# converter (convert_auair_tab.py) emit exactly these columns in this order.
STRING_COLUMNS = ("image_name", "platform", "flight_id")
FLOAT_COLUMNS = (
    "longitude",
    "latitude",
    "altitude",
    "linear_x",
    "linear_y",
    "linear_z",
    "angle_phi",
    "angle_theta",
    "angle_psi",
)
INTEGER_COLUMNS = (
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
)
# Kept for callers that iterate every non-timestamp numeric column.
NUMERIC_COLUMNS = FLOAT_COLUMNS + INTEGER_COLUMNS
REQUIRED_NUMERIC_COLUMNS = NUMERIC_COLUMNS
FLIGHT_COLUMNS = [
    "flight_id",
    TIME_COLUMN,
    "image_name",
    "image_width",
    "image_height",
    "platform",
    "longitude",
    "latitude",
    "altitude",
    "linear_x",
    "linear_y",
    "linear_z",
    "angle_phi",
    "angle_theta",
    "angle_psi",
    "num_objects",
    "obj_human",
    "obj_car",
    "obj_truck",
    "obj_van",
    "obj_motorbike",
    "obj_bicycle",
    "obj_bus",
    "obj_trailer",
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
        elif column_name in FLOAT_COLUMNS:
            expressions.append(f"cast(trim({quoted}) AS double) AS {quoted}")
        elif column_name in INTEGER_COLUMNS:
            expressions.append(
                f"cast(cast(trim({quoted}) AS double) AS int) AS {quoted}"
            )
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
