"""Spark preprocessing functions for cleaned MX .tab/.tab.zst datasets.

The functions in this module do not depend on Dagster, MinIO, DVC, or Great
Expectations. A Dagster asset can import them and supply its own Spark session
and input path.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


TIMESTAMP_COLUMN = "timestamp"
AIRCRAFT_TYPE_COLUMN = "aircraft_type"
DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSX"
DEFAULT_MAX_COLUMNS = 100_000


def _quote_identifier(column_name: str) -> str:
    """Quote a Spark SQL identifier, including names containing backticks."""

    return f"`{column_name.replace('`', '``')}`"


def _normalize_spark_path(input_path: Path | str) -> str:
    """Resolve local paths while preserving Spark-compatible remote URIs."""

    path_text = str(input_path)
    if "://" in path_text:
        return path_text
    return str(Path(path_text).resolve())


def read_tab_dataframe(
    spark: "SparkSession",
    input_path: Path | str,
    *,
    max_columns: int = DEFAULT_MAX_COLUMNS,
) -> "DataFrame":
    """Read a header-bearing .tab or .tab.zst file as a Spark DataFrame.

    Spark/Hadoop selects the decompression codec from the file extension. All
    columns are initially read as strings so preprocessing owns type conversion.
    ``maxColumns`` is raised for the MX10000-MX50000 wide schemas.
    """

    if max_columns < 3:
        raise ValueError("max_columns must be at least 3.")

    path = _normalize_spark_path(input_path)
    return (
        spark.read.option("header", True)
        .option("sep", "\t")
        .option("encoding", "UTF-8")
        .option("mode", "FAILFAST")
        .option("inferSchema", False)
        .option("maxColumns", max_columns)
        .csv(path)
    )


def preprocess_tab_dataframe(
    dataframe: "DataFrame",
    *,
    timestamp_format: str = DEFAULT_TIMESTAMP_FORMAT,
) -> "DataFrame":
    """Normalize metadata columns and cast every feature column to double.

    Expected input layout:

    ``timestamp, aircraft_type, <dynamic feature columns...>``

    The transformation is lazy and preserves row count. Invalid timestamp or
    numeric values become null and are left for the validation layer to report.
    """

    columns = dataframe.columns
    required_prefix = [TIMESTAMP_COLUMN, AIRCRAFT_TYPE_COLUMN]

    if columns[:2] != required_prefix:
        raise ValueError(
            "The first two columns must be "
            f"{required_prefix}; received {columns[:2]}."
        )
    if len(columns) < 3:
        raise ValueError("At least one feature column is required.")
    if len(columns) != len(set(columns)):
        raise ValueError("Duplicate column names are not supported.")

    timestamp_name = _quote_identifier(TIMESTAMP_COLUMN)
    aircraft_name = _quote_identifier(AIRCRAFT_TYPE_COLUMN)
    escaped_timestamp_format = timestamp_format.replace("'", "''")

    expressions = [
        (
            f"to_timestamp(trim({timestamp_name}), "
            f"'{escaped_timestamp_format}') AS {timestamp_name}"
        ),
        f"upper(trim({aircraft_name})) AS {aircraft_name}",
    ]
    expressions.extend(
        (
            f"cast(trim({_quote_identifier(column_name)}) AS double) "
            f"AS {_quote_identifier(column_name)}"
        )
        for column_name in columns[2:]
    )

    # One selectExpr call avoids tens of thousands of Python-to-JVM calls for
    # MX10000-MX50000 schemas while still producing a single lazy projection.
    return dataframe.selectExpr(*expressions)
