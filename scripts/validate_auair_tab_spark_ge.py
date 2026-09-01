"""Placeholder Great Expectations rules for processed AU-AIR telemetry."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import great_expectations as gx

try:
    from scripts.preprocess_auair_tab_spark import (
        DEFAULT_TIMESTAMP_FORMAT,
        FLIGHT_ID_COLUMN,
        MIN_COLUMN_COUNT,
        REQUIRED_PREFIX,
        TIME_COLUMN,
        create_spark_session,
        preprocess_auair_dataframe,
        read_tab_dataframe,
        spark_readable_tab_input,
    )
    from scripts.preprocess_tab_spark import (
        DEFAULT_MAX_COLUMNS,
        DEFAULT_SPARK_MASTER,
    )
    from scripts.validate_tab_spark_ge import write_validation_report
except ModuleNotFoundError:
    from preprocess_auair_tab_spark import (
        DEFAULT_TIMESTAMP_FORMAT,
        FLIGHT_ID_COLUMN,
        MIN_COLUMN_COUNT,
        REQUIRED_PREFIX,
        TIME_COLUMN,
        create_spark_session,
        preprocess_auair_dataframe,
        read_tab_dataframe,
        spark_readable_tab_input,
    )
    from preprocess_tab_spark import DEFAULT_MAX_COLUMNS, DEFAULT_SPARK_MASTER
    from validate_tab_spark_ge import write_validation_report


CORE_COLUMNS = [
    "image_name",
    "image_width",
    "image_height",
    "platform",
    "longtitude",
    "latitude",
    "altitude",
]


def validate_auair_dataframe(
    dataframe,
    *,
    expected_row_count: int,
    expected_column_count: int,
    result_format: str = "BASIC",
    report_path: Path | str | None = None,
) -> dict[str, Any]:
    """Run a deliberately small first-pass AU-AIR quality suite."""

    if dataframe.columns[:2] != REQUIRED_PREFIX:
        raise ValueError(
            f"Processed AU-AIR columns must start with {REQUIRED_PREFIX}."
        )
    if expected_row_count <= 0:
        raise ValueError("expected_row_count must be greater than zero.")
    if expected_column_count < MIN_COLUMN_COUNT:
        raise ValueError(
            f"expected_column_count must be at least {MIN_COLUMN_COUNT}."
        )
    if len(dataframe.columns) != expected_column_count:
        raise ValueError(
            f"Expected {expected_column_count} AU-AIR columns; "
            f"received {len(dataframe.columns)}."
        )

    missing_core = [
        column_name
        for column_name in CORE_COLUMNS
        if column_name not in dataframe.columns
    ]
    if missing_core:
        raise ValueError(f"Missing core AU-AIR columns: {missing_core}")

    expectations: list[Any] = [
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=dataframe.columns
        ),
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=expected_row_count,
            max_value=expected_row_count,
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=FLIGHT_ID_COLUMN),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=TIME_COLUMN),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="image_name"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="platform"),
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column=FLIGHT_ID_COLUMN,
            regex=r"^flight_[1-9][0-9]*_[0-9]{4}-[0-9]{2}-[0-9]{2}$",
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="latitude",
            min_value=-90,
            max_value=90,
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="longtitude",
            min_value=-180,
            max_value=180,
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="altitude",
            min_value=0,
            max_value=100_000,
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="image_width",
            min_value=1,
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="image_height",
            min_value=1,
        ),
    ]
    if "num_objects" in dataframe.columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="num_objects",
                min_value=0,
            )
        )

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_spark(
        name="auair_processed_spark_source",
        force_reuse_spark_context=True,
        persist=False,
    )
    data_asset = data_source.add_dataframe_asset(
        name="auair_processed_dataframe"
    )
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name="auair_processed_batch"
    )
    suite = gx.ExpectationSuite(
        name="auair_processed_placeholder_suite",
        expectations=expectations,
    )
    context.suites.add(suite)
    validation_definition = gx.ValidationDefinition(
        name="auair_processed_validation",
        data=batch_definition,
        suite=suite,
    )
    validation_result = validation_definition.run(
        batch_parameters={"dataframe": dataframe},
        result_format=result_format,
    )

    ge_result = validation_result.to_json_dict()
    output: dict[str, Any] = {
        "success": bool(ge_result["success"]),
        "statistics": ge_result["statistics"],
        "rules_profile": "auair-placeholder-v1",
        "validated_columns": dataframe.columns,
        "result": ge_result,
    }
    written_report = write_validation_report(output, report_path)
    output["report_path"] = str(written_report)
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate processed AU-AIR TAB ZSTD parts with GE on Spark."
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--expected-row-count", required=True, type=int)
    parser.add_argument("--expected-column-count", required=True, type=int)
    parser.add_argument("--result-format", default="BASIC")
    parser.add_argument("--max-columns", type=int, default=DEFAULT_MAX_COLUMNS)
    parser.add_argument("--timestamp-format", default=DEFAULT_TIMESTAMP_FORMAT)
    parser.add_argument("--spark-master", default=DEFAULT_SPARK_MASTER)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session(
        app_name="auair-telemetry-validation",
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
            )
            validation = validate_auair_dataframe(
                processed,
                expected_row_count=args.expected_row_count,
                expected_column_count=args.expected_column_count,
                result_format=args.result_format,
                report_path=args.report,
            )
        print(
            f"AU-AIR validation "
            f"{'passed' if validation['success'] else 'failed'}: "
            f"{validation['statistics']}"
        )
        if not validation["success"]:
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
