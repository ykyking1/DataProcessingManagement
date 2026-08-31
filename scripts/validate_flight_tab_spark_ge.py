"""Great Expectations validation for processed flight telemetry on Spark."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import great_expectations as gx

try:
    from scripts.preprocess_flight_tab_spark import (
        FLIGHT_COLUMNS,
        REQUIRED_NUMERIC_COLUMNS,
        create_spark_session,
        preprocess_flight_dataframe,
        read_tab_dataframe,
        spark_readable_tab_input,
    )
    from scripts.preprocess_tab_spark import (
        DEFAULT_MAX_COLUMNS,
        DEFAULT_SPARK_MASTER,
        DEFAULT_TIMESTAMP_FORMAT,
    )
    from scripts.validate_tab_spark_ge import write_validation_report
except ModuleNotFoundError:
    from preprocess_flight_tab_spark import (
        FLIGHT_COLUMNS,
        REQUIRED_NUMERIC_COLUMNS,
        create_spark_session,
        preprocess_flight_dataframe,
        read_tab_dataframe,
        spark_readable_tab_input,
    )
    from preprocess_tab_spark import (
        DEFAULT_MAX_COLUMNS,
        DEFAULT_SPARK_MASTER,
        DEFAULT_TIMESTAMP_FORMAT,
    )
    from validate_tab_spark_ge import write_validation_report


OBJECT_COUNT_COLUMNS = [
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


def validate_flight_dataframe(
    dataframe,
    *,
    expected_row_count: int,
    result_format: str = "BASIC",
    report_path: Path | str | None = None,
) -> dict[str, Any]:
    if dataframe.columns != FLIGHT_COLUMNS:
        raise ValueError(
            "Processed flight columns do not match the dashboard contract: "
            f"{dataframe.columns}"
        )
    if expected_row_count <= 0:
        raise ValueError("expected_row_count must be greater than zero.")

    expectations: list[Any] = [
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=FLIGHT_COLUMNS
        ),
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=expected_row_count,
            max_value=expected_row_count,
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="time"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="flight_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="image_name"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="platform"),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="latitude",
            min_value=-90,
            max_value=90,
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="longitude",
            min_value=-180,
            max_value=180,
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="altitude",
            min_value=0,
            max_value=30_000,
        ),
    ]
    expectations.extend(
        gx.expectations.ExpectColumnValuesToNotBeNull(column=column_name)
        for column_name in REQUIRED_NUMERIC_COLUMNS
    )
    # Nesne sayıları negatif olamaz.
    expectations.extend(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column=column_name,
            min_value=0,
        )
        for column_name in OBJECT_COUNT_COLUMNS
    )

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_spark(
        name="flight_processed_spark_source",
        force_reuse_spark_context=True,
        persist=False,
    )
    data_asset = data_source.add_dataframe_asset(
        name="flight_processed_dataframe"
    )
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name="flight_processed_batch"
    )
    suite = gx.ExpectationSuite(
        name="flight_processed_suite",
        expectations=expectations,
    )
    context.suites.add(suite)
    validation_definition = gx.ValidationDefinition(
        name="flight_processed_validation",
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
        "validated_columns": FLIGHT_COLUMNS,
        "result": ge_result,
    }
    written_report = write_validation_report(output, report_path)
    output["report_path"] = str(written_report)
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate processed flight TAB ZSTD parts with GE on Spark."
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--expected-row-count", required=True, type=int)
    parser.add_argument("--result-format", default="BASIC")
    parser.add_argument("--max-columns", type=int, default=DEFAULT_MAX_COLUMNS)
    parser.add_argument("--timestamp-format", default=DEFAULT_TIMESTAMP_FORMAT)
    parser.add_argument("--spark-master", default=DEFAULT_SPARK_MASTER)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session(
        app_name="flight-telemetry-validation",
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
            validation = validate_flight_dataframe(
                processed,
                expected_row_count=args.expected_row_count,
                result_format=args.result_format,
                report_path=args.report,
            )
        print(
            f"Validation {'passed' if validation['success'] else 'failed'}: "
            f"{validation['statistics']}"
        )
        if not validation["success"]:
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
