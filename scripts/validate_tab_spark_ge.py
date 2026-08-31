"""Great Expectations validation functions for processed Spark DataFrames.

This module is independent of Dagster, MinIO, and DVC. It receives the Spark
DataFrame produced by ``preprocess_tab_dataframe`` and returns a JSON-compatible
Great Expectations validation result that an orchestrator can log or publish.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

import great_expectations as gx

try:
    from scripts.preprocess_tab_spark import (
        AIRCRAFT_TYPE_COLUMN,
        DEFAULT_MAX_COLUMNS,
        DEFAULT_SPARK_MASTER,
        DEFAULT_TIMESTAMP_FORMAT,
        TIMESTAMP_COLUMN,
        create_spark_session,
        preprocess_tab_dataframe,
        read_tab_dataframe,
        spark_readable_tab_input,
    )
except ModuleNotFoundError:
    # Direct execution places scripts, rather than the project root, first
    # on sys.path; import the sibling module in that mode.
    from preprocess_tab_spark import (
        AIRCRAFT_TYPE_COLUMN,
        DEFAULT_MAX_COLUMNS,
        DEFAULT_SPARK_MASTER,
        DEFAULT_TIMESTAMP_FORMAT,
        TIMESTAMP_COLUMN,
        create_spark_session,
        preprocess_tab_dataframe,
        read_tab_dataframe,
        spark_readable_tab_input,
    )

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT_DIR = PROJECT_ROOT / "reports" / "validation"


def _default_report_path() -> Path:
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return DEFAULT_REPORT_DIR / f"ge_validation_{run_timestamp}.json"


def write_validation_report(
    validation: dict[str, Any],
    output_path: Path | str | None = None,
) -> Path:
    """Write a JSON-compatible GE result atomically and return its path."""

    report_path = (
        _default_report_path()
        if output_path is None
        else Path(output_path).resolve()
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = report_path.with_suffix(report_path.suffix + ".tmp")
    temporary_path.write_text(
        json.dumps(validation, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary_path.replace(report_path)
    return report_path


def _default_feature_sample(feature_columns: Sequence[str]) -> list[str]:
    """Return first, middle, and last features without duplicates."""

    if not feature_columns:
        raise ValueError("At least one feature column is required for validation.")

    indexes = (0, len(feature_columns) // 2, len(feature_columns) - 1)
    return list(dict.fromkeys(feature_columns[index] for index in indexes))


def _resolve_feature_columns(
    dataframe: "DataFrame",
    requested_columns: Sequence[str] | None,
) -> list[str]:
    available_features = dataframe.columns[2:]
    selected = (
        _default_feature_sample(available_features)
        if requested_columns is None
        else list(dict.fromkeys(requested_columns))
    )

    if not selected:
        raise ValueError("feature_columns cannot be empty.")

    missing = [column for column in selected if column not in available_features]
    if missing:
        raise ValueError(f"Unknown feature columns requested: {missing}")
    return selected


def validate_tab_dataframe(
    dataframe: "DataFrame",
    *,
    expected_aircraft_type: str | None = None,
    feature_columns: Sequence[str] | None = None,
    result_format: str = "BASIC",
    report_path: Path | str | None = None,
    write_report: bool = True,
) -> dict[str, Any]:
    """Validate a processed MX Spark DataFrame with GE's Spark backend.

    The initial integration suite deliberately stays small:

    * the table must contain at least one row,
    * ``timestamp`` and ``aircraft_type`` must not be null,
    * aircraft values must match ``expected_aircraft_type`` when supplied,
    * selected feature columns must not be null.

    By default, the first, middle, and last feature columns are selected. This
    avoids creating tens of thousands of Spark jobs for very wide MX schemas;
    callers can provide a different subset as domain rules become clear.
    """

    required_prefix = [TIMESTAMP_COLUMN, AIRCRAFT_TYPE_COLUMN]
    if dataframe.columns[:2] != required_prefix:
        raise ValueError(
            "The first two columns must be "
            f"{required_prefix}; received {dataframe.columns[:2]}."
        )

    selected_features = _resolve_feature_columns(dataframe, feature_columns)
    expectations: list[Any] = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=TIMESTAMP_COLUMN),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=AIRCRAFT_TYPE_COLUMN),
    ]

    if expected_aircraft_type is not None:
        normalized_aircraft_type = expected_aircraft_type.strip().upper()
        if not normalized_aircraft_type:
            raise ValueError("expected_aircraft_type cannot be blank.")
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column=AIRCRAFT_TYPE_COLUMN,
                value_set=[normalized_aircraft_type],
            )
        )

    expectations.extend(
        gx.expectations.ExpectColumnValuesToNotBeNull(column=column_name)
        for column_name in selected_features
    )

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_spark(
        name="mx_processed_spark_source",
        force_reuse_spark_context=True,
        persist=False,
    )
    data_asset = data_source.add_dataframe_asset(name="mx_processed_dataframe")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name="mx_processed_batch"
    )
    suite = gx.ExpectationSuite(
        name="mx_processed_suite",
        expectations=expectations,
    )
    context.suites.add(suite)
    validation_definition = gx.ValidationDefinition(
        name="mx_processed_validation",
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
        "validated_feature_columns": selected_features,
        "result": ge_result,
    }
    if write_report:
        written_report = write_validation_report(output, report_path)
        output["report_path"] = str(written_report)
    else:
        output["report_path"] = None
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate processed tab-separated ZSTD parts with GE on Spark."
    )
    parser.add_argument("--input", required=True, help="Processed directory or URI")
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--expected-aircraft-type")
    parser.add_argument("--feature-column", action="append", dest="feature_columns")
    parser.add_argument("--result-format", default="BASIC")
    parser.add_argument("--max-columns", type=int, default=DEFAULT_MAX_COLUMNS)
    parser.add_argument(
        "--timestamp-format",
        default=DEFAULT_TIMESTAMP_FORMAT,
    )
    parser.add_argument("--spark-master", default=DEFAULT_SPARK_MASTER)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session(
        app_name="dvc-tab-validation",
        master=args.spark_master,
    )
    try:
        with spark_readable_tab_input(args.input) as readable_input:
            source = read_tab_dataframe(
                spark,
                readable_input,
                max_columns=args.max_columns,
            )
            processed = preprocess_tab_dataframe(
                source,
                timestamp_format=args.timestamp_format,
            )
            validation = validate_tab_dataframe(
                processed,
                expected_aircraft_type=args.expected_aircraft_type,
                feature_columns=args.feature_columns,
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
