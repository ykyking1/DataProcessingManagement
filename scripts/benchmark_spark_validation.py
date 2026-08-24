"""Compare native Spark and Great Expectations validation on one Spark DataFrame."""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import great_expectations as gx
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from benchmark_spark_processing import (
    DEFAULT_INPUT,
    FEATURE_COLUMNS,
    PROJECT_ROOT,
    build_spark,
    ensure_windows_local_fs_adapter,
    parquet_metrics,
    spark_runtime_environment,
)


DEFAULT_PROCESSED_INPUT = DEFAULT_INPUT.with_name("random_numbers_v1_processed.parquet")
DEFAULT_REPORT_DIR = PROJECT_ROOT / "reports" / "benchmarks"
DERIVED_COLUMNS = ["feature_mean", "feature_spread", "risk_score", "risk_band"]
EXPECTED_COLUMNS = FEATURE_COLUMNS + DERIVED_COLUMNS
NUMERIC_RULE_COLUMNS = [
    "feature_01",
    "feature_08",
    "feature_16",
    "feature_mean",
    "feature_spread",
    "risk_score",
]
RISK_BANDS = ["low", "medium", "high"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare native Spark and GE-on-Spark validation."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_PROCESSED_INPUT)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--master", default="local[8]")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument(
        "--order",
        choices=("native-first", "ge-first"),
        default="native-first",
        help="Validation execution order on the same processed DataFrame.",
    )
    return parser.parse_args()


def spark_job_metrics(spark: SparkSession, group_id: str) -> dict[str, Any]:
    tracker = spark.sparkContext.statusTracker()
    job_ids = sorted(tracker.getJobIdsForGroup(group_id))
    stage_ids: set[int] = set()
    for job_id in job_ids:
        job_info = tracker.getJobInfo(job_id)
        if job_info is not None:
            stage_ids.update(job_info.stageIds)
    return {
        "job_count": len(job_ids),
        "job_ids": job_ids,
        "stage_count": len(stage_ids),
        "stage_ids": sorted(stage_ids),
    }


def native_validate(dataframe: DataFrame, expected_rows: int) -> dict[str, Any]:
    aggregations = [F.count(F.lit(1)).alias("row_count")]
    for column in NUMERIC_RULE_COLUMNS:
        aggregations.extend(
            [
                F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(
                    f"{column}__null_count"
                ),
                F.min(F.col(column)).alias(f"{column}__min"),
                F.max(F.col(column)).alias(f"{column}__max"),
            ]
        )
    aggregations.extend(
        [
            F.sum(F.when(F.col("risk_band").isNull(), 1).otherwise(0)).alias(
                "risk_band__null_count"
            ),
            F.sum(
                F.when(
                    F.col("risk_band").isNotNull()
                    & ~F.col("risk_band").isin(RISK_BANDS),
                    1,
                ).otherwise(0)
            ).alias("risk_band__unexpected_count"),
        ]
    )
    observed = dataframe.agg(*aggregations).first().asDict()

    rules: list[dict[str, Any]] = [
        {
            "expectation": "table_columns_to_match_ordered_list",
            "success": dataframe.columns == EXPECTED_COLUMNS,
            "observed": dataframe.columns,
            "expected": EXPECTED_COLUMNS,
        },
        {
            "expectation": "table_row_count_to_equal",
            "success": observed["row_count"] == expected_rows,
            "observed": observed["row_count"],
            "expected": expected_rows,
        },
    ]
    for column in NUMERIC_RULE_COLUMNS:
        null_count = observed[f"{column}__null_count"]
        minimum = observed[f"{column}__min"]
        maximum = observed[f"{column}__max"]
        rules.extend(
            [
                {
                    "expectation": "column_values_to_not_be_null",
                    "column": column,
                    "success": null_count == 0,
                    "observed": {"null_count": null_count},
                },
                {
                    "expectation": "column_values_to_be_between",
                    "column": column,
                    "success": (
                        minimum is not None
                        and maximum is not None
                        and minimum >= 0.0
                        and maximum <= 1.0
                    ),
                    "observed": {"min": minimum, "max": maximum},
                    "expected": {"min": 0.0, "max": 1.0},
                },
            ]
        )
    rules.extend(
        [
            {
                "expectation": "column_values_to_not_be_null",
                "column": "risk_band",
                "success": observed["risk_band__null_count"] == 0,
                "observed": {
                    "null_count": observed["risk_band__null_count"]
                },
            },
            {
                "expectation": "column_values_to_be_in_set",
                "column": "risk_band",
                "success": observed["risk_band__unexpected_count"] == 0,
                "observed": {
                    "unexpected_count": observed["risk_band__unexpected_count"]
                },
                "expected": RISK_BANDS,
            },
        ]
    )
    return {
        "success": all(rule["success"] for rule in rules),
        "expectation_count": len(rules),
        "rules": rules,
    }


def ge_validate(dataframe: DataFrame, expected_rows: int) -> dict[str, Any]:
    expectations: list[Any] = [
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=EXPECTED_COLUMNS
        ),
        gx.expectations.ExpectTableRowCountToEqual(value=expected_rows),
    ]
    for column in NUMERIC_RULE_COLUMNS:
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToNotBeNull(column=column),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column=column,
                    min_value=0.0,
                    max_value=1.0,
                ),
            ]
        )
    expectations.extend(
        [
            gx.expectations.ExpectColumnValuesToNotBeNull(column="risk_band"),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="risk_band",
                value_set=RISK_BANDS,
            ),
        ]
    )

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_spark(
        name="spark_validation_benchmark_source",
        force_reuse_spark_context=True,
        persist=False,
    )
    data_asset = data_source.add_dataframe_asset(
        name="spark_validation_benchmark_asset"
    )
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name="processed_v1_batch"
    )
    suite = gx.ExpectationSuite(
        name="processed_v1_suite",
        expectations=expectations,
    )
    context.suites.add(suite)
    validation_definition = gx.ValidationDefinition(
        name="processed_v1_validation",
        data=batch_definition,
        suite=suite,
    )
    validation_result = validation_definition.run(
        batch_parameters={"dataframe": dataframe},
        result_format="BASIC",
    )
    return validation_result.to_json_dict()


def run_validator(
    spark: SparkSession,
    name: str,
    dataframe: DataFrame,
    expected_rows: int,
) -> dict[str, Any]:
    group_id = f"{name}-validation"
    spark.sparkContext.setJobGroup(group_id, f"{name} validation benchmark")
    started = time.perf_counter()
    if name == "native_spark":
        details = native_validate(dataframe, expected_rows)
    elif name == "ge_on_spark":
        details = ge_validate(dataframe, expected_rows)
    else:
        raise ValueError(f"Unknown validator: {name}")
    duration = time.perf_counter() - started
    spark.sparkContext.setJobGroup("", "")

    success = bool(details["success"])
    expectation_count = (
        int(details["expectation_count"])
        if name == "native_spark"
        else int(details["statistics"]["evaluated_expectations"])
    )
    return {
        "name": name,
        "success": success,
        "expectation_count": expectation_count,
        "duration_seconds": round(duration, 6),
        **spark_job_metrics(spark, group_id),
        "details": details,
    }


def write_report(result: dict[str, Any], report_dir: Path) -> tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    run_id = result["run_id"]
    json_path = report_dir / f"spark_validation_{run_id}.json"
    csv_path = report_dir / "spark_validation_results.csv"
    json_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    write_header = not csv_path.exists()
    fieldnames = [
        "run_id",
        "validator",
        "success",
        "expectation_count",
        "duration_seconds",
        "job_count",
        "stage_count",
        "input_gib",
        "input_rows",
        "master",
        "driver_memory",
    ]
    with csv_path.open("a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        for validation in result["validations"]:
            writer.writerow(
                {
                    "run_id": run_id,
                    "validator": validation["name"],
                    "success": validation["success"],
                    "expectation_count": validation["expectation_count"],
                    "duration_seconds": validation["duration_seconds"],
                    "job_count": validation["job_count"],
                    "stage_count": validation["stage_count"],
                    "input_gib": result["input"]["gib"],
                    "input_rows": result["input"]["rows"],
                    "master": result["master"],
                    "driver_memory": result["driver_memory"],
                }
            )
    return json_path, csv_path


def main() -> None:
    args = parse_args()
    input_path = args.input.resolve()
    report_dir = args.report_dir.resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Processed Parquet does not exist: {input_path}")

    input_bytes, input_rows, input_row_groups = parquet_metrics(input_path)
    adapter = ensure_windows_local_fs_adapter()
    benchmark_started = time.perf_counter()
    spark: SparkSession | None = None

    try:
        with spark_runtime_environment() as runtime_root:
            spark_started = time.perf_counter()
            spark = build_spark(
                args,
                runtime_root,
                adapter,
                app_name="spark-validation-benchmark",
            )
            spark_start_seconds = time.perf_counter() - spark_started

            read_plan_started = time.perf_counter()
            dataframe = spark.read.parquet(str(input_path))
            read_plan_seconds = time.perf_counter() - read_plan_started

            order = (
                ["native_spark", "ge_on_spark"]
                if args.order == "native-first"
                else ["ge_on_spark", "native_spark"]
            )
            validations = [
                run_validator(spark, name, dataframe, input_rows) for name in order
            ]

            result = {
                "run_id": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
                "benchmark": "native_spark_vs_ge_on_spark_validation",
                "great_expectations_version": gx.__version__,
                "spark_version": spark.version,
                "python_version": platform.python_version(),
                "platform": platform.platform(),
                "logical_cores": os.cpu_count(),
                "master": args.master,
                "driver_memory": args.driver_memory,
                "execution_order": order,
                "cache_storage_level": None,
                "input": {
                    "path": str(input_path),
                    "bytes": input_bytes,
                    "gib": round(input_bytes / 1024**3, 6),
                    "rows": input_rows,
                    "row_groups": input_row_groups,
                    "columns": EXPECTED_COLUMNS,
                },
                "timings_seconds": {
                    "spark_start": round(spark_start_seconds, 6),
                    "shared_read_plan": round(read_plan_seconds, 6),
                    "total": round(time.perf_counter() - benchmark_started, 6),
                },
                "validations": validations,
            }
            json_path, csv_path = write_report(result, report_dir)
            spark.stop()
            spark = None
    finally:
        if spark is not None:
            spark.stop()

    summary = {
        "run_id": result["run_id"],
        "input_gib": result["input"]["gib"],
        "input_rows": result["input"]["rows"],
        "shared_read_plan_seconds": result["timings_seconds"]["shared_read_plan"],
        "validations": [
            {
                "name": validation["name"],
                "success": validation["success"],
                "duration_seconds": validation["duration_seconds"],
                "job_count": validation["job_count"],
                "stage_count": validation["stage_count"],
            }
            for validation in result["validations"]
        ],
    }
    print(json.dumps(summary, indent=2))
    print(f"JSON report: {json_path}")
    print(f"CSV summary: {csv_path}")


if __name__ == "__main__":
    main()
