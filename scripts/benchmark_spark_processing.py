"""Benchmark a shared Spark processing step on the synthetic V1 Parquet file."""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import shutil
import subprocess
import tempfile
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pyarrow.parquet as pq
import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = (
    PROJECT_ROOT
    / "data"
    / "benchmark"
    / "source_versions"
    / "random_numbers_v1.parquet"
)
DEFAULT_OUTPUT = DEFAULT_INPUT.with_name("random_numbers_v1_processed.parquet")
DEFAULT_REPORT_DIR = PROJECT_ROOT / "reports" / "benchmarks"
FEATURE_COLUMNS = [f"feature_{index:02d}" for index in range(1, 17)]
WINDOWS_LOCAL_FS_SOURCE = (
    PROJECT_ROOT / "tools" / "spark" / "WindowsLocalFileSystem.java"
)
WINDOWS_LOCAL_FS_CLASS = "benchmark.spark.WindowsLocalFileSystem"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process the V1 benchmark Parquet file with local Spark."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--master", default="local[8]")
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument(
        "--limit-rows",
        type=int,
        help="Limit the input for a smoke test. Omit for the full benchmark.",
    )
    return parser.parse_args()


@contextmanager
def spark_runtime_environment() -> Iterator[Path]:
    """Provide an accessible local Spark temp directory on managed Windows PCs."""
    runtime_root = PROJECT_ROOT / "tmp" / "spark-runtime"
    runtime_root.mkdir(parents=True, exist_ok=True)

    environment_names = ("TEMP", "TMP", "SPARK_LOCAL_DIRS", "SPARK_LOCAL_IP")
    previous_environment = {name: os.environ.get(name) for name in environment_names}
    previous_tempdir = tempfile.tempdir
    original_mkdtemp = tempfile.mkdtemp

    os.environ["TEMP"] = str(runtime_root)
    os.environ["TMP"] = str(runtime_root)
    os.environ["SPARK_LOCAL_DIRS"] = str(runtime_root)
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    tempfile.tempdir = str(runtime_root)

    def accessible_mkdtemp(
        suffix: str | None = None,
        prefix: str | None = None,
        dir: str | os.PathLike[str] | None = None,
    ) -> str:
        parent = Path(dir) if dir is not None else runtime_root
        parent.mkdir(parents=True, exist_ok=True)
        directory = parent / f"{prefix or 'tmp'}{uuid.uuid4().hex}{suffix or ''}"
        os.mkdir(directory, 0o777)
        return str(directory)

    if os.name == "nt":
        tempfile.mkdtemp = accessible_mkdtemp

    try:
        yield runtime_root
    finally:
        tempfile.mkdtemp = original_mkdtemp
        tempfile.tempdir = previous_tempdir
        for name, value in previous_environment.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def ensure_windows_local_fs_adapter() -> Path | None:
    if os.name != "nt":
        return None

    pyspark_root = Path(pyspark.__file__).resolve().parent
    hadoop_api_jars = sorted((pyspark_root / "jars").glob("hadoop-client-api-*.jar"))
    if len(hadoop_api_jars) != 1:
        raise RuntimeError(
            "Expected exactly one hadoop-client-api JAR in the PySpark installation."
        )

    build_root = PROJECT_ROOT / "tmp" / "spark-windows-localfs"
    classes_dir = build_root / "classes"
    adapter_jar = build_root / "spark-windows-localfs.jar"
    classes_dir.mkdir(parents=True, exist_ok=True)

    requires_build = (
        not adapter_jar.exists()
        or adapter_jar.stat().st_mtime < WINDOWS_LOCAL_FS_SOURCE.stat().st_mtime
    )
    if requires_build:
        javac_executable = shutil.which("javac")
        jar_executable = shutil.which("jar")
        if jar_executable is None:
            program_files = Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
            jar_candidates = sorted((program_files / "Java").rglob("bin/jar.exe"))
            jar_executable = str(jar_candidates[-1]) if jar_candidates else None
        if javac_executable is None or jar_executable is None:
            raise RuntimeError("A full JDK with javac and jar is required on Windows.")

        subprocess.run(
            [
                javac_executable,
                "-cp",
                str(hadoop_api_jars[0]),
                "-d",
                str(classes_dir),
                str(WINDOWS_LOCAL_FS_SOURCE),
            ],
            cwd=PROJECT_ROOT,
            check=True,
        )
        subprocess.run(
            [
                jar_executable,
                "--create",
                "--file",
                str(adapter_jar),
                "-C",
                str(classes_dir),
                ".",
            ],
            cwd=PROJECT_ROOT,
            check=True,
        )
    return adapter_jar


def build_spark(
    args: argparse.Namespace,
    runtime_root: Path,
    local_fs_adapter: Path | None,
    app_name: str = "native-spark-processing-benchmark",
) -> SparkSession:
    logical_cores = os.cpu_count() or 2
    builder = (
        SparkSession.builder.master(args.master)
        .appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.local.dir", str(runtime_root))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", str(max(8, logical_cores * 2)))
        .config("spark.ui.enabled", "false")
    )
    if local_fs_adapter is not None:
        builder = (
            builder.config("spark.driver.extraClassPath", str(local_fs_adapter.resolve()))
            .config("spark.executor.extraClassPath", str(local_fs_adapter.resolve()))
            .config("spark.hadoop.fs.file.impl", WINDOWS_LOCAL_FS_CLASS)
            .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_data(source: DataFrame) -> DataFrame:
    missing_columns = sorted(set(FEATURE_COLUMNS) - set(source.columns))
    if missing_columns:
        raise ValueError(f"Input is missing expected columns: {missing_columns}")

    cleaned = source.dropna(subset=FEATURE_COLUMNS)
    feature_sum = sum((F.col(column) for column in FEATURE_COLUMNS), F.lit(0.0))
    feature_mean = feature_sum / F.lit(float(len(FEATURE_COLUMNS)))
    feature_spread = F.greatest(*FEATURE_COLUMNS) - F.least(*FEATURE_COLUMNS)
    risk_score = (
        F.col("feature_01") * F.lit(0.5)
        + F.col("feature_02") * F.lit(0.3)
        + F.col("feature_03") * F.lit(0.2)
    )

    return (
        cleaned.withColumn("feature_mean", feature_mean)
        .withColumn("feature_spread", feature_spread)
        .withColumn("risk_score", risk_score)
        .withColumn(
            "risk_band",
            F.when(F.col("risk_score") < 0.33, F.lit("low"))
            .when(F.col("risk_score") < 0.66, F.lit("medium"))
            .otherwise(F.lit("high")),
        )
    )


def parquet_metrics(path: Path) -> tuple[int, int, int]:
    files = [path] if path.is_file() else sorted(path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet files found under: {path}")

    size_bytes = 0
    row_count = 0
    row_groups = 0
    for file_path in files:
        metadata = pq.ParquetFile(file_path).metadata
        size_bytes += file_path.stat().st_size
        row_count += metadata.num_rows
        row_groups += metadata.num_row_groups
    return size_bytes, row_count, row_groups


def write_report(result: dict[str, object], report_dir: Path) -> tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    run_id = str(result["run_id"])
    json_path = report_dir / f"spark_processing_{run_id}.json"
    csv_path = report_dir / "spark_processing_results.csv"

    json_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    csv_row = {
        "run_id": result["run_id"],
        "spark_version": result["spark_version"],
        "master": result["master"],
        "limit_rows": result["limit_rows"],
        "input_rows": result["input"]["rows"],
        "output_rows": result["output"]["rows"],
        "input_gib": result["input"]["gib"],
        "output_gib": result["output"]["gib"],
        "spark_start_seconds": result["timings_seconds"]["spark_start"],
        "plan_seconds": result["timings_seconds"]["read_and_transform_plan"],
        "process_write_seconds": result["timings_seconds"]["process_and_write"],
        "total_seconds": result["timings_seconds"]["total"],
    }
    write_header = not csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(csv_row))
        if write_header:
            writer.writeheader()
        writer.writerow(csv_row)
    return json_path, csv_path


def main() -> None:
    args = parse_args()
    input_path = args.input.resolve()
    output_path = args.output.resolve()
    report_dir = args.report_dir.resolve()

    if not input_path.exists():
        raise FileNotFoundError(f"Input Parquet file does not exist: {input_path}")
    if args.limit_rows is not None and args.limit_rows <= 0:
        raise ValueError("--limit-rows must be greater than zero.")

    input_bytes, full_input_rows, input_row_groups = parquet_metrics(input_path)
    local_fs_adapter = ensure_windows_local_fs_adapter()
    benchmark_started = time.perf_counter()
    spark: SparkSession | None = None

    try:
        with spark_runtime_environment() as runtime_root:
            spark_started = time.perf_counter()
            spark = build_spark(args, runtime_root, local_fs_adapter)
            spark_start_seconds = time.perf_counter() - spark_started

            plan_started = time.perf_counter()
            source = spark.read.parquet(str(input_path))
            if args.limit_rows is not None:
                source = source.limit(args.limit_rows)
            processed = process_data(source)
            plan_seconds = time.perf_counter() - plan_started

            write_started = time.perf_counter()
            processed.write.mode("overwrite").parquet(str(output_path))
            process_write_seconds = time.perf_counter() - write_started

            output_bytes, output_rows, output_row_groups = parquet_metrics(output_path)
            result = {
                "run_id": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
                "benchmark": "native_spark_processing",
                "spark_version": spark.version,
                "python_version": platform.python_version(),
                "platform": platform.platform(),
                "logical_cores": os.cpu_count(),
                "master": args.master,
                "driver_memory": args.driver_memory,
                "limit_rows": args.limit_rows,
                "input": {
                    "path": str(input_path),
                    "bytes": input_bytes,
                    "gib": round(input_bytes / 1024**3, 6),
                    "rows": full_input_rows,
                    "row_groups": input_row_groups,
                    "columns": FEATURE_COLUMNS,
                },
                "output": {
                    "path": str(output_path),
                    "bytes": output_bytes,
                    "gib": round(output_bytes / 1024**3, 6),
                    "rows": output_rows,
                    "row_groups": output_row_groups,
                    "derived_columns": [
                        "feature_mean",
                        "feature_spread",
                        "risk_score",
                        "risk_band",
                    ],
                },
                "timings_seconds": {
                    "spark_start": round(spark_start_seconds, 6),
                    "read_and_transform_plan": round(plan_seconds, 6),
                    "process_and_write": round(process_write_seconds, 6),
                    "total": round(time.perf_counter() - benchmark_started, 6),
                },
            }
            json_path, csv_path = write_report(result, report_dir)
            spark.stop()
            spark = None
    finally:
        if spark is not None:
            spark.stop()

    print(json.dumps(result, indent=2))
    print(f"JSON report: {json_path}")
    print(f"CSV summary: {csv_path}")


if __name__ == "__main__":
    main()
