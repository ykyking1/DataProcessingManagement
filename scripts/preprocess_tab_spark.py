"""Spark preprocessing functions for cleaned MX .tab/.tab.zst datasets.

The functions in this module do not depend on Dagster, MinIO, DVC, or Great
Expectations. A Dagster asset can import them and supply its own Spark session
and input path.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import zstandard as zstd
import pyspark

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TIMESTAMP_COLUMN = "timestamp"
AIRCRAFT_TYPE_COLUMN = "aircraft_type"
DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
DEFAULT_MAX_COLUMNS = 100_000
DEFAULT_SPARK_MASTER = "local[2]"
DEFAULT_ZSTD_LEVEL = 12
ZSTD_SUFFIXES = {".zst", ".zstd"}
WINDOWS_LOCAL_FS_SOURCE = (
    PROJECT_ROOT / "tools" / "spark" / "WindowsLocalFileSystem.java"
)
WINDOWS_LOCAL_FS_CLASS = "project.spark.WindowsLocalFileSystem"


def _quote_identifier(column_name: str) -> str:
    """Quote a Spark SQL identifier, including names containing backticks."""

    return f"`{column_name.replace('`', '``')}`"


def _normalize_spark_path(input_path: Path | str) -> str:
    """Resolve local paths while preserving Spark-compatible remote URIs."""

    path_text = str(input_path)
    if "://" in path_text:
        return path_text
    return str(Path(path_text).resolve())


def _decompress_zstd_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    decompressor = zstd.ZstdDecompressor()
    with source.open("rb") as input_file, destination.open("wb") as output_file:
        decompressor.copy_stream(input_file, output_file)


@contextmanager
def spark_readable_tab_input(input_path: Path | str):
    """Stage local ZSTD input as temporary .tab files when Hadoop lacks ZSTD."""

    path_text = str(input_path)
    if "://" in path_text:
        yield path_text
        return

    source = Path(path_text).resolve()
    if source.is_file():
        zstd_files = [source] if source.suffix.lower() in ZSTD_SUFFIXES else []
    elif source.is_dir():
        zstd_files = sorted(
            path
            for path in source.rglob("*")
            if path.is_file() and path.suffix.lower() in ZSTD_SUFFIXES
        )
    else:
        raise FileNotFoundError(f"Input path not found: {source}")

    if not zstd_files:
        yield str(source)
        return

    with tempfile.TemporaryDirectory(prefix="dvc_tab_zstd_") as temporary_dir:
        staging_directory = Path(temporary_dir)
        for index, zstd_file in enumerate(zstd_files):
            staged_file = staging_directory / f"part-{index:05d}.tab"
            _decompress_zstd_file(zstd_file, staged_file)
        yield str(staging_directory)


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
            f"try_to_timestamp(trim({timestamp_name}), "
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


def ensure_windows_local_fs_adapter() -> Path | None:
    """Build the local Windows Hadoop filesystem adapter when required."""

    if os.name != "nt":
        return None

    pyspark_root = Path(pyspark.__file__).resolve().parent
    hadoop_api_jars = sorted((pyspark_root / "jars").glob("hadoop-client-api-*.jar"))
    if len(hadoop_api_jars) != 1:
        raise RuntimeError(
            "Expected exactly one hadoop-client-api JAR in PySpark."
        )

    build_root = PROJECT_ROOT / "tmp" / "spark-windows-localfs"
    classes_directory = build_root / "classes"
    adapter_jar = build_root / "spark-windows-localfs.jar"
    classes_directory.mkdir(parents=True, exist_ok=True)

    requires_build = (
        not adapter_jar.exists()
        or adapter_jar.stat().st_mtime < WINDOWS_LOCAL_FS_SOURCE.stat().st_mtime
    )
    if requires_build:
        javac_executable = shutil.which("javac")
        jar_executable = shutil.which("jar")
        java_home = os.environ.get("JAVA_HOME")
        if java_home:
            java_bin = Path(java_home) / "bin"
            if javac_executable is None:
                javac_candidate = java_bin / "javac.exe"
                if javac_candidate.is_file():
                    javac_executable = str(javac_candidate)
            if jar_executable is None:
                jar_candidate = java_bin / "jar.exe"
                if jar_candidate.is_file():
                    jar_executable = str(jar_candidate)
        if javac_executable is None or jar_executable is None:
            raise RuntimeError("A full JDK with javac and jar is required on Windows.")
        subprocess.run(
            [
                javac_executable,
                "-cp",
                str(hadoop_api_jars[0]),
                "-d",
                str(classes_directory),
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
                str(classes_directory),
                ".",
            ],
            cwd=PROJECT_ROOT,
            check=True,
        )
    return adapter_jar


def create_spark_session(
    *,
    app_name: str,
    master: str = DEFAULT_SPARK_MASTER,
) -> "SparkSession":
    """Create the Spark session used by the standalone DVC repro CLI."""

    from pyspark.sql import SparkSession

    local_fs_adapter = ensure_windows_local_fs_adapter()
    builder = SparkSession.builder.appName(app_name).master(master)
    if master.startswith("local"):
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        builder = (
            builder.config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
        )
    if local_fs_adapter is not None:
        adapter_path = str(local_fs_adapter.resolve())
        builder = (
            builder.config("spark.driver.extraClassPath", adapter_path)
            .config("spark.executor.extraClassPath", adapter_path)
            .config("spark.hadoop.fs.file.impl", WINDOWS_LOCAL_FS_CLASS)
            .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        )
    return builder.getOrCreate()


def write_processed_tab_zstd(
    dataframe: "DataFrame",
    output_path: Path | str,
    *,
    compression_level: int = DEFAULT_ZSTD_LEVEL,
) -> int:
    """Write distributed tab-separated ZSTD parts to an output directory."""

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

    staging_directory = destination.with_name(destination.name + ".spark-tmp")
    compressed_directory = destination.with_name(destination.name + ".zstd-tmp")
    for temporary_path in (staging_directory, compressed_directory):
        if temporary_path.exists():
            shutil.rmtree(temporary_path)

    (
        dataframe.write.mode("overwrite")
        .option("header", True)
        .option("sep", "\t")
        .option("encoding", "UTF-8")
        .csv(str(staging_directory))
    )

    compressed_directory.mkdir(parents=True)
    compressor = zstd.ZstdCompressor(level=compression_level)
    part_files = sorted(staging_directory.glob("part-*"))
    try:
        if not part_files:
            raise RuntimeError("Spark did not produce any tab part files.")
        for index, part_file in enumerate(part_files):
            compressed_file = compressed_directory / f"part-{index:05d}.tab.zst"
            with part_file.open("rb") as input_file, compressed_file.open(
                "wb"
            ) as output_file:
                compressor.copy_stream(input_file, output_file)

        if destination.exists():
            shutil.rmtree(destination)
        compressed_directory.replace(destination)
    finally:
        shutil.rmtree(staging_directory, ignore_errors=True)
        if compressed_directory.exists():
            shutil.rmtree(compressed_directory, ignore_errors=True)

    return len(part_files)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preprocess a cleaned .tab/.tab.zst dataset with Spark."
    )
    parser.add_argument("--input", required=True, help="Input file or URI")
    parser.add_argument(
        "--output",
        required=True,
        help="Directory for distributed tab-separated ZSTD output parts",
    )
    parser.add_argument(
        "--max-columns",
        type=int,
        default=DEFAULT_MAX_COLUMNS,
    )
    parser.add_argument(
        "--timestamp-format",
        default=DEFAULT_TIMESTAMP_FORMAT,
    )
    parser.add_argument(
        "--spark-master",
        default=DEFAULT_SPARK_MASTER,
    )
    parser.add_argument(
        "--zstd-level",
        type=int,
        default=DEFAULT_ZSTD_LEVEL,
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session(
        app_name="dvc-tab-preprocessing",
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
            part_count = write_processed_tab_zstd(
                processed,
                args.output,
                compression_level=args.zstd_level,
            )
        print(
            f"Processed {len(processed.columns):,} columns into "
            f"{part_count} ZSTD-compressed tab part(s): {args.output}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
