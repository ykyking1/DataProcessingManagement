"""
UAV Telemetri Pipeline - Dagster Asset Tanımları

Ağır iş mantığı (veri okuma/temizleme, ClickHouse'a yazma, DVC ile
versiyonlama) artık scripts/ altındaki bağımsız (dagster'a bağımlı
olmayan) Python script'lerinde yaşıyor. Buradaki asset'ler yalnızca:

    1. Script'i subprocess ile doğru argümanlarla çalıştırır,
    2. Script'in ürettiği JSON metadata dosyasını okur,
    3. Bu metadata'yı Dagster'a (MaterializeResult) ve Postgres'e
       (record_asset_metadata) işler.

Bu sayede script'ler dagster kurulu olmayan bir ortamda da (örn. CI,
manuel çalıştırma) doğrudan çalıştırılıp test edilebilir.
"""

import json
import os
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

from dagster import Config, Failure, MaterializeResult, MetadataValue, asset

from partitions import daily_partitions
from metadata_store import record_asset_metadata


# ---------------------------------------------------------------------------
# Ortak yollar
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGSTER_DIR = PROJECT_ROOT / "dagster"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"

RAW_INTERIM_DIR = DAGSTER_DIR / "data" / "interim" / "raw"
PROCESSED_DATA_DIR = DAGSTER_DIR / "data" / "processed"
MX_TAB_PROCESSED_DIR = PROJECT_ROOT / "tmp" / "dagster_spark_ge" / "processed"
MX_TAB_VALIDATION_REPORT = (
    PROJECT_ROOT
    / "reports"
    / "validation"
    / "dagster_spark_ge_validation.json"
)
MX_TAB_DVC_RELEASE_DIR = PROJECT_ROOT / "data" / "processed" / "mx_tab"

PIPELINE_GIT_PATHS = [
    "dagster",
    "scripts",
    "dvc.yaml",
    "params.yaml",
    "requirements*.txt",
    "pyproject.toml",
    "uv.lock",
    "poetry.lock",
    ":(exclude)dagster/data",
]


@contextmanager
def _temp_metadata_path():
    """Script'in JSON metadata yazacağı geçici bir dosya yolu üretir."""

    fd, path_str = tempfile.mkstemp(suffix=".json", prefix="asset_metadata_")
    os.close(fd)
    path = Path(path_str)

    try:
        yield path
    finally:
        path.unlink(missing_ok=True)


def _run_script(context, command: list, cwd: Path = DAGSTER_DIR) -> None:
    """Script'i çalıştırır; stdout/stderr'i Dagster'ın event log'una yazar.

    subprocess.run(check=True) tek başına yalnızca "non-zero exit status"
    diyen genel bir CalledProcessError fırlatır -- script'in gerçek hata
    mesajı (traceback) yalnızca compute log sekmesinde görünür ve kolayca
    kaçırılabilir. Burada script'in çıktısını yakalayıp context.log'a
    yazıyoruz ki asıl hata doğrudan run'ın event log'unda görünsün.
    """

    result = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        context.log.info(result.stdout.rstrip())

    if result.returncode != 0:
        if result.stderr:
            context.log.error(result.stderr.rstrip())

        script_name = Path(command[1]).name
        raise RuntimeError(
            f"{script_name} başarısız oldu (exit code {result.returncode}).\n"
            f"{result.stderr.rstrip() if result.stderr else '(stderr boş)'}"
        )


def _resolve_project_path(path_value: str) -> Path:
    """Resolve relative asset config paths from the repository root."""

    path = Path(path_value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _quality_report_metadata(report_path: Path, report: dict) -> dict:
    """Build the Dagster metadata shown for a GE validation materialization."""

    statistics = report.get("statistics", {})
    return {
        "quality_status": "passed" if report.get("success") else "failed",
        "evaluated_expectations": statistics.get("evaluated_expectations", 0),
        "successful_expectations": statistics.get("successful_expectations", 0),
        "unsuccessful_expectations": statistics.get(
            "unsuccessful_expectations", 0
        ),
        "success_percent": statistics.get("success_percent", 0.0),
        "validated_feature_columns": MetadataValue.json(
            report.get("validated_feature_columns", [])
        ),
        "report_path": MetadataValue.path(str(report_path)),
        "quality_report": MetadataValue.json(report),
    }


# ===========================================================================
# spark_processed_tab -> spark_validated_tab (manual integration demo)
# ===========================================================================


class SparkTabProcessingConfig(Config):
    """Local inputs for the MX Spark preprocessing integration demo."""

    input_path: str = "data/processed/mx_small_cleaned/mx10000_10rows_clean.tab"
    output_path: str = str(MX_TAB_PROCESSED_DIR)
    max_columns: int = 100_000
    timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
    spark_master: str = "local[2]"
    zstd_level: int = 12


@asset(
    group_name="mx_tab_quality",
    compute_kind="spark",
    description=(
        "Processes a cleaned MX .tab/.tab.zst dataset with Spark and writes "
        "partitioned .tab.zst output for the validation asset."
    ),
)
def spark_processed_tab(context, config: SparkTabProcessingConfig):
    input_path = _resolve_project_path(config.input_path)
    output_path = _resolve_project_path(config.output_path)

    command = [
        sys.executable,
        str(SCRIPTS_DIR / "preprocess_tab_spark.py"),
        "--input",
        str(input_path),
        "--output",
        str(output_path),
        "--max-columns",
        str(config.max_columns),
        "--timestamp-format",
        config.timestamp_format,
        "--spark-master",
        config.spark_master,
        "--zstd-level",
        str(config.zstd_level),
    ]

    context.log.info("Spark preprocessing started: %s", input_path)
    _run_script(context, command, cwd=PROJECT_ROOT)

    part_files = sorted(output_path.glob("part-*.tab.zst"))
    output_size_bytes = sum(path.stat().st_size for path in part_files)
    context.log.info(
        "Spark preprocessing completed: %s part(s), %s bytes.",
        len(part_files),
        output_size_bytes,
    )

    return MaterializeResult(
        value=str(output_path),
        metadata={
            "input_path": MetadataValue.path(str(input_path)),
            "output_path": MetadataValue.path(str(output_path)),
            "part_count": len(part_files),
            "output_size_bytes": output_size_bytes,
            "spark_master": config.spark_master,
            "zstd_level": config.zstd_level,
        },
    )


class SparkTabValidationConfig(Config):
    """Great Expectations settings for processed MX tab data."""

    report_path: str = str(MX_TAB_VALIDATION_REPORT)
    expected_aircraft_type: str | None = "MX10000"
    result_format: str = "BASIC"
    max_columns: int = 100_000
    timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
    spark_master: str = "local[2]"


@asset(
    group_name="mx_tab_quality",
    compute_kind="great_expectations",
    description=(
        "Validates the Spark-processed MX dataset with Great Expectations "
        "and exposes the JSON quality report in Dagster metadata."
    ),
)
def spark_validated_tab(
    context,
    spark_processed_tab: str,
    config: SparkTabValidationConfig,
):
    processed_path = Path(spark_processed_tab).resolve()
    report_path = _resolve_project_path(config.report_path)
    report_path.unlink(missing_ok=True)

    command = [
        sys.executable,
        str(SCRIPTS_DIR / "validate_tab_spark_ge.py"),
        "--input",
        str(processed_path),
        "--report",
        str(report_path),
        "--result-format",
        config.result_format,
        "--max-columns",
        str(config.max_columns),
        "--timestamp-format",
        config.timestamp_format,
        "--spark-master",
        config.spark_master,
    ]
    if config.expected_aircraft_type is not None:
        command.extend(
            ["--expected-aircraft-type", config.expected_aircraft_type]
        )

    context.log.info("GE validation started: %s", processed_path)
    execution_error = None
    try:
        _run_script(context, command, cwd=PROJECT_ROOT)
    except RuntimeError as error:
        execution_error = error

    if not report_path.is_file():
        if execution_error is not None:
            raise execution_error
        raise RuntimeError(f"Validation report was not created: {report_path}")

    report = json.loads(report_path.read_text(encoding="utf-8"))
    metadata = _quality_report_metadata(report_path, report)
    statistics = report.get("statistics", {})
    context.log.info(
        "GE quality result: success=%s, passed=%s/%s.",
        report.get("success"),
        statistics.get("successful_expectations", 0),
        statistics.get("evaluated_expectations", 0),
    )

    if execution_error is not None or not report.get("success", False):
        raise Failure(
            description=f"GE validation failed. Report: {report_path}",
            metadata=metadata,
        ) from execution_error

    return MaterializeResult(
        value=str(report_path),
        metadata=metadata,
    )


# ===========================================================================
# raw_uav_telemetry
# ===========================================================================

class RawTelemetryConfig(Config):
    """
    Hangi kaynak dosyanın okunacağını belirtir.

    - Sensor tarafından tetiklenen run'larda telemetry_sensor.py bu alanı
      bulduğu dosyanın tam yoluyla doldurur (run_config üzerinden).
    - Manuel çalıştırma veya backfill'de belirtilmezse varsayılan örnek
      dosya kullanılır.
    """

    file_path: str = "data/au_air/telemetry.parquet"

    flight_id: str = ""
    """
    Bu dosyanın ait olduğu uçuşun kimliği (örn. "flight_1", "ucus_003").

    Boş bırakılırsa dosya adının uzantısız hali (path.stem) kullanılır.
    Sensor tarafından tetiklenen run'larda genelde boş bırakılır; dosya
    adı zaten uçuşu ayırt etmeye yeter (örn. telemetry_013.parquet ->
    flight_id = "telemetry_013").
    """


@asset(
    compute_kind="python",
    group_name="raw_layer",
    partitions_def=daily_partitions,
    description=(
        "AU-AIR telemetri verisini kaynaktan (sensor'ün bulduğu dosya "
        "veya varsayılan örnek dosya) okur; günlük partition'a göre "
        "filtreler."
    ),
)
def raw_uav_telemetry(context, config: RawTelemetryConfig):

    partition_date = context.partition_key

    with _temp_metadata_path() as metadata_path:

        command = [
            sys.executable,
            str(SCRIPTS_DIR / "ingest_telemetry.py"),
            "--file-path", config.file_path,
            "--partition-date", partition_date,
            "--flight-id", config.flight_id,
            "--output-dir", str(RAW_INTERIM_DIR),
            "--metadata-out", str(metadata_path),
        ]

        context.log.info(
            f"ingest_telemetry.py çalıştırılıyor (dosya={config.file_path}, "
            f"partition={partition_date})."
        )
        _run_script(context, command)

        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    flight_id = metadata["flight_id"]
    schema = metadata["schema"]

    context.log.info(
        f"AU-AIR verisi okundu (partition={partition_date}, "
        f"flight_id={flight_id}, dosya={metadata['source_file']}): "
        f"{metadata['row_count']} satır"
    )

    record_asset_metadata(
        context,
        group_name="raw_layer",
        flight_id=flight_id,
        row_count=metadata["row_count"],
        metadata={
            "partition": partition_date,
            "flight_id": flight_id,
            "source_file": metadata["source_file"],
            "row_count": metadata["row_count"],
            "column_count": metadata["column_count"],
            "schema": schema,
        },
    )

    return MaterializeResult(
        value=metadata["output_file"],
        metadata={
            "partition": partition_date,
            "flight_id": flight_id,
            "source_file": metadata["source_file"],
            "row_count": metadata["row_count"],
            "column_count": metadata["column_count"],
            "schema": MetadataValue.json(schema),
        },
    )


# ===========================================================================
# processed_telemetry
# ===========================================================================

@asset(
    group_name="processing",
    partitions_def=daily_partitions,
    description="Raw telemetri verisini işleyerek curated katmana hazırlar.",
)
def processed_telemetry(context, raw_uav_telemetry: str):

    partition_date = context.partition_key

    with _temp_metadata_path() as metadata_path:

        command = [
            sys.executable,
            str(SCRIPTS_DIR / "process_telemetry.py"),
            "--input-path", raw_uav_telemetry,
            "--partition-date", partition_date,
            "--output-dir", str(PROCESSED_DATA_DIR),
            "--metadata-out", str(metadata_path),
        ]

        context.log.info(
            f"process_telemetry.py çalıştırılıyor (partition={partition_date})."
        )
        _run_script(context, command)

        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    processed_files = metadata["processed_files"]
    flights = metadata["flights"]
    schema = metadata["schema"]

    if processed_files:
        context.log.info(
            f"İşlenmiş veri diske kaydedildi (partition={partition_date}): "
            f"{processed_files}"
        )

    # Her uçuş için ayrı bir metadata geçmişi satırı -- dashboard'daki
    # Katalog sekmesinde uçuş bazlı filtreleme bunun üzerinden yapılır.
    for record in metadata["flight_records"]:
        record_asset_metadata(
            context,
            group_name="processing",
            flight_id=record["flight_id"],
            row_count=record["row_count"],
            metadata={
                "partition": partition_date,
                "flight_id": record["flight_id"],
                "row_count": record["row_count"],
                "column_count": record["column_count"],
                "columns": record["columns"],
                "schema": schema,
                "processed_file": record["processed_file"],
            },
        )

    return MaterializeResult(
        value=processed_files,
        metadata={
            "partition": partition_date,
            "row_count": metadata["row_count"],
            "column_count": metadata["column_count"],
            "columns": ", ".join(metadata["columns"]),
            "flights": ", ".join(flights) if flights else "-",
            "schema": MetadataValue.json(schema),
            "processed_files": (
                MetadataValue.json(processed_files)
                if processed_files
                else "-"
            ),
        },
    )


# ===========================================================================
# clickhouse_telemetry
# ===========================================================================

@asset(
    group_name="storage",
    compute_kind="clickhouse",
    partitions_def=daily_partitions,
    description="Processed AU-AIR telemetri verisini ClickHouse'a yazar.",
)
def clickhouse_telemetry(context, processed_telemetry: list):

    partition_date = context.partition_key

    if not processed_telemetry:
        context.log.info(
            f"İşlenmiş dosya bulunmadığı için ClickHouse'a yazılacak veri "
            f"yok (partition={partition_date})."
        )
        return MaterializeResult(
            metadata={
                "partition": partition_date,
                "row_count": 0,
            }
        )

    with _temp_metadata_path() as metadata_path:

        command = [
            sys.executable,
            str(SCRIPTS_DIR / "load_clickhouse.py"),
            "--partition-date", partition_date,
            "--metadata-out", str(metadata_path),
        ]
        for file_path in processed_telemetry:
            command += ["--input-file", file_path]

        context.log.info(
            f"load_clickhouse.py çalıştırılıyor ({len(processed_telemetry)} "
            f"dosya, partition={partition_date})."
        )
        _run_script(context, command)

        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    flight_ids_in_batch = metadata["flights"]
    schema = metadata["schema"]

    context.log.info(
        f"ClickHouse'a {metadata['row_count']} satır yazıldı "
        f"(partition={partition_date})."
    )

    if flight_ids_in_batch:
        for record in metadata["flight_records"]:
            record_asset_metadata(
                context,
                group_name="storage",
                flight_id=record["flight_id"],
                row_count=record["row_count"],
                metadata={
                    "partition": partition_date,
                    "flight_id": record["flight_id"],
                    "table": metadata["table"],
                    "row_count": record["row_count"],
                    "column_count": metadata["column_count"],
                    "database": metadata["database"],
                    "schema": schema,
                },
            )
    else:
        record_asset_metadata(
            context,
            group_name="storage",
            flight_id=None,
            row_count=metadata["row_count"],
            metadata={
                "partition": partition_date,
                "flight_id": None,
                "table": metadata["table"],
                "row_count": metadata["row_count"],
                "column_count": metadata["column_count"],
                "database": metadata["database"],
                "schema": schema,
            },
        )

    return MaterializeResult(
        metadata={
            "partition": partition_date,
            "flights": (
                ", ".join(flight_ids_in_batch) if flight_ids_in_batch else "-"
            ),
            "table": metadata["table"],
            "row_count": metadata["row_count"],
            "column_count": metadata["column_count"],
            "database": metadata["database"],
            "schema": MetadataValue.json(schema),
        }
    )


# ===========================================================================
# extended_telemetry_load
# ===========================================================================
#
# AU-AIR'in sabit 17 sütunluk şemasına (raw_uav_telemetry -> processed_
# telemetry -> clickhouse_telemetry zinciri) uymayan, çok daha geniş
# şemalı (binlerce sütunlu) kaynak dosyalar için ayrı bir asset. Bu
# dosyalar genelde birden fazla uçuşu/tarihi tek dosyada birleştirdiği
# için günlük partition modeline uymuyor -- bu yüzden partitions_def
# YOK; Dagster UI'dan "Materialize" ile elle, işlenecek dosyanın yolu
# config olarak verilerek tetiklenir. scripts/load_extended_telemetry.py
# şemayı dosyanın kendisinden çıkarıp ayrı bir ClickHouse tablosuna
# (varsayılan: telemetry_extended) yazar -- mevcut `telemetry` tablosuna
# dokunmaz.

class ExtendedTelemetryConfig(Config):

    file_path: str
    """İşlenecek geniş şemalı dosyanın tam yolu (.tab/.tab.gz/.csv/.csv.gz)."""

    table_name: str = "telemetry_extended"
    """Verinin yazılacağı ClickHouse tablosu."""

    chunk_rows: int = 50_000
    """Her INSERT'te ClickHouse'a gönderilecek satır sayısı."""


@asset(
    group_name="extended",
    compute_kind="clickhouse",
    description=(
        "AU-AIR'in sabit şemasına uymayan, geniş şemalı (binlerce "
        "sütunlu) bir telemetri dosyasını -- şemasını dosyadan çıkararak "
        "-- ayrı bir ClickHouse tablosuna yükler."
    ),
)
def extended_telemetry_load(context, config: ExtendedTelemetryConfig):

    with _temp_metadata_path() as metadata_path:

        command = [
            sys.executable,
            str(SCRIPTS_DIR / "load_extended_telemetry.py"),
            "--file-path", config.file_path,
            "--table-name", config.table_name,
            "--chunk-rows", str(config.chunk_rows),
            "--metadata-out", str(metadata_path),
        ]

        context.log.info(
            f"load_extended_telemetry.py çalıştırılıyor (dosya="
            f"{config.file_path}, tablo={config.table_name})."
        )
        _run_script(context, command)

        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    context.log.info(
        f"ClickHouse'a {metadata['row_count']} satır yazıldı "
        f"(tablo={metadata['table']}, {metadata['elapsed_seconds']}sn)."
    )

    return MaterializeResult(
        metadata={
            "source_file": metadata["source_file"],
            "table": metadata["table"],
            "row_count": metadata["row_count"],
            "column_count": metadata["column_count"],
            "chunk_count": metadata["chunk_count"],
            "elapsed_seconds": metadata["elapsed_seconds"],
            "time_column_source": metadata["time_column_source"],
            "schema": MetadataValue.json(metadata["schema"]),
        }
    )


# ===========================================================================
# dvc_published_telemetry
# ===========================================================================

def _get_pipeline_git_sha() -> str:
    configured_sha = os.getenv("PIPELINE_GIT_SHA")
    if configured_sha:
        return configured_sha

    result = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _get_pipeline_version(pipeline_git_sha: str) -> str:
    configured_version = os.getenv("PIPELINE_VERSION")
    if configured_version:
        return configured_version

    result = subprocess.run(
        [
            "git",
            "describe",
            "--tags",
            "--match",
            "pipeline-v*",
            "--abbrev=0",
            "HEAD",
        ],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        return f"unreleased-{pipeline_git_sha}"

    pipeline_tag = result.stdout.strip()
    committed_changes = subprocess.run(
        [
            "git",
            "diff",
            "--quiet",
            f"{pipeline_tag}..HEAD",
            "--",
            *PIPELINE_GIT_PATHS,
        ],
        cwd=PROJECT_ROOT,
        check=False,
    )
    working_tree_changes = subprocess.run(
        [
            "git",
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            *PIPELINE_GIT_PATHS,
        ],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    if committed_changes.returncode not in (0, 1):
        raise RuntimeError(
            f"Could not compare pipeline code with {pipeline_tag}."
        )

    if committed_changes.returncode == 1 or working_tree_changes.stdout.strip():
        return f"unreleased-{pipeline_git_sha}"

    return pipeline_tag


@asset(
    group_name="publishing",
    partitions_def=daily_partitions,
    description=(
        "Track the successful Dagster processed output with DVC and prepare "
        "its version commit metadata."
    ),
)
def dvc_published_telemetry(context, processed_telemetry: list):
    """Version the processed Dagster output without running dvc repro."""

    pipeline_git_sha = _get_pipeline_git_sha()
    pipeline_version = _get_pipeline_version(pipeline_git_sha)
    batch_id = context.partition_key

    command = [
        sys.executable,
        str(SCRIPTS_DIR / "publish_validated_data.py"),
        "--data-path",
        str(PROCESSED_DATA_DIR),
        "--pipeline-version",
        pipeline_version,
        "--pipeline-git-sha",
        pipeline_git_sha,
        "--raw-batches",
        batch_id,
    ]

    context.log.info(
        "Starting DVC data versioning for Dagster partition %s.",
        batch_id,
    )
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)

    record_asset_metadata(
        context,
        group_name="publishing",
        flight_id=None,
        row_count=None,
        metadata={
            "partition": batch_id,
            "data_path": str(PROCESSED_DATA_DIR),
            "pipeline_version": pipeline_version,
            "pipeline_git_sha": pipeline_git_sha,
            "raw_batches": batch_id,
            "git_commit_created": False,
        },
    )

    return MaterializeResult(
        metadata={
            "data_path": MetadataValue.path(str(PROCESSED_DATA_DIR)),
            "pipeline_version": pipeline_version,
            "pipeline_git_sha": pipeline_git_sha,
            "raw_batches": batch_id,
            "git_commit_created": False,
        }
    )


@asset(
    group_name="publishing",
    compute_kind="dvc",
    description=(
        "Stages successfully validated MX output at a stable data path, "
        "updates its DVC pointer, and logs the proposed Git commit metadata."
    ),
)
def dvc_published_mx_tab(
    context,
    spark_processed_tab: str,
    spark_validated_tab: str,
):
    """Update the MX DVC pointer without Git commit, Git push, or dvc push."""

    source_path = Path(spark_processed_tab).resolve()
    validation_report_path = Path(spark_validated_tab).resolve()
    release_path = MX_TAB_DVC_RELEASE_DIR.resolve()
    pointer_path = release_path.with_name(f"{release_path.name}.dvc")
    pipeline_git_sha = _get_pipeline_git_sha()
    pipeline_version = _get_pipeline_version(pipeline_git_sha)
    raw_batch = source_path.parent.name

    command = [
        sys.executable,
        str(SCRIPTS_DIR / "publish_validated_data.py"),
        "--data-path",
        str(source_path),
        "--release-path",
        str(release_path),
        "--pipeline-version",
        pipeline_version,
        "--pipeline-git-sha",
        pipeline_git_sha,
        "--raw-batches",
        raw_batch,
    ]

    context.log.info(
        "DVC release started for validated MX batch %s.", raw_batch
    )
    _run_script(context, command, cwd=PROJECT_ROOT)

    if not pointer_path.is_file():
        raise RuntimeError(f"DVC pointer was not created: {pointer_path}")

    relative_release_path = release_path.relative_to(PROJECT_ROOT)
    relative_pointer_path = pointer_path.relative_to(PROJECT_ROOT)
    context.log.info(
        "DVC pointer updated. Review it and create the Git commit manually: %s",
        relative_pointer_path.as_posix(),
    )

    return MaterializeResult(
        value=str(pointer_path),
        metadata={
            "source_path": MetadataValue.path(str(source_path)),
            "release_path": MetadataValue.path(str(release_path)),
            "dvc_pointer": MetadataValue.path(str(pointer_path)),
            "validation_report": MetadataValue.path(
                str(validation_report_path)
            ),
            "pipeline_version": pipeline_version,
            "pipeline_git_sha": pipeline_git_sha,
            "raw_batch": raw_batch,
            "dvc_target": relative_release_path.as_posix(),
            "dvc_pointer_git_path": relative_pointer_path.as_posix(),
            "dvc_pushed": False,
            "git_commit_created": False,
            "git_push_performed": False,
        },
    )
