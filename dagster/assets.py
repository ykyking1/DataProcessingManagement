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

from dagster import Config, MaterializeResult, MetadataValue, asset

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
