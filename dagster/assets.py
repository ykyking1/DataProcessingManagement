"""Dagster assets for generated high-column AU-AIR telemetry."""

import json
import mimetypes
import os
import signal
import subprocess
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse

from dagster import Config, Failure, MaterializeResult, MetadataValue, asset
from minio import Minio


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.stage_raw_tab import stage_raw_tab_stream
from scripts.publish_processed_with_dvc import publish_processed_batch
from postgres_catalog import (
    ensure_job_run,
    record_asset_materialization,
    resolve_pipeline_identity,
)


DEFAULT_RAW_BUCKET = "data-raw"
DEFAULT_STAGED_BUCKET = "data-staged"
DEFAULT_STAGED_PREFIX = "auair-tab"
DEFAULT_MULTIPART_PART_SIZE_MIB = 128
DEFAULT_ARTIFACT_BUCKET = "pipeline-artifacts"
DEFAULT_DATASET_ID = "auair"
AUAIR_MIN_COLUMN_COUNT = 17


def _environment_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def create_minio_client() -> Minio:
    """Create a MinIO client from host- or container-specific environment."""

    configured_endpoint = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000").strip()
    secure = _environment_flag("MINIO_SECURE")

    if "://" in configured_endpoint:
        parsed = urlparse(configured_endpoint)
        if not parsed.netloc:
            raise ValueError(f"Invalid MINIO_ENDPOINT: {configured_endpoint}")
        endpoint = parsed.netloc
        secure = parsed.scheme.lower() == "https"
    else:
        endpoint = configured_endpoint.rstrip("/")

    access_key = os.getenv(
        "MINIO_ACCESS_KEY",
        os.getenv("MINIO_ROOT_USER", "minioadmin"),
    )
    secret_key = os.getenv(
        "MINIO_SECRET_KEY",
        os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
    )
    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


def _normalise_etag(etag: str | None) -> str | None:
    return etag.strip('"') if etag else None


def _data_docs_object_key(prefix: str, relative_path: str) -> str:
    """Join a generated Data Docs path to its private MinIO bundle prefix."""

    relative = PurePosixPath(relative_path)
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ValueError(f"Unsafe Data Docs relative path: {relative_path}")
    return str(PurePosixPath(prefix) / relative)


def _artifact_content_type(path: Path) -> str:
    overrides = {
        ".css": "text/css",
        ".html": "text/html",
        ".js": "application/javascript",
        ".json": "application/json",
        ".otf": "font/otf",
        ".svg": "image/svg+xml",
        ".ttf": "font/ttf",
        ".woff": "font/woff",
        ".woff2": "font/woff2",
    }
    return overrides.get(
        path.suffix.lower(),
        mimetypes.guess_type(path.name)[0] or "application/octet-stream",
    )


def _staged_object_key(source_key: str, staged_prefix: str) -> str:
    source_name = PurePosixPath(source_key).name
    if not source_name:
        raise ValueError(f"Source object key has no file name: {source_key}")
    if not source_name.lower().endswith(".tab"):
        raise ValueError(
            f"Generated AU-AIR raw object must end with .tab: {source_key}"
        )

    staged_name = f"{source_name}.zst"
    prefix = staged_prefix.strip("/")
    return f"{prefix}/{staged_name}" if prefix else staged_name


class RawToStagedConfig(Config):
    """Generated AU-AIR object and compression settings from the raw sensor."""

    source_bucket: str = DEFAULT_RAW_BUCKET
    source_key: str
    source_etag: str | None = None
    staged_bucket: str = DEFAULT_STAGED_BUCKET
    staged_prefix: str = DEFAULT_STAGED_PREFIX
    zstd_level: int = 12
    zstd_threads: int = 0
    multipart_part_size_mib: int = DEFAULT_MULTIPART_PART_SIZE_MIB


@asset(
    group_name="auair_raw_to_staged",
    compute_kind="minio+zstd",
    description=(
        "Streams one generated AU-AIR .tab object from MinIO, cleans whitespace and "
        "delimiter artifacts, compresses it with ZSTD, and streams the "
        "result back to the staged bucket without local dataset files."
    ),
)
def staged_auair_tab(context, config: RawToStagedConfig) -> MaterializeResult:
    dataset_id = DEFAULT_DATASET_ID
    source_name = PurePosixPath(config.source_key).name
    staged_key = _staged_object_key(config.source_key, config.staged_prefix)
    batch_id = source_name[: -len(".tab")]
    catalog_run = _ensure_catalog_run(
        context,
        dataset_id=dataset_id,
        batch_id=batch_id,
    )
    client = create_minio_client()
    source_stat = client.stat_object(config.source_bucket, config.source_key)
    actual_source_etag = _normalise_etag(source_stat.etag)
    expected_source_etag = _normalise_etag(config.source_etag)
    if expected_source_etag and actual_source_etag != expected_source_etag:
        raise RuntimeError(
            "Raw object changed after the sensor observed it: "
            f"expected ETag {expected_source_etag}, got {actual_source_etag}."
        )

    multipart_part_size = config.multipart_part_size_mib * 1024 * 1024
    if config.multipart_part_size_mib < 5:
        raise ValueError("multipart_part_size_mib must be at least 5 MiB.")

    context.log.info(
        "Generated AU-AIR staging started: s3://%s/%s (etag=%s)",
        config.source_bucket,
        config.source_key,
        actual_source_etag,
    )

    if not client.bucket_exists(config.staged_bucket):
        client.make_bucket(config.staged_bucket)

    object_metadata = {
        "dataset-id": dataset_id,
        "source-bucket": config.source_bucket,
        "source-key": config.source_key,
        "source-etag": actual_source_etag or "unknown",
        "source-size-bytes": str(source_stat.size),
        "zstd-level": str(config.zstd_level),
    }

    source_stream = client.get_object(config.source_bucket, config.source_key)
    staged_stream = stage_raw_tab_stream(
        source_stream,
        zstd_level=config.zstd_level,
        zstd_threads=config.zstd_threads,
    )
    try:
        client.put_object(
            config.staged_bucket,
            staged_key,
            staged_stream,
            length=-1,
            part_size=multipart_part_size,
            content_type="application/zstd",
            metadata=object_metadata,
        )
        result = staged_stream.result()
    finally:
        staged_stream.close()
        source_stream.close()
        source_stream.release_conn()

    staged_stat = client.stat_object(config.staged_bucket, staged_key)
    if staged_stat.size != result.staged_size_bytes:
        raise RuntimeError(
            "Staged object size verification failed: "
            f"stream={result.staged_size_bytes}, MinIO={staged_stat.size}."
        )

    staged_etag = _normalise_etag(staged_stat.etag)
    context.log.info(
        "Generated AU-AIR staging completed: "
        "s3://%s/%s (%s rows, %s columns, %.2fx).",
        config.staged_bucket,
        staged_key,
        result.row_count,
        result.column_count,
        result.raw_to_staged_ratio,
    )

    result_metadata = asdict(result)
    source_uri = f"s3://{config.source_bucket}/{config.source_key}"
    staged_uri = f"s3://{config.staged_bucket}/{staged_key}"
    record_asset_materialization(
        context,
        catalog_run,
        asset_key="staged_auair_tab",
        asset_group="auair_raw_to_staged",
        dataset_id=dataset_id,
        batch_id=batch_id,
        input_uri=source_uri,
        input_etag=actual_source_etag or "unknown",
        output_uri=staged_uri,
        output_etag=staged_etag or "unknown",
        row_count=result.row_count,
        column_count=result.column_count,
        part_count=1,
        output_size_bytes=result.staged_size_bytes,
        metadata={
            "fields_trimmed": result.fields_trimmed,
            "trailing_fields_removed": result.trailing_fields_removed,
            "surplus_empty_fields_removed": result.surplus_empty_fields_removed,
            "blank_lines_skipped": result.blank_lines_skipped,
            "raw_size_bytes": result.raw_size_bytes,
            "cleaned_size_bytes": result.cleaned_size_bytes,
            "compression_ratio": result.raw_to_staged_ratio,
            "compression": result.compression,
            "zstd_level": result.zstd_level,
            "zstd_threads": result.zstd_threads,
            "multipart_part_size_mib": config.multipart_part_size_mib,
        },
    )
    downstream_value = {
        "dataset_id": dataset_id,
        "batch_id": batch_id,
        "staged_bucket": config.staged_bucket,
        "staged_key": staged_key,
        "staged_uri": staged_uri,
        "staged_etag": staged_etag or "unknown",
        "row_count": result.row_count,
        "column_count": result.column_count,
    }
    return MaterializeResult(
        value=downstream_value,
        metadata={
            "dataset_id": dataset_id,
            "batch_id": batch_id,
            "source_uri": source_uri,
            "source_etag": actual_source_etag or "unknown",
            "staged_uri": staged_uri,
            "staged_etag": staged_etag or "unknown",
            "row_count": result.row_count,
            "column_count": result.column_count,
            "fields_trimmed": result.fields_trimmed,
            "trailing_fields_removed": result.trailing_fields_removed,
            "surplus_empty_fields_removed": result.surplus_empty_fields_removed,
            "blank_lines_skipped": result.blank_lines_skipped,
            "raw_size_bytes": result.raw_size_bytes,
            "cleaned_size_bytes": result.cleaned_size_bytes,
            "staged_size_bytes": result.staged_size_bytes,
            "compression_ratio": result.raw_to_staged_ratio,
            "compression": result.compression,
            "zstd_level": result.zstd_level,
            "zstd_threads": result.zstd_threads,
            "multipart_part_size_mib": config.multipart_part_size_mib,
            "staging_result": MetadataValue.json(result_metadata),
        }
    )


def _dvc_repo_root() -> Path:
    repo_root = Path(os.getenv("DVC_REPO_ROOT", PROJECT_ROOT)).resolve()
    if not (repo_root / ".dvc").is_dir():
        raise RuntimeError(f"DVC repository is not available: {repo_root}")
    return repo_root


def _ensure_catalog_run(
    context,
    *,
    dataset_id: str | None = None,
    batch_id: str | None = None,
):
    repo_root = _dvc_repo_root()
    identity = resolve_pipeline_identity(repo_root)
    return ensure_job_run(
        context,
        identity,
        dataset_id=dataset_id,
        batch_id=batch_id,
    )


def _run_pipeline_script(context, command: list[str], *, cwd: Path) -> None:
    process = subprocess.Popen(
        command,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=os.name == "posix",
    )
    try:
        stdout, stderr = process.communicate()
    except BaseException:
        if process.poll() is None:
            try:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGTERM)
                else:
                    process.terminate()
                process.communicate(timeout=30)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                if process.poll() is None:
                    if os.name == "posix":
                        os.killpg(process.pid, signal.SIGKILL)
                    else:
                        process.kill()
                    process.communicate()
        raise

    if stdout:
        context.log.info(stdout.rstrip())
    if stderr:
        log_method = context.log.error if process.returncode else context.log.info
        log_method(stderr.rstrip())
    if process.returncode:
        raise RuntimeError(
            f"Pipeline script failed with exit code {process.returncode}: "
            f"{Path(command[1]).name}"
        )


class StagedAuairProcessingConfig(Config):
    """Polars preprocessing knobs for the staged generated AU-AIR batch.

    The staged object identity (bucket/key/etag) and the row/column counts now
    arrive from the upstream ``staged_auair_tab`` asset instead of a sensor, so
    only the tuning parameters remain configurable here.
    """

    max_columns: int = 100_000
    timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss.SSS"
    zstd_level: int = 12


@asset(
    group_name="auair_pipeline",
    compute_kind="polars",
    description=(
        "Downloads one staged generated AU-AIR .tab.zst object, preprocesses "
        "its dynamic columns with Polars, and writes ZSTD output parts."
    ),
)
def processed_auair_batch(
    context,
    staged_auair_tab: dict,
    config: StagedAuairProcessingConfig,
) -> MaterializeResult:
    repo_root = _dvc_repo_root()
    dataset_id = DEFAULT_DATASET_ID
    batch_id = staged_auair_tab["batch_id"]
    row_count = staged_auair_tab["row_count"]
    column_count = staged_auair_tab["column_count"]
    source_bucket = staged_auair_tab["staged_bucket"]
    source_key = staged_auair_tab["staged_key"]
    expected_etag = _normalise_etag(staged_auair_tab["staged_etag"])
    if column_count < AUAIR_MIN_COLUMN_COUNT:
        raise ValueError(
            f"Generated AU-AIR telemetry must contain at least "
            f"{AUAIR_MIN_COLUMN_COUNT} columns; received "
            f"{column_count}."
        )

    catalog_run = _ensure_catalog_run(
        context,
        dataset_id=dataset_id,
        batch_id=batch_id,
    )

    client = create_minio_client()
    source_stat = client.stat_object(source_bucket, source_key)
    actual_etag = _normalise_etag(source_stat.etag) or "unknown"
    if expected_etag not in (None, "unknown") and actual_etag != expected_etag:
        raise RuntimeError(
            "Staged object changed after it was written: "
            f"expected {expected_etag}, got {actual_etag}."
        )

    batch_path = (
        repo_root
        / "data"
        / "processed"
        / dataset_id
        / "batches"
        / batch_id
    )
    script_path = repo_root / "scripts" / "preprocess_auair_tab_polars.py"
    if not script_path.is_file():
        raise FileNotFoundError(f"Polars preprocessing script missing: {script_path}")

    context.log.info(
        "Polars preprocessing started: s3://%s/%s -> %s",
        source_bucket,
        source_key,
        batch_path,
    )
    with tempfile.TemporaryDirectory(prefix="dpm-staged-input-") as temp:
        local_input = Path(temp) / PurePosixPath(source_key).name
        client.fget_object(
            source_bucket,
            source_key,
            str(local_input),
        )
        command = [
            sys.executable,
            str(script_path),
            "--input",
            str(local_input),
            "--output",
            str(batch_path),
            "--max-columns",
            str(config.max_columns),
            "--timestamp-format",
            config.timestamp_format,
            "--zstd-level",
            str(config.zstd_level),
        ]
        command.extend(["--min-columns", str(AUAIR_MIN_COLUMN_COUNT)])
        _run_pipeline_script(context, command, cwd=repo_root)

    part_files = sorted(batch_path.glob("part-*.tab.zst"))
    if not part_files:
        raise RuntimeError(f"Polars produced no ZSTD part files: {batch_path}")
    output_size = sum(path.stat().st_size for path in part_files)
    value = {
        "dataset_id": dataset_id,
        "batch_id": batch_id,
        "batch_path": str(batch_path),
        "source_bucket": source_bucket,
        "source_key": source_key,
        "source_etag": actual_etag,
        "row_count": row_count,
        "column_count": column_count,
        "part_count": len(part_files),
        "output_size_bytes": output_size,
        "processing_engine": "polars",
    }
    source_uri = f"s3://{source_bucket}/{source_key}"
    record_asset_materialization(
        context,
        catalog_run,
        asset_key="processed_auair_batch",
        asset_group="auair_pipeline",
        dataset_id=dataset_id,
        batch_id=batch_id,
        input_uri=source_uri,
        input_etag=actual_etag,
        output_uri=batch_path.as_uri(),
        row_count=row_count,
        column_count=column_count,
        part_count=len(part_files),
        output_size_bytes=output_size,
        metadata={
            "processed_path": str(batch_path),
            "processing_engine": "polars",
            "zstd_level": config.zstd_level,
        },
    )
    return MaterializeResult(
        value=value,
        metadata={
            "dataset_id": dataset_id,
            "batch_id": batch_id,
            "source_uri": source_uri,
            "source_etag": actual_etag,
            "processed_path": MetadataValue.path(str(batch_path)),
            "row_count": row_count,
            "column_count": column_count,
            "part_count": len(part_files),
            "output_size_bytes": output_size,
            "processing_engine": "polars",
        },
    )


class ProcessedAuairValidationConfig(Config):
    """Great Expectations settings for the processed AU-AIR batch."""

    artifact_bucket: str = DEFAULT_ARTIFACT_BUCKET
    result_format: str = "BASIC"
    max_columns: int = 100_000
    timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss.SSS"
    spark_master: str = "local[2]"


@asset(
    group_name="auair_pipeline",
    compute_kind="great_expectations+spark",
    description=(
        "Validates one processed generated AU-AIR batch with Great Expectations "
        "on Spark and uploads its JSON result and Data Docs bundle to MinIO."
    ),
)
def validated_auair_batch(
    context,
    processed_auair_batch: dict,
    config: ProcessedAuairValidationConfig,
) -> MaterializeResult:
    repo_root = _dvc_repo_root()
    dataset_id = processed_auair_batch["dataset_id"]
    batch_id = processed_auair_batch["batch_id"]
    catalog_run = _ensure_catalog_run(
        context,
        dataset_id=dataset_id,
        batch_id=batch_id,
    )
    source_etag = processed_auair_batch["source_etag"]
    script_path = repo_root / "scripts" / "validate_auair_tab_spark_ge.py"
    if not script_path.is_file():
        raise FileNotFoundError(f"GE validation script missing: {script_path}")

    report_key = (
        f"validation/{dataset_id}/{batch_id}/{source_etag[:12]}.json"
    )
    data_docs_prefix = (
        f"validation/{dataset_id}/{batch_id}/{source_etag[:12]}"
    )
    with tempfile.TemporaryDirectory(prefix="dpm-validation-report-") as temp:
        report_path = Path(temp) / "ge-validation-result.json"
        data_docs_directory = Path(temp) / "data-docs"
        command = [
            sys.executable,
            str(script_path),
            "--input",
            processed_auair_batch["batch_path"],
            "--report",
            str(report_path),
            "--data-docs-dir",
            str(data_docs_directory),
            "--expected-row-count",
            str(processed_auair_batch["row_count"]),
            "--result-format",
            config.result_format,
            "--max-columns",
            str(config.max_columns),
            "--timestamp-format",
            config.timestamp_format,
            "--spark-master",
            config.spark_master,
        ]
        command.extend(
            [
                "--expected-column-count",
                str(processed_auair_batch["column_count"]),
            ]
        )
        execution_error = None
        try:
            _run_pipeline_script(context, command, cwd=repo_root)
        except RuntimeError as error:
            execution_error = error

        if not report_path.is_file():
            if execution_error:
                raise execution_error
            raise RuntimeError(f"GE report was not created: {report_path}")

        report = json.loads(report_path.read_text(encoding="utf-8"))
        data_docs = report.get("data_docs")
        if not isinstance(data_docs, dict):
            raise RuntimeError("GE report does not describe a Data Docs bundle.")

        index_key = _data_docs_object_key(
            data_docs_prefix,
            str(data_docs.get("index_path", "")),
        )
        validation_key = _data_docs_object_key(
            data_docs_prefix,
            str(data_docs.get("validation_path", "")),
        )
        if not (data_docs_directory / data_docs["index_path"]).is_file():
            raise RuntimeError("GE Data Docs index file is missing.")
        if not (data_docs_directory / data_docs["validation_path"]).is_file():
            raise RuntimeError("GE Data Docs validation page is missing.")

        client = create_minio_client()
        if not client.bucket_exists(config.artifact_bucket):
            client.make_bucket(config.artifact_bucket)

        generated_files = sorted(
            path for path in data_docs_directory.rglob("*") if path.is_file()
        )
        if len(generated_files) != int(data_docs.get("file_count", -1)):
            raise RuntimeError(
                "GE Data Docs file count changed before its MinIO upload."
            )
        object_metadata = {
            "dataset-id": dataset_id,
            "batch-id": batch_id,
            "source-etag": source_etag,
        }
        for generated_file in generated_files:
            relative_path = generated_file.relative_to(
                data_docs_directory
            ).as_posix()
            client.fput_object(
                config.artifact_bucket,
                _data_docs_object_key(data_docs_prefix, relative_path),
                str(generated_file),
                content_type=_artifact_content_type(generated_file),
                metadata=object_metadata,
            )

        data_docs.update(
            {
                "index_key": index_key,
                "validation_key": validation_key,
                "index_uri": f"s3://{config.artifact_bucket}/{index_key}",
                "validation_uri": (
                    f"s3://{config.artifact_bucket}/{validation_key}"
                ),
            }
        )
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        client.fput_object(
            config.artifact_bucket,
            report_key,
            str(report_path),
            content_type="application/json",
            metadata=object_metadata,
        )
        context.log.info(
            "Quality report and %s Data Docs files uploaded: s3://%s/%s",
            len(generated_files),
            config.artifact_bucket,
            report_key,
        )

    statistics = report.get("statistics", {})
    metadata = {
        "dataset_id": dataset_id,
        "batch_id": batch_id,
        "quality_status": "passed" if report.get("success") else "failed",
        "evaluated_expectations": statistics.get("evaluated_expectations", 0),
        "successful_expectations": statistics.get("successful_expectations", 0),
        "unsuccessful_expectations": statistics.get(
            "unsuccessful_expectations", 0
        ),
        "success_percent": statistics.get("success_percent", 0.0),
        "report_uri": f"s3://{config.artifact_bucket}/{report_key}",
        "data_docs_uri": report["data_docs"]["validation_uri"],
        "quality_report": MetadataValue.json(report),
    }
    if execution_error or not report.get("success", False):
        raise Failure(
            description=f"GE validation failed for {dataset_id}/{batch_id}.",
            metadata=metadata,
        ) from execution_error

    value = dict(processed_auair_batch)
    value["report_uri"] = f"s3://{config.artifact_bucket}/{report_key}"
    value["data_docs_uri"] = report["data_docs"]["validation_uri"]
    value["validation_statistics"] = statistics
    record_asset_materialization(
        context,
        catalog_run,
        asset_key="validated_auair_batch",
        asset_group="auair_pipeline",
        dataset_id=dataset_id,
        batch_id=batch_id,
        input_uri=Path(processed_auair_batch["batch_path"]).resolve().as_uri(),
        input_etag=source_etag,
        output_uri=value["report_uri"],
        row_count=processed_auair_batch["row_count"],
        column_count=processed_auair_batch["column_count"],
        part_count=processed_auair_batch["part_count"],
        output_size_bytes=processed_auair_batch["output_size_bytes"],
        metadata={
            "quality_status": "passed",
            "evaluated_expectations": statistics.get(
                "evaluated_expectations", 0
            ),
            "successful_expectations": statistics.get(
                "successful_expectations", 0
            ),
            "unsuccessful_expectations": statistics.get(
                "unsuccessful_expectations", 0
            ),
            "success_percent": statistics.get("success_percent", 0.0),
            "validation_report_uri": value["report_uri"],
            "data_docs_uri": value["data_docs_uri"],
        },
    )
    return MaterializeResult(value=value, metadata=metadata)

class ClickHouseAuairConfig(Config):
    """High-column ClickHouse destination and bulk-load safety limits."""

    database: str = os.getenv("CLICKHOUSE_DATABASE", "default")
    table: str = os.getenv("CLICKHOUSE_AUAIR_TABLE", "auair_telemetry")
    visible_view: str = os.getenv(
        "CLICKHOUSE_AUAIR_VISIBLE_VIEW",
        "auair_telemetry_committed",
    )
    commit_table: str = os.getenv(
        "CLICKHOUSE_AUAIR_COMMIT_TABLE",
        "auair_telemetry_workflow_commits",
    )
    safe_cell_limit: int = int(
        os.getenv("CLICKHOUSE_WIDE_SAFE_CELL_LIMIT", "1000000000")
    )
    max_rows_per_chunk: int = int(
        os.getenv("CLICKHOUSE_WIDE_MAX_ROWS_PER_CHUNK", "200000")
    )
    row_chunk_zstd_level: int = int(
        os.getenv("CLICKHOUSE_ROW_CHUNK_ZSTD_LEVEL", "6")
    )
    ingest_bucket: str = os.getenv(
        "CLICKHOUSE_INGEST_BUCKET", DEFAULT_ARTIFACT_BUCKET
    )
    ingest_prefix: str = os.getenv(
        "CLICKHOUSE_INGEST_PREFIX", "_clickhouse-ingest/auair"
    )


@asset(
    group_name="auair_pipeline",
    compute_kind="clickhouse",
    description=(
        "Writes a validated dynamic-column AU-AIR batch to ClickHouse before "
        "the batch is allowed to proceed to DVC publication."
    ),
)
def clickhouse_auair_batch(
    context,
    validated_auair_batch: dict,
    config: ClickHouseAuairConfig,
) -> MaterializeResult:
    repo_root = _dvc_repo_root()
    dataset_id = validated_auair_batch["dataset_id"]
    batch_id = validated_auair_batch["batch_id"]
    catalog_run = _ensure_catalog_run(
        context,
        dataset_id=dataset_id,
        batch_id=batch_id,
    )
    script_path = (
        repo_root / "scripts" / "load_validated_auair_to_clickhouse.py"
    )
    if not script_path.is_file():
        raise FileNotFoundError(f"ClickHouse loader script missing: {script_path}")

    with tempfile.TemporaryDirectory(prefix="dpm-auair-clickhouse-") as temp:
        metadata_path = Path(temp) / "clickhouse-load.json"
        command = [
            sys.executable,
            str(script_path),
            "--input",
            validated_auair_batch["batch_path"],
            "--dataset-id",
            dataset_id,
            "--batch-id",
            batch_id,
            "--dagster-run-id",
            context.run_id,
            "--source-etag",
            validated_auair_batch["source_etag"],
            "--validation-report-uri",
            validated_auair_batch["report_uri"],
            "--expected-row-count",
            str(validated_auair_batch["row_count"]),
            "--expected-column-count",
            str(validated_auair_batch["column_count"]),
            "--metadata-out",
            str(metadata_path),
            "--database",
            config.database,
            "--table",
            config.table,
            "--visible-view",
            config.visible_view,
            "--commit-table",
            config.commit_table,
            "--safe-cell-limit",
            str(config.safe_cell_limit),
            "--max-rows-per-chunk",
            str(config.max_rows_per_chunk),
            "--row-chunk-zstd-level",
            str(config.row_chunk_zstd_level),
            "--ingest-bucket",
            config.ingest_bucket,
            "--ingest-prefix",
            config.ingest_prefix,
        ]
        context.log.info(
            "ClickHouse AU-AIR load started: %s/%s -> %s.%s",
            dataset_id,
            batch_id,
            config.database,
            config.table,
        )
        _run_pipeline_script(context, command, cwd=repo_root)
        if not metadata_path.is_file():
            raise RuntimeError(
                f"ClickHouse loader metadata was not created: {metadata_path}"
            )
        load_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    value = dict(validated_auair_batch)
    value["clickhouse"] = load_metadata
    record_asset_materialization(
        context,
        catalog_run,
        asset_key="clickhouse_auair_batch",
        asset_group="auair_pipeline",
        dataset_id=dataset_id,
        batch_id=batch_id,
        input_uri=Path(validated_auair_batch["batch_path"]).resolve().as_uri(),
        input_etag=validated_auair_batch["source_etag"],
        output_uri=load_metadata["output_uri"],
        row_count=load_metadata["stored_row_count"],
        column_count=load_metadata["source_column_count"],
        part_count=load_metadata["input_part_count"],
        metadata={
            "clickhouse_database": load_metadata["database"],
            "clickhouse_table": load_metadata["table"],
            "clickhouse_visible_view": load_metadata["visible_view"],
            "clickhouse_workflow_commit_table": load_metadata[
                "workflow_commit_table"
            ],
            "workflow_visibility": load_metadata["workflow_visibility"],
            "flight_count": load_metadata["flight_count"],
            "flight_ids": load_metadata["flight_ids"],
            "insert_chunk_count": load_metadata["insert_chunk_count"],
            "row_chunk_size": load_metadata["row_chunk_size"],
            "safe_cell_limit": load_metadata["safe_cell_limit"],
            "load_method": load_metadata["load_method"],
            "storage_layout": load_metadata["storage_layout"],
            "storage_codec": load_metadata["storage_codec"],
            "validation_report_uri": validated_auair_batch["report_uri"],
        },
    )
    return MaterializeResult(
        value=value,
        metadata={
            "dataset_id": dataset_id,
            "batch_id": batch_id,
            "clickhouse_database": load_metadata["database"],
            "clickhouse_table": load_metadata["table"],
            "clickhouse_visible_view": load_metadata["visible_view"],
            "workflow_visibility": load_metadata["workflow_visibility"],
            "row_count": load_metadata["stored_row_count"],
            "column_count": load_metadata["source_column_count"],
            "flight_count": load_metadata["flight_count"],
            "flight_ids": MetadataValue.json(load_metadata["flight_ids"]),
            "insert_chunk_count": load_metadata["insert_chunk_count"],
            "row_chunk_size": load_metadata["row_chunk_size"],
            "safe_cell_limit": load_metadata["safe_cell_limit"],
            "load_method": load_metadata["load_method"],
            "storage_layout": load_metadata["storage_layout"],
            "output_uri": load_metadata["output_uri"],
        },
    )


@asset(
    group_name="auair_pipeline",
    compute_kind="dvc+minio",
    description=(
        "Publishes the processed AU-AIR dataset to the MinIO-backed DVC remote "
        "only after the ClickHouse write has completed successfully."
    ),
)
def published_auair_dataset(
    context,
    clickhouse_auair_batch: dict,
) -> MaterializeResult:
    repo_root = _dvc_repo_root()
    dataset_id = clickhouse_auair_batch["dataset_id"]
    batch_id = clickhouse_auair_batch["batch_id"]
    catalog_run = _ensure_catalog_run(
        context,
        dataset_id=dataset_id,
        batch_id=batch_id,
    )
    pipeline_identity = catalog_run.identity
    result = publish_processed_batch(
        clickhouse_auair_batch["batch_path"],
        repo_root=repo_root,
        dataset_id=dataset_id,
        column_count=clickhouse_auair_batch["column_count"],
        row_count=clickhouse_auair_batch["row_count"],
        batch_id=batch_id,
        dvc_remote=os.getenv("DVC_REMOTE_NAME", "minio"),
        dvc_remote_url=os.getenv("DVC_REMOTE_URL", "s3://dvc-cache"),
        dvc_endpoint_url=os.getenv(
            "DVC_S3_ENDPOINT_URL", "http://127.0.0.1:9000"
        ),
    )
    relative_pointer = result.pointer_path.relative_to(repo_root)
    clickhouse_uri = clickhouse_auair_batch["clickhouse"]["output_uri"]
    record_asset_materialization(
        context,
        catalog_run,
        asset_key="published_auair_dataset",
        asset_group="auair_pipeline",
        dataset_id=result.dataset_id,
        batch_id=result.batch_id,
        input_uri=Path(clickhouse_auair_batch["batch_path"]).resolve().as_uri(),
        input_etag=clickhouse_auair_batch["source_etag"],
        output_uri=relative_pointer.as_posix(),
        output_etag=result.dvc_hash,
        row_count=clickhouse_auair_batch["row_count"],
        column_count=clickhouse_auair_batch["column_count"],
        part_count=result.file_count,
        output_size_bytes=result.size_bytes,
        metadata={
            "clickhouse_uri": clickhouse_uri,
            "dvc_pointer": relative_pointer.as_posix(),
            "dvc_remote": result.dvc_remote,
            "dvc_remote_url": result.dvc_remote_url,
            "dvc_hash_name": result.dvc_hash_name,
            "dvc_hash": result.dvc_hash,
            "validation_report_uri": clickhouse_auair_batch["report_uri"],
        },
    )
    value = dict(clickhouse_auair_batch)
    value["dvc"] = {
        "pointer": relative_pointer.as_posix(),
        "remote": result.dvc_remote,
        "remote_url": result.dvc_remote_url,
        "hash_name": result.dvc_hash_name,
        "hash": result.dvc_hash,
    }
    return MaterializeResult(
        value=value,
        metadata={
            "dataset_id": result.dataset_id,
            "batch_id": result.batch_id,
            "clickhouse_uri": clickhouse_uri,
            "dvc_pointer": relative_pointer.as_posix(),
            "dvc_remote": result.dvc_remote,
            "dvc_remote_url": result.dvc_remote_url,
            "dvc_hash_name": result.dvc_hash_name,
            "dvc_hash": result.dvc_hash,
            "pipeline_version": pipeline_identity.version,
            "pipeline_git_tag": pipeline_identity.git_tag or "none",
            "pipeline_git_sha": pipeline_identity.git_sha,
            "pipeline_git_dirty": pipeline_identity.git_dirty,
            "repository_git_sha": pipeline_identity.repository_git_sha,
            "repository_git_dirty": pipeline_identity.repository_git_dirty,
            "validation_report_uri": clickhouse_auair_batch["report_uri"],
        },
    )
