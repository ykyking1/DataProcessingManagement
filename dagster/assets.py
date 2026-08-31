"""Dagster assets for the MinIO-backed flight telemetry workflow."""

import io
import json
import os
import re
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
from scripts.convert_auair_tab import FLIGHT_COLUMNS as AUAIR_FRAME_COLUMNS
from scripts.convert_auair_tab import convert as convert_auair_wide_tab
from postgres_catalog import (
    ensure_job_run,
    record_asset_materialization,
    resolve_pipeline_identity,
)


DEFAULT_RAW_BUCKET = "data-raw"
DEFAULT_STAGED_BUCKET = "data-staged"
DEFAULT_STAGED_PREFIX = "flight-tab"
DEFAULT_MULTIPART_PART_SIZE_MIB = 128
DEFAULT_ARTIFACT_BUCKET = "pipeline-artifacts"
DEFAULT_DATASET_ID = "flightdemo"
# Native AU-AIR frame schema: flight_id, time, image_name, image_width,
# image_height, platform, longitude, latitude, altitude, linear_x/y/z,
# angle_phi/theta/psi, num_objects, obj_human/car/truck/van/motorbike/
# bicycle/bus/trailer.
FLIGHT_COLUMN_COUNT = 24


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


def _staged_object_key(source_key: str, staged_prefix: str) -> str:
    source_name = PurePosixPath(source_key).name
    if not source_name:
        raise ValueError(f"Source object key has no file name: {source_key}")
    if not source_name.lower().endswith(".tab"):
        raise ValueError(f"Raw flight object must end with .tab: {source_key}")

    staged_name = f"{source_name}.zst"
    prefix = staged_prefix.strip("/")
    return f"{prefix}/{staged_name}" if prefix else staged_name


_BATCH_ID_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


def _sanitise_batch_id(name: str) -> str:
    """Turn an arbitrary raw file stem into a filesystem-safe batch id.

    The staged filename is no longer required to follow the
    ``flightdemo_<N>rows`` convention (see staged_flight_minio_sensor);
    the batch id is derived from whatever name the operator uploaded and
    is used as a directory name under data/processed/flightdemo/batches/.
    """

    cleaned = _BATCH_ID_UNSAFE.sub("_", name).strip("._-")
    return cleaned or "batch"


def _normalise_flight_frame_tab(context, raw_path: Path, work_dir: Path) -> Path:
    """Return a path to a .tab that follows the native 24-column frame contract.

    Accepts, without a preconversion step, either:
      * a file already in the native contract (header == AUAIR_FRAME_COLUMNS),
        returned unchanged; or
      * a wide AU-AIR export (one anchor frame followed by hundreds of
        repeated ``image_name, ...`` blocks, ``longtitude`` header typo),
        which is projected to the first frame block via convert_auair_tab.

    Anything else raises, so a genuinely wrong schema still fails loudly.
    """

    with raw_path.open("r", encoding="utf-8", newline="") as handle:
        header_line = handle.readline()

    if not header_line:
        raise Failure(description=f"Raw flight .tab is empty: {raw_path.name}")

    header = [field.strip() for field in header_line.rstrip("\r\n").split("\t")]
    fixed = ["longitude" if name == "longtitude" else name for name in header]
    contract = list(AUAIR_FRAME_COLUMNS)

    if fixed == contract:
        return raw_path

    looks_like_auair_wide = (
        fixed[: len(contract)] == contract or "longtitude" in header
    )
    if looks_like_auair_wide:
        context.log.info(
            "Wide AU-AIR export detected (%d header columns); projecting to "
            "the %d-column frame contract with convert_auair_tab.",
            len(header),
            len(contract),
        )
        try:
            converted = convert_auair_wide_tab(
                raw_path,
                work_dir / "converted",
                label="auair",
                assume_utc=True,
                limit=None,
            )
        except SystemExit as exc:  # convert_auair_tab uses SystemExit for errors
            raise Failure(
                description=f"AU-AIR wide .tab conversion failed: {exc}"
            ) from exc
        return Path(converted)

    raise Failure(
        description=(
            "Raw flight .tab header is neither the native frame contract nor a "
            "recognised wide AU-AIR export. First columns: "
            + ", ".join(header[:6])
        )
    )


class RawToStagedConfig(Config):
    """Source object and compression settings supplied by the raw sensor."""

    source_bucket: str = DEFAULT_RAW_BUCKET
    source_key: str
    source_etag: str | None = None
    staged_bucket: str = DEFAULT_STAGED_BUCKET
    staged_prefix: str = DEFAULT_STAGED_PREFIX
    zstd_level: int = 12
    zstd_threads: int = 0
    multipart_part_size_mib: int = DEFAULT_MULTIPART_PART_SIZE_MIB


@asset(
    group_name="raw_to_staged",
    compute_kind="minio+zstd",
    description=(
        "Streams one raw flight .tab object from MinIO, cleans whitespace and "
        "delimiter artifacts, compresses it with ZSTD, and streams the "
        "result back to the staged bucket without local dataset files."
    ),
)
def staged_flight_tab(context, config: RawToStagedConfig) -> MaterializeResult:
    catalog_run = _ensure_catalog_run(context)
    client = create_minio_client()
    source_stat = client.stat_object(config.source_bucket, config.source_key)
    actual_source_etag = _normalise_etag(source_stat.etag)
    expected_source_etag = _normalise_etag(config.source_etag)
    if expected_source_etag and actual_source_etag != expected_source_etag:
        raise RuntimeError(
            "Raw object changed after the sensor observed it: "
            f"expected ETag {expected_source_etag}, got {actual_source_etag}."
        )

    staged_key = _staged_object_key(config.source_key, config.staged_prefix)
    multipart_part_size = config.multipart_part_size_mib * 1024 * 1024
    if config.multipart_part_size_mib < 5:
        raise ValueError("multipart_part_size_mib must be at least 5 MiB.")

    context.log.info(
        "Raw flight staging started: s3://%s/%s (etag=%s)",
        config.source_bucket,
        config.source_key,
        actual_source_etag,
    )

    if not client.bucket_exists(config.staged_bucket):
        client.make_bucket(config.staged_bucket)

    object_metadata = {
        "source-bucket": config.source_bucket,
        "source-key": config.source_key,
        "source-etag": actual_source_etag or "unknown",
        "source-size-bytes": str(source_stat.size),
        "zstd-level": str(config.zstd_level),
    }

    # The raw object is pulled to a temporary local file so its header can
    # be inspected and, if it is a wide AU-AIR export, projected to the
    # native 24-column frame contract before staging (see
    # _normalise_flight_frame_tab). A native-contract file passes through
    # untouched. Everything downstream still receives a strict 24-column
    # .tab.zst, so the Spark / Great Expectations / ClickHouse guards are
    # unchanged.
    with tempfile.TemporaryDirectory(prefix="dpm-raw-flight-") as raw_temp_dir:
        raw_temp_path = Path(raw_temp_dir)
        local_raw = raw_temp_path / PurePosixPath(config.source_key).name
        client.fget_object(
            config.source_bucket,
            config.source_key,
            str(local_raw),
        )

        stage_input_path = _normalise_flight_frame_tab(
            context, local_raw, raw_temp_path
        )

        with stage_input_path.open("rb") as source_stream:
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

    staged_stat = client.stat_object(config.staged_bucket, staged_key)
    if staged_stat.size != result.staged_size_bytes:
        raise RuntimeError(
            "Staged object size verification failed: "
            f"stream={result.staged_size_bytes}, MinIO={staged_stat.size}."
        )

    staged_etag = _normalise_etag(staged_stat.etag)
    context.log.info(
        "Raw flight staging completed: s3://%s/%s (%s rows, %s columns, %.2fx).",
        config.staged_bucket,
        staged_key,
        result.row_count,
        result.column_count,
        result.raw_to_staged_ratio,
    )

    result_metadata = asdict(result)
    source_name = PurePosixPath(config.source_key).name
    dataset_id = DEFAULT_DATASET_ID
    batch_id = _sanitise_batch_id(
        source_name[: -len(".tab")]
        if source_name.lower().endswith(".tab")
        else source_name
    )

    # Sidecar next to the staged object: staged_flight_minio_sensor reads
    # this instead of parsing row_count / column_count / dataset / batch
    # out of the filename, so the staged file name is now free-form.
    sidecar_key = f"{staged_key}.meta.json"
    sidecar_bytes = json.dumps(
        {
            "dataset_id": dataset_id,
            "batch_id": batch_id,
            "row_count": result.row_count,
            "column_count": result.column_count,
            "source_key": config.source_key,
            "source_etag": actual_source_etag or "unknown",
        },
        sort_keys=True,
    ).encode("utf-8")
    client.put_object(
        config.staged_bucket,
        sidecar_key,
        io.BytesIO(sidecar_bytes),
        length=len(sidecar_bytes),
        content_type="application/json",
    )

    source_uri = f"s3://{config.source_bucket}/{config.source_key}"
    staged_uri = f"s3://{config.staged_bucket}/{staged_key}"
    record_asset_materialization(
        context,
        catalog_run,
        asset_key="staged_flight_tab",
        asset_group="raw_to_staged",
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
    return MaterializeResult(
        metadata={
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
    result = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        context.log.info(result.stdout.rstrip())
    if result.stderr:
        log_method = context.log.error if result.returncode else context.log.info
        log_method(result.stderr.rstrip())
    if result.returncode:
        raise RuntimeError(
            f"Pipeline script failed with exit code {result.returncode}: "
            f"{Path(command[1]).name}"
        )


class StagedBatchProcessingConfig(Config):
    """Staged MinIO object selected by the staged-data sensor."""

    source_bucket: str = DEFAULT_STAGED_BUCKET
    source_key: str
    source_etag: str
    dataset_id: str
    batch_id: str
    row_count: int
    column_count: int
    max_columns: int = 100_000
    timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
    spark_master: str = "local[2]"
    zstd_level: int = 12


@asset(
    group_name="staged_to_published",
    compute_kind="spark",
    description=(
        "Downloads one staged flight .tab.zst object, preprocesses it with local "
        "Spark, and writes the batch directly into its stable DVC dataset "
        "workspace."
    ),
)
def processed_flight_batch(
    context,
    config: StagedBatchProcessingConfig,
) -> MaterializeResult:
    repo_root = _dvc_repo_root()
    expected_dataset_id = DEFAULT_DATASET_ID
    catalog_run = _ensure_catalog_run(
        context,
        dataset_id=expected_dataset_id,
        batch_id=config.batch_id,
    )
    if config.dataset_id.lower() != expected_dataset_id:
        raise ValueError(
            f"Dataset id {config.dataset_id!r} does not match "
            f"the active flight dataset {expected_dataset_id!r}."
        )
    if config.column_count != FLIGHT_COLUMN_COUNT:
        raise ValueError(
            f"Flight telemetry must contain {FLIGHT_COLUMN_COUNT} columns; "
            f"received {config.column_count}."
        )

    client = create_minio_client()
    source_stat = client.stat_object(config.source_bucket, config.source_key)
    actual_etag = _normalise_etag(source_stat.etag) or "unknown"
    if actual_etag != _normalise_etag(config.source_etag):
        raise RuntimeError(
            "Staged object changed after the sensor observed it: "
            f"expected {config.source_etag}, got {actual_etag}."
        )

    batch_path = (
        repo_root
        / "data"
        / "processed"
        / expected_dataset_id
        / "batches"
        / config.batch_id
    )
    script_path = repo_root / "scripts" / "preprocess_flight_tab_spark.py"
    if not script_path.is_file():
        raise FileNotFoundError(f"Spark preprocessing script missing: {script_path}")

    context.log.info(
        "Spark preprocessing started: s3://%s/%s -> %s",
        config.source_bucket,
        config.source_key,
        batch_path,
    )
    with tempfile.TemporaryDirectory(prefix="dpm-staged-input-") as temp:
        local_input = Path(temp) / PurePosixPath(config.source_key).name
        client.fget_object(
            config.source_bucket,
            config.source_key,
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
            "--spark-master",
            config.spark_master,
            "--zstd-level",
            str(config.zstd_level),
        ]
        _run_pipeline_script(context, command, cwd=repo_root)

    part_files = sorted(batch_path.glob("part-*.tab.zst"))
    if not part_files:
        raise RuntimeError(f"Spark produced no ZSTD part files: {batch_path}")
    output_size = sum(path.stat().st_size for path in part_files)
    value = {
        "dataset_id": expected_dataset_id,
        "batch_id": config.batch_id,
        "batch_path": str(batch_path),
        "source_bucket": config.source_bucket,
        "source_key": config.source_key,
        "source_etag": actual_etag,
        "row_count": config.row_count,
        "column_count": config.column_count,
        "part_count": len(part_files),
        "output_size_bytes": output_size,
        "spark_master": config.spark_master,
    }
    source_uri = f"s3://{config.source_bucket}/{config.source_key}"
    record_asset_materialization(
        context,
        catalog_run,
        asset_key="processed_flight_batch",
        asset_group="staged_to_published",
        dataset_id=expected_dataset_id,
        batch_id=config.batch_id,
        input_uri=source_uri,
        input_etag=actual_etag,
        output_uri=batch_path.as_uri(),
        row_count=config.row_count,
        column_count=config.column_count,
        part_count=len(part_files),
        output_size_bytes=output_size,
        metadata={
            "processed_path": str(batch_path),
            "spark_master": config.spark_master,
            "zstd_level": config.zstd_level,
        },
    )
    return MaterializeResult(
        value=value,
        metadata={
            "dataset_id": expected_dataset_id,
            "batch_id": config.batch_id,
            "source_uri": source_uri,
            "source_etag": actual_etag,
            "processed_path": MetadataValue.path(str(batch_path)),
            "row_count": config.row_count,
            "column_count": config.column_count,
            "part_count": len(part_files),
            "output_size_bytes": output_size,
            "spark_master": config.spark_master,
        },
    )


class ProcessedBatchValidationConfig(Config):
    """Great Expectations settings for the processed flight batch."""

    artifact_bucket: str = DEFAULT_ARTIFACT_BUCKET
    result_format: str = "BASIC"
    max_columns: int = 100_000
    timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
    spark_master: str = "local[2]"


@asset(
    group_name="staged_to_published",
    compute_kind="great_expectations+spark",
    description=(
        "Validates one processed flight batch with Great Expectations on Spark "
        "and uploads the JSON quality report to MinIO."
    ),
)
def validated_flight_batch(
    context,
    processed_flight_batch: dict,
    config: ProcessedBatchValidationConfig,
) -> MaterializeResult:
    repo_root = _dvc_repo_root()
    dataset_id = processed_flight_batch["dataset_id"]
    batch_id = processed_flight_batch["batch_id"]
    catalog_run = _ensure_catalog_run(
        context,
        dataset_id=dataset_id,
        batch_id=batch_id,
    )
    source_etag = processed_flight_batch["source_etag"]
    script_path = repo_root / "scripts" / "validate_flight_tab_spark_ge.py"
    if not script_path.is_file():
        raise FileNotFoundError(f"GE validation script missing: {script_path}")

    report_key = (
        f"validation/{dataset_id}/{batch_id}/{source_etag[:12]}.json"
    )
    with tempfile.TemporaryDirectory(prefix="dpm-validation-report-") as temp:
        report_path = Path(temp) / "ge-validation-result.json"
        command = [
            sys.executable,
            str(script_path),
            "--input",
            processed_flight_batch["batch_path"],
            "--report",
            str(report_path),
            "--expected-row-count",
            str(processed_flight_batch["row_count"]),
            "--result-format",
            config.result_format,
            "--max-columns",
            str(config.max_columns),
            "--timestamp-format",
            config.timestamp_format,
            "--spark-master",
            config.spark_master,
        ]
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
        client = create_minio_client()
        if not client.bucket_exists(config.artifact_bucket):
            client.make_bucket(config.artifact_bucket)
        client.fput_object(
            config.artifact_bucket,
            report_key,
            str(report_path),
            content_type="application/json",
            metadata={
                "dataset-id": dataset_id,
                "batch-id": batch_id,
                "source-etag": source_etag,
            },
        )
        context.log.info(
            "Quality report uploaded: s3://%s/%s",
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
        "quality_report": MetadataValue.json(report),
    }
    if execution_error or not report.get("success", False):
        raise Failure(
            description=f"GE validation failed for {dataset_id}/{batch_id}.",
            metadata=metadata,
        ) from execution_error

    value = dict(processed_flight_batch)
    value["report_uri"] = f"s3://{config.artifact_bucket}/{report_key}"
    value["validation_statistics"] = statistics
    record_asset_materialization(
        context,
        catalog_run,
        asset_key="validated_flight_batch",
        asset_group="staged_to_published",
        dataset_id=dataset_id,
        batch_id=batch_id,
        input_uri=Path(processed_flight_batch["batch_path"]).resolve().as_uri(),
        input_etag=source_etag,
        output_uri=value["report_uri"],
        row_count=processed_flight_batch["row_count"],
        column_count=processed_flight_batch["column_count"],
        part_count=processed_flight_batch["part_count"],
        output_size_bytes=processed_flight_batch["output_size_bytes"],
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
        },
    )
    return MaterializeResult(value=value, metadata=metadata)


class ClickHouseFlightConfig(Config):
    """Query-serving ClickHouse settings for one validated flight batch."""

    database: str = os.getenv("CLICKHOUSE_DATABASE", "default")
    table: str = os.getenv("CLICKHOUSE_TABLE", "telemetry")
    insert_chunk_rows: int = int(
        os.getenv("CLICKHOUSE_INSERT_CHUNK_ROWS", "10000")
    )


@asset(
    group_name="staged_to_published",
    compute_kind="clickhouse",
    description=(
        "Loads a successfully validated flight batch into the wide ClickHouse "
        "telemetry table consumed directly by the dashboard."
    ),
)
def clickhouse_flight_batch(
    context,
    validated_flight_batch: dict,
    config: ClickHouseFlightConfig,
) -> MaterializeResult:
    repo_root = _dvc_repo_root()
    dataset_id = validated_flight_batch["dataset_id"]
    batch_id = validated_flight_batch["batch_id"]
    catalog_run = _ensure_catalog_run(
        context,
        dataset_id=dataset_id,
        batch_id=batch_id,
    )
    script_path = (
        repo_root / "scripts" / "load_validated_flight_to_clickhouse.py"
    )
    if not script_path.is_file():
        raise FileNotFoundError(f"ClickHouse loader script missing: {script_path}")

    with tempfile.TemporaryDirectory(prefix="dpm-clickhouse-metadata-") as temp:
        metadata_path = Path(temp) / "clickhouse-load.json"
        command = [
            sys.executable,
            str(script_path),
            "--input",
            validated_flight_batch["batch_path"],
            "--dataset-id",
            dataset_id,
            "--batch-id",
            batch_id,
            "--dagster-run-id",
            context.run_id,
            "--source-etag",
            validated_flight_batch["source_etag"],
            "--validation-report-uri",
            validated_flight_batch["report_uri"],
            "--expected-row-count",
            str(validated_flight_batch["row_count"]),
            "--metadata-out",
            str(metadata_path),
            "--database",
            config.database,
            "--table",
            config.table,
            "--insert-chunk-rows",
            str(config.insert_chunk_rows),
        ]
        context.log.info(
            "ClickHouse flight telemetry load started: %s/%s -> %s.%s",
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

    value = dict(validated_flight_batch)
    value["clickhouse"] = load_metadata
    record_asset_materialization(
        context,
        catalog_run,
        asset_key="clickhouse_flight_batch",
        asset_group="staged_to_published",
        dataset_id=dataset_id,
        batch_id=batch_id,
        input_uri=Path(validated_flight_batch["batch_path"]).resolve().as_uri(),
        input_etag=validated_flight_batch["source_etag"],
        output_uri=load_metadata["output_uri"],
        row_count=load_metadata["telemetry_row_count"],
        column_count=load_metadata["source_column_count"],
        part_count=load_metadata["input_part_count"],
        metadata={
            "clickhouse_database": load_metadata["database"],
            "clickhouse_table": load_metadata["table"],
            "source_row_count": load_metadata["source_row_count"],
            "source_column_count": load_metadata["source_column_count"],
            "telemetry_row_count": load_metadata["telemetry_row_count"],
            "flight_count": load_metadata["flight_count"],
            "flight_ids": load_metadata["flight_ids"],
            "input_part_count": load_metadata["input_part_count"],
            "insert_chunk_count": load_metadata["insert_chunk_count"],
            "storage_codec": load_metadata["storage_codec"],
            "validation_report_uri": validated_flight_batch["report_uri"],
        },
    )
    return MaterializeResult(
        value=value,
        metadata={
            "dataset_id": dataset_id,
            "batch_id": batch_id,
            "clickhouse_database": load_metadata["database"],
            "clickhouse_table": load_metadata["table"],
            "source_row_count": load_metadata["source_row_count"],
            "telemetry_row_count": load_metadata["telemetry_row_count"],
            "flight_count": load_metadata["flight_count"],
            "flight_ids": MetadataValue.json(load_metadata["flight_ids"]),
            "insert_chunk_count": load_metadata["insert_chunk_count"],
            "storage_codec": load_metadata["storage_codec"],
            "output_uri": load_metadata["output_uri"],
        },
    )


@asset(
    group_name="staged_to_published",
    compute_kind="dvc+minio",
    description=(
        "Updates the logical flight telemetry DVC pointer and pushes content to the "
        "MinIO DVC remote."
    ),
)
def published_flight_dataset(
    context,
    validated_flight_batch: dict,
) -> MaterializeResult:
    repo_root = _dvc_repo_root()
    catalog_run = _ensure_catalog_run(
        context,
        dataset_id=validated_flight_batch["dataset_id"],
        batch_id=validated_flight_batch["batch_id"],
    )
    pipeline_identity = catalog_run.identity
    result = publish_processed_batch(
        validated_flight_batch["batch_path"],
        repo_root=repo_root,
        dataset_id=validated_flight_batch["dataset_id"],
        column_count=validated_flight_batch["column_count"],
        row_count=validated_flight_batch["row_count"],
        batch_id=validated_flight_batch["batch_id"],
        dvc_remote=os.getenv("DVC_REMOTE_NAME", "minio"),
        dvc_remote_url=os.getenv("DVC_REMOTE_URL", "s3://dvc-cache"),
        dvc_endpoint_url=os.getenv(
            "DVC_S3_ENDPOINT_URL", "http://127.0.0.1:9000"
        ),
    )
    relative_pointer = result.pointer_path.relative_to(repo_root)
    record_asset_materialization(
        context,
        catalog_run,
        asset_key="published_flight_dataset",
        asset_group="staged_to_published",
        dataset_id=result.dataset_id,
        batch_id=result.batch_id,
        input_uri=Path(validated_flight_batch["batch_path"]).resolve().as_uri(),
        input_etag=validated_flight_batch["source_etag"],
        output_uri=relative_pointer.as_posix(),
        output_etag=result.dvc_hash,
        row_count=validated_flight_batch["row_count"],
        column_count=validated_flight_batch["column_count"],
        part_count=result.file_count,
        output_size_bytes=result.size_bytes,
        metadata={
            "dvc_pointer": relative_pointer.as_posix(),
            "dvc_remote": result.dvc_remote,
            "dvc_remote_url": result.dvc_remote_url,
            "dvc_hash_name": result.dvc_hash_name,
            "dvc_hash": result.dvc_hash,
            "validation_report_uri": validated_flight_batch["report_uri"],
        },
    )
    return MaterializeResult(
        metadata={
            "dataset_id": result.dataset_id,
            "batch_id": result.batch_id,
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
            "validation_report_uri": validated_flight_batch["report_uri"],
        }
    )
