"""Dagster code location for the MinIO-backed data workflows."""

import json
import os
from pathlib import Path, PurePosixPath

from dagster import (
    AssetSelection,
    DagsterRunStatus,
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    RunsFilter,
    SkipReason,
    define_asset_job,
    run_status_sensor,
    sensor,
)
from dotenv import load_dotenv


load_dotenv(Path(__file__).resolve().parent / ".env")

import assets
from alerting import alert_on_failure, clear_alert_on_success
from postgres_catalog import record_terminal_job_run, resolve_pipeline_identity


raw_flight_to_staged_job = define_asset_job(
    name="raw_flight_to_staged_job",
    selection=AssetSelection.assets(assets.staged_flight_tab),
    hooks={alert_on_failure, clear_alert_on_success},
)

staged_flight_to_published_job = define_asset_job(
    name="staged_flight_to_published_job",
    selection=AssetSelection.assets(
        assets.processed_flight_batch,
        assets.validated_flight_batch,
        assets.clickhouse_flight_batch,
        assets.published_flight_dataset,
    ),
    hooks={alert_on_failure, clear_alert_on_success},
)


MONITORED_PIPELINE_JOBS = [
    raw_flight_to_staged_job,
    staged_flight_to_published_job,
]


def _record_terminal_status(context, status: str) -> None:
    repo_root = Path(
        os.getenv("DVC_REPO_ROOT", Path(__file__).resolve().parents[1])
    ).resolve()
    identity = resolve_pipeline_identity(repo_root)
    record_terminal_job_run(context, identity, status)
    context.log.info(
        "PostgreSQL pipeline catalog updated: run=%s, status=%s",
        context.dagster_run.run_id,
        status,
    )


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=MONITORED_PIPELINE_JOBS,
    default_status=DefaultSensorStatus.RUNNING,
)
def postgres_run_success_sensor(context):
    _record_terminal_status(context, "SUCCESS")


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    monitored_jobs=MONITORED_PIPELINE_JOBS,
    default_status=DefaultSensorStatus.RUNNING,
)
def postgres_run_failure_sensor(context):
    _record_terminal_status(context, "FAILURE")


@run_status_sensor(
    run_status=DagsterRunStatus.CANCELED,
    monitored_jobs=MONITORED_PIPELINE_JOBS,
    default_status=DefaultSensorStatus.RUNNING,
)
def postgres_run_canceled_sensor(context):
    _record_terminal_status(context, "CANCELED")


# The staged filename no longer has to encode dataset id / row count
# (``flightdemo_<N>rows...``). staged_flight_tab now writes a
# ``<key>.tab.zst.meta.json`` sidecar with those values, and this sensor
# reads it -- so any ``*.tab.zst`` name is accepted.
STAGED_FLIGHT_SIDECAR_SUFFIX = ".meta.json"


def _job_has_active_run(context, job_name: str) -> bool:
    active_statuses = [
        DagsterRunStatus.QUEUED,
        DagsterRunStatus.NOT_STARTED,
        DagsterRunStatus.MANAGED,
        DagsterRunStatus.STARTING,
        DagsterRunStatus.STARTED,
        DagsterRunStatus.CANCELING,
    ]
    return bool(
        context.instance.get_runs(
            filters=RunsFilter(
                job_name=job_name,
                statuses=active_statuses,
            ),
            limit=1,
        )
    )


@sensor(
    job=raw_flight_to_staged_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Watches raw flight .tab objects in MinIO and launches one staging run "
        "for each new object key/ETag combination."
    ),
)
def raw_flight_minio_sensor(context):
    if _job_has_active_run(context, raw_flight_to_staged_job.name):
        return SkipReason("A raw-to-staged run is already active.")

    source_bucket = os.getenv("MINIO_RAW_BUCKET", assets.DEFAULT_RAW_BUCKET)
    source_prefix = os.getenv("MINIO_RAW_PREFIX", "flight-tab/inbox/").strip("/")
    if source_prefix:
        source_prefix = f"{source_prefix}/"

    client = assets.create_minio_client()
    if not client.bucket_exists(source_bucket):
        return SkipReason(f"MinIO bucket does not exist yet: {source_bucket}")

    try:
        observed_etags = json.loads(context.cursor) if context.cursor else {}
    except (TypeError, json.JSONDecodeError):
        observed_etags = {}
    if not isinstance(observed_etags, dict):
        observed_etags = {}

    candidates = sorted(
        (
            item
            for item in client.list_objects(
                source_bucket,
                prefix=source_prefix,
                recursive=True,
            )
            if not item.is_dir and item.object_name.lower().endswith(".tab")
        ),
        key=lambda item: item.object_name,
    )

    for item in candidates:
        source_etag = assets._normalise_etag(item.etag) or "unknown"
        object_identity = f"{source_bucket}/{item.object_name}"
        if observed_etags.get(object_identity) == source_etag:
            continue

        observed_etags[object_identity] = source_etag
        context.update_cursor(json.dumps(observed_etags, sort_keys=True))
        return RunRequest(
            run_key=f"raw-minio:{object_identity}:{source_etag}",
            run_config={
                "ops": {
                    "staged_flight_tab": {
                        "config": {
                            "source_bucket": source_bucket,
                            "source_key": item.object_name,
                            "source_etag": source_etag,
                        }
                    }
                }
            },
            tags={
                "source_bucket": source_bucket,
                "source_key": item.object_name,
                "source_etag": source_etag,
            },
        )

    return SkipReason(
        f"No new .tab objects under s3://{source_bucket}/{source_prefix}."
    )


@sensor(
    job=staged_flight_to_published_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Watches staged flight .tab.zst objects in MinIO and launches the "
        "Spark -> GE -> ClickHouse + DVC workflow for each object key/ETag pair."
    ),
)
def staged_flight_minio_sensor(context):
    if _job_has_active_run(context, staged_flight_to_published_job.name):
        return SkipReason("A staged-to-published run is already active.")

    source_bucket = os.getenv(
        "MINIO_STAGED_BUCKET", assets.DEFAULT_STAGED_BUCKET
    )
    source_prefix = os.getenv(
        "MINIO_STAGED_PREFIX", assets.DEFAULT_STAGED_PREFIX
    ).strip("/")
    if source_prefix:
        source_prefix = f"{source_prefix}/"

    client = assets.create_minio_client()
    if not client.bucket_exists(source_bucket):
        return SkipReason(f"MinIO bucket does not exist yet: {source_bucket}")

    try:
        observed_etags = json.loads(context.cursor) if context.cursor else {}
    except (TypeError, json.JSONDecodeError):
        observed_etags = {}
    if not isinstance(observed_etags, dict):
        observed_etags = {}

    candidates = sorted(
        (
            item
            for item in client.list_objects(
                source_bucket,
                prefix=source_prefix,
                recursive=True,
            )
            if not item.is_dir
            and item.object_name.lower().endswith(".tab.zst")
        ),
        key=lambda item: item.object_name,
    )

    cursor_changed = False
    for item in candidates:
        source_etag = assets._normalise_etag(item.etag) or "unknown"
        object_identity = f"{source_bucket}/{item.object_name}"
        if observed_etags.get(object_identity) == source_etag:
            continue

        # dataset id / batch id / row count / column count come from the
        # sidecar staged_flight_tab writes, not from the filename.
        sidecar_key = f"{item.object_name}{STAGED_FLIGHT_SIDECAR_SUFFIX}"
        sidecar = None
        try:
            sidecar_response = client.get_object(source_bucket, sidecar_key)
            try:
                sidecar = json.loads(sidecar_response.read())
            finally:
                sidecar_response.close()
                sidecar_response.release_conn()
        except Exception:  # noqa: BLE001 - missing/corrupt sidecar handled below
            sidecar = None

        if not isinstance(sidecar, dict) or "row_count" not in sidecar:
            context.log.warning(
                "Skipping staged object without a usable %s sidecar: s3://%s/%s "
                "(stage it through raw_flight_to_staged_job).",
                STAGED_FLIGHT_SIDECAR_SUFFIX,
                source_bucket,
                item.object_name,
            )
            observed_etags[object_identity] = source_etag
            cursor_changed = True
            continue

        file_name = PurePosixPath(item.object_name).name
        dataset_id = str(
            sidecar.get("dataset_id") or assets.DEFAULT_DATASET_ID
        ).lower()
        batch_id = str(sidecar.get("batch_id") or file_name[: -len(".tab.zst")])
        try:
            row_count = int(sidecar["row_count"])
            column_count = int(
                sidecar.get("column_count") or assets.FLIGHT_COLUMN_COUNT
            )
        except (TypeError, ValueError):
            context.log.warning(
                "Skipping staged object with a non-numeric sidecar count: "
                "s3://%s/%s",
                source_bucket,
                item.object_name,
            )
            observed_etags[object_identity] = source_etag
            cursor_changed = True
            continue

        observed_etags[object_identity] = source_etag
        context.update_cursor(json.dumps(observed_etags, sort_keys=True))
        return RunRequest(
            run_key=f"staged-minio:{object_identity}:{source_etag}",
            run_config={
                "ops": {
                    "processed_flight_batch": {
                        "config": {
                            "source_bucket": source_bucket,
                            "source_key": item.object_name,
                            "source_etag": source_etag,
                            "dataset_id": dataset_id,
                            "batch_id": batch_id,
                            "row_count": row_count,
                            "column_count": column_count,
                        }
                    }
                }
            },
            tags={
                "dataset_id": dataset_id,
                "batch_id": batch_id,
                "source_bucket": source_bucket,
                "source_key": item.object_name,
                "source_etag": source_etag,
            },
        )

    if cursor_changed:
        context.update_cursor(json.dumps(observed_etags, sort_keys=True))
    return SkipReason(
        f"No new supported .tab.zst objects under "
        f"s3://{source_bucket}/{source_prefix}."
    )


defs = Definitions(
    assets=[
        assets.staged_flight_tab,
        assets.processed_flight_batch,
        assets.validated_flight_batch,
        assets.clickhouse_flight_batch,
        assets.published_flight_dataset,
    ],
    jobs=[raw_flight_to_staged_job, staged_flight_to_published_job],
    sensors=[
        raw_flight_minio_sensor,
        staged_flight_minio_sensor,
        postgres_run_success_sensor,
        postgres_run_failure_sensor,
        postgres_run_canceled_sensor,
    ],
)
