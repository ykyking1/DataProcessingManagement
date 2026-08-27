"""PostgreSQL catalog access for MX Dagster runs and materializations."""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator, Mapping

import psycopg2
import psycopg2.extras


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA_PATH = PROJECT_ROOT / "docs" / "postgres_pipeline_catalog_schema.sql"


@dataclass(frozen=True)
class PipelineIdentity:
    """Immutable code and container identity captured for one pipeline run."""

    version: str
    git_tag: str | None
    git_sha: str
    git_dirty: bool
    container_image: str | None
    container_image_digest: str | None


@dataclass(frozen=True)
class CatalogRun:
    """Catalog row selected for a Dagster run."""

    id: int
    identity: PipelineIdentity


def _environment_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def resolve_pipeline_identity(repo_root: Path | str) -> PipelineIdentity:
    """Resolve the code identity once and persist it with the first run row."""

    configured_sha = os.getenv("PIPELINE_GIT_SHA", "").strip()
    configured_version = os.getenv("PIPELINE_VERSION", "").strip()
    configured_tag = os.getenv("PIPELINE_GIT_TAG", "").strip() or None
    container_image = os.getenv("PIPELINE_CONTAINER_IMAGE", "").strip() or None
    container_digest = (
        os.getenv("PIPELINE_CONTAINER_IMAGE_DIGEST", "").strip() or None
    )

    if configured_sha:
        return PipelineIdentity(
            version=configured_version or f"unreleased-{configured_sha[:7]}",
            git_tag=configured_tag,
            git_sha=configured_sha,
            git_dirty=_environment_flag("PIPELINE_GIT_DIRTY"),
            container_image=container_image,
            container_image_digest=container_digest,
        )

    try:
        from git import Repo

        repo = Repo(Path(repo_root).resolve())
        commit = repo.head.commit
        git_sha = commit.hexsha
        matching_tags = sorted(
            tag.name
            for tag in repo.tags
            if tag.commit == commit and tag.name.startswith("pipeline-v")
        )
        git_tag = configured_tag or (matching_tags[-1] if matching_tags else None)
        git_dirty = repo.is_dirty(untracked_files=True)
        version = configured_version or git_tag or f"unreleased-{git_sha[:7]}"
        if not configured_version and git_dirty:
            version = f"{version}-dirty"
        return PipelineIdentity(
            version=version,
            git_tag=git_tag,
            git_sha=git_sha,
            git_dirty=git_dirty,
            container_image=container_image,
            container_image_digest=container_digest,
        )
    except Exception:
        return PipelineIdentity(
            version=configured_version or "unreleased-unknown",
            git_tag=configured_tag,
            git_sha="unknown",
            git_dirty=_environment_flag("PIPELINE_GIT_DIRTY"),
            container_image=container_image,
            container_image_digest=container_digest,
        )


def _connection_parameters() -> dict[str, Any]:
    return {
        "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DATABASE", "postgres"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
        "connect_timeout": int(os.getenv("POSTGRES_CONNECT_TIMEOUT", "5")),
        "application_name": "dpm-dagster-pipeline-catalog",
    }


@lru_cache(maxsize=1)
def _schema_sql() -> str:
    configured_path = os.getenv("POSTGRES_CATALOG_SCHEMA_PATH", "").strip()
    schema_path = Path(configured_path) if configured_path else DEFAULT_SCHEMA_PATH
    if not schema_path.is_file():
        raise FileNotFoundError(f"PostgreSQL catalog schema not found: {schema_path}")
    return schema_path.read_text(encoding="utf-8")


@contextmanager
def _catalog_connection() -> Iterator[Any]:
    connection = psycopg2.connect(**_connection_parameters())
    try:
        with connection.cursor() as cursor:
            cursor.execute(_schema_sql())
        connection.commit()
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _run_started_at(instance: Any, run_id: str) -> datetime:
    record = instance.get_run_record_by_id(run_id)
    if record is not None and record.start_time is not None:
        return datetime.fromtimestamp(record.start_time, tz=timezone.utc)
    if record is not None:
        return record.create_timestamp
    return datetime.now(timezone.utc)


def _run_finished_at(instance: Any, run_id: str) -> datetime:
    record = instance.get_run_record_by_id(run_id)
    if record is not None and record.end_time is not None:
        return datetime.fromtimestamp(record.end_time, tz=timezone.utc)
    return datetime.now(timezone.utc)


def _stored_catalog_run(row: tuple[Any, ...]) -> CatalogRun:
    return CatalogRun(
        id=int(row[0]),
        identity=PipelineIdentity(
            version=row[1],
            git_tag=row[2],
            git_sha=row[3],
            git_dirty=bool(row[4]),
            container_image=row[5],
            container_image_digest=row[6],
        ),
    )


def ensure_job_run(
    context: Any,
    identity: PipelineIdentity,
    *,
    dataset_id: str | None = None,
    batch_id: str | None = None,
) -> CatalogRun:
    """Create a STARTED row once and return its persisted code identity."""

    dagster_run = context.instance.get_run_by_id(context.run_id)
    if dagster_run is None:
        raise RuntimeError(f"Dagster run not found: {context.run_id}")

    tags = dict(dagster_run.tags)
    resolved_dataset_id = dataset_id or tags.get("dataset_id")
    resolved_batch_id = batch_id or tags.get("batch_id")

    with _catalog_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline_catalog.pipeline_job_runs AS existing (
                    dagster_run_id,
                    parent_run_id,
                    job_name,
                    run_status,
                    dataset_id,
                    batch_id,
                    source_bucket,
                    source_object_key,
                    source_etag,
                    pipeline_version,
                    pipeline_git_tag,
                    pipeline_git_sha,
                    pipeline_git_dirty,
                    container_image,
                    container_image_digest,
                    run_tags,
                    started_at
                )
                VALUES (
                    %s, %s, %s, 'STARTED', %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (dagster_run_id) DO UPDATE SET
                    parent_run_id = COALESCE(
                        existing.parent_run_id,
                        EXCLUDED.parent_run_id
                    ),
                    job_name = EXCLUDED.job_name,
                    dataset_id = COALESCE(
                        existing.dataset_id,
                        EXCLUDED.dataset_id
                    ),
                    batch_id = COALESCE(
                        existing.batch_id,
                        EXCLUDED.batch_id
                    ),
                    source_bucket = COALESCE(
                        existing.source_bucket,
                        EXCLUDED.source_bucket
                    ),
                    source_object_key = COALESCE(
                        existing.source_object_key,
                        EXCLUDED.source_object_key
                    ),
                    source_etag = COALESCE(
                        existing.source_etag,
                        EXCLUDED.source_etag
                    ),
                    run_tags = EXCLUDED.run_tags,
                    updated_at = now()
                RETURNING
                    id,
                    pipeline_version,
                    pipeline_git_tag,
                    pipeline_git_sha,
                    pipeline_git_dirty,
                    container_image,
                    container_image_digest
                """,
                (
                    dagster_run.run_id,
                    dagster_run.parent_run_id,
                    dagster_run.job_name,
                    resolved_dataset_id,
                    resolved_batch_id,
                    tags.get("source_bucket"),
                    tags.get("source_key"),
                    tags.get("source_etag"),
                    identity.version,
                    identity.git_tag,
                    identity.git_sha,
                    identity.git_dirty,
                    identity.container_image,
                    identity.container_image_digest,
                    psycopg2.extras.Json(tags),
                    _run_started_at(context.instance, dagster_run.run_id),
                ),
            )
            row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"PostgreSQL run row was not returned: {context.run_id}")
    return _stored_catalog_run(row)


def record_asset_materialization(
    context: Any,
    catalog_run: CatalogRun,
    *,
    asset_key: str,
    asset_group: str | None,
    dataset_id: str | None,
    batch_id: str | None,
    input_uri: str | None = None,
    input_etag: str | None = None,
    output_uri: str | None = None,
    output_etag: str | None = None,
    row_count: int | None = None,
    column_count: int | None = None,
    part_count: int | None = None,
    output_size_bytes: int | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> None:
    """Insert or idempotently refresh one successful asset materialization."""

    metadata_value = dict(metadata or {})
    with _catalog_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE pipeline_catalog.pipeline_job_runs
                SET
                    dataset_id = COALESCE(dataset_id, %s),
                    batch_id = COALESCE(batch_id, %s),
                    updated_at = now()
                WHERE id = %s
                """,
                (dataset_id, batch_id, catalog_run.id),
            )
            cursor.execute(
                """
                INSERT INTO pipeline_catalog.pipeline_asset_materializations (
                    job_run_id,
                    asset_key,
                    asset_group,
                    dataset_id,
                    batch_id,
                    input_uri,
                    input_etag,
                    output_uri,
                    output_etag,
                    row_count,
                    column_count,
                    part_count,
                    output_size_bytes,
                    metadata
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (job_run_id, asset_key) DO UPDATE SET
                    asset_group = EXCLUDED.asset_group,
                    dataset_id = EXCLUDED.dataset_id,
                    batch_id = EXCLUDED.batch_id,
                    input_uri = EXCLUDED.input_uri,
                    input_etag = EXCLUDED.input_etag,
                    output_uri = EXCLUDED.output_uri,
                    output_etag = EXCLUDED.output_etag,
                    row_count = EXCLUDED.row_count,
                    column_count = EXCLUDED.column_count,
                    part_count = EXCLUDED.part_count,
                    output_size_bytes = EXCLUDED.output_size_bytes,
                    metadata = EXCLUDED.metadata,
                    materialized_at = now()
                """,
                (
                    catalog_run.id,
                    asset_key,
                    asset_group,
                    dataset_id,
                    batch_id,
                    input_uri,
                    input_etag,
                    output_uri,
                    output_etag,
                    row_count,
                    column_count,
                    part_count,
                    output_size_bytes,
                    psycopg2.extras.Json(metadata_value),
                ),
            )


def mark_job_run_failure(
    run_id: str,
    *,
    failed_step: str,
    error: BaseException | None,
) -> None:
    """Persist the failed step immediately; the terminal sensor adds end time."""

    error_type = type(error).__name__ if error is not None else "UnknownError"
    error_message = str(error or "Unknown pipeline error")
    with _catalog_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE pipeline_catalog.pipeline_job_runs
                SET
                    run_status = 'FAILURE',
                    failed_step = %s,
                    error_type = %s,
                    error_message = %s,
                    updated_at = now()
                WHERE dagster_run_id = %s
                """,
                (failed_step, error_type, error_message, run_id),
            )


def record_terminal_job_run(
    context: Any,
    identity: PipelineIdentity,
    status: str,
) -> None:
    """Upsert a run and persist its authoritative terminal Dagster status."""

    dagster_run = context.dagster_run
    tags = dict(dagster_run.tags)
    finished_at = _run_finished_at(context.instance, dagster_run.run_id)
    started_at = _run_started_at(context.instance, dagster_run.run_id)
    error_message = (
        context.dagster_event.message if status == "FAILURE" else None
    )

    with _catalog_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline_catalog.pipeline_job_runs AS existing (
                    dagster_run_id,
                    parent_run_id,
                    job_name,
                    run_status,
                    dataset_id,
                    batch_id,
                    source_bucket,
                    source_object_key,
                    source_etag,
                    pipeline_version,
                    pipeline_git_tag,
                    pipeline_git_sha,
                    pipeline_git_dirty,
                    container_image,
                    container_image_digest,
                    run_tags,
                    error_message,
                    started_at,
                    finished_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (dagster_run_id) DO UPDATE SET
                    run_status = EXCLUDED.run_status,
                    error_message = COALESCE(
                        existing.error_message,
                        EXCLUDED.error_message
                    ),
                    finished_at = EXCLUDED.finished_at,
                    updated_at = now()
                """,
                (
                    dagster_run.run_id,
                    dagster_run.parent_run_id,
                    dagster_run.job_name,
                    status,
                    tags.get("dataset_id"),
                    tags.get("batch_id"),
                    tags.get("source_bucket"),
                    tags.get("source_key"),
                    tags.get("source_etag"),
                    identity.version,
                    identity.git_tag,
                    identity.git_sha,
                    identity.git_dirty,
                    identity.container_image,
                    identity.container_image_digest,
                    psycopg2.extras.Json(tags),
                    error_message,
                    started_at,
                    finished_at,
                ),
            )
