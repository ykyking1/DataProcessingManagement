"""Build a suggested DVC commit message from the PostgreSQL run catalog.

This module is deliberately independent from Dagster and Git. It only reads
the immutable run/materialization metadata already stored in PostgreSQL and
never creates a commit.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Mapping

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / "dagster" / ".env")


class CommitMessageError(RuntimeError):
    """Raised when a run cannot safely produce a commit suggestion."""


def _connection_parameters() -> dict[str, Any]:
    return {
        "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DATABASE", "postgres"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
        "connect_timeout": int(os.getenv("POSTGRES_CONNECT_TIMEOUT", "5")),
        "application_name": "dpm-commit-message-generator",
    }


def _fetch_run_and_assets(
    run_id: str,
) -> tuple[Mapping[str, Any], dict[str, Mapping[str, Any]]]:
    connection = psycopg2.connect(**_connection_parameters())
    try:
        connection.set_session(readonly=True, autocommit=True)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    dagster_run_id,
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
                    started_at,
                    finished_at
                FROM pipeline_catalog.pipeline_job_runs
                WHERE dagster_run_id = %s
                """,
                (run_id,),
            )
            run = cursor.fetchone()
            if run is None:
                raise CommitMessageError(
                    f"PostgreSQL catalogunda run bulunamadı: {run_id}"
                )

            cursor.execute(
                """
                SELECT
                    asset_key,
                    dataset_id,
                    batch_id,
                    output_uri,
                    output_etag,
                    row_count,
                    column_count,
                    part_count,
                    output_size_bytes,
                    metadata,
                    materialized_at
                FROM pipeline_catalog.pipeline_asset_materializations
                WHERE job_run_id = %s
                ORDER BY materialized_at, id
                """,
                (run["id"],),
            )
            assets = {row["asset_key"]: row for row in cursor.fetchall()}
            return run, assets
    finally:
        connection.close()


def _append_trailer(lines: list[str], name: str, value: Any) -> None:
    if value is not None and str(value).strip():
        lines.append(f"{name}: {value}")


def _format_percent(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        return str(value)


def _build_commit_message(
    run: Mapping[str, Any],
    assets: Mapping[str, Mapping[str, Any]],
) -> str:
    if run["run_status"] != "SUCCESS":
        raise CommitMessageError(
            "Commit mesajı yalnızca SUCCESS durumundaki run için üretilebilir: "
            f"{run['dagster_run_id']} ({run['run_status']})"
        )

    published = assets.get("published_mx_dataset")
    if published is None:
        raise CommitMessageError(
            "Run başarılı olsa da published_mx_dataset kaydı bulunamadı; "
            "commit önerisi üretmek güvenli değil."
        )

    dataset_id = published.get("dataset_id") or run.get("dataset_id")
    batch_id = published.get("batch_id") or run.get("batch_id")
    if not dataset_id or not batch_id:
        raise CommitMessageError("Run kaydında dataset_id veya batch_id eksik.")

    published_metadata = published.get("metadata") or {}
    validation = assets.get("validated_mx_batch") or {}
    validation_metadata = validation.get("metadata") or {}

    dvc_hash_name = published_metadata.get("dvc_hash_name")
    dvc_hash = published.get("output_etag") or published_metadata.get("dvc_hash")
    dvc_hash_value = (
        f"{dvc_hash_name}:{dvc_hash}" if dvc_hash_name and dvc_hash else dvc_hash
    )

    subject = f"data({dataset_id}): publish {batch_id}"
    trailers: list[str] = []
    _append_trailer(trailers, "Dagster-Run", run["dagster_run_id"])
    _append_trailer(trailers, "Pipeline-Version", run["pipeline_version"])
    _append_trailer(trailers, "Pipeline-Git-Tag", run["pipeline_git_tag"])
    _append_trailer(trailers, "Pipeline-Git-SHA", run["pipeline_git_sha"])
    _append_trailer(
        trailers,
        "Pipeline-Git-Dirty",
        str(bool(run["pipeline_git_dirty"])).lower(),
    )
    _append_trailer(trailers, "DVC-Pointer", published.get("output_uri"))
    _append_trailer(trailers, "DVC-Hash", dvc_hash_value)
    _append_trailer(trailers, "Rows", published.get("row_count"))
    _append_trailer(trailers, "Columns", published.get("column_count"))
    _append_trailer(trailers, "Parts", published.get("part_count"))
    _append_trailer(
        trailers,
        "Validation-Success",
        _format_percent(validation_metadata.get("success_percent")),
    )

    return "\n".join([subject, "", *trailers])


def get_commit_message(run_id: str) -> str:
    """Return a suggested commit message for one successful Dagster run."""

    normalized_run_id = run_id.strip()
    if not normalized_run_id:
        raise ValueError("run_id boş olamaz.")
    run, assets = _fetch_run_and_assets(normalized_run_id)
    return _build_commit_message(run, assets)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-id",
        required=True,
        help="Commit önerisi üretilecek tam Dagster run ID.",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Başlık olmadan yalnızca commit mesajını yazdır.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        message = get_commit_message(args.run_id)
    except (CommitMessageError, ValueError, psycopg2.Error) as error:
        print(f"Commit mesajı üretilemedi: {error}", file=sys.stderr)
        return 1

    if not args.raw:
        print("Suggested commit message:\n")
    print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
