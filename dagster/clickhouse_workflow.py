"""Commit or roll back ClickHouse rows produced by one Dagster workflow run.

The wide AU-AIR storage table receives rows before the DVC publication step.
Those rows carry the Dagster run id and remain hidden behind a committed view.
The terminal run-status sensors call this module to either publish the run in
the small commit registry or delete its rows after failure/cancellation.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from clickhouse_driver import Client


SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
WORKFLOW_RUN_ID_COLUMN = "dagster_run_id"


@dataclass(frozen=True)
class ClickHouseWorkflowResult:
    action: str
    batch_id: str
    dagster_run_id: str
    row_count: int
    skipped: bool = False


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _quote_identifier(value: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe ClickHouse identifier: {value!r}")
    return f"`{value}`"


def _configured_names() -> tuple[str, str, str, str]:
    database = os.getenv("CLICKHOUSE_DATABASE", "default")
    table = os.getenv("CLICKHOUSE_AUAIR_TABLE", "auair_telemetry")
    visible_view = os.getenv(
        "CLICKHOUSE_AUAIR_VISIBLE_VIEW",
        "auair_telemetry_committed",
    )
    commit_table = os.getenv(
        "CLICKHOUSE_AUAIR_COMMIT_TABLE",
        "auair_telemetry_workflow_commits",
    )
    for value in (database, table, visible_view, commit_table):
        _quote_identifier(value)
    if len({table, visible_view, commit_table}) != 3:
        raise ValueError(
            "ClickHouse storage table, committed view and commit table "
            "must use distinct names."
        )
    return database, table, visible_view, commit_table


def _client() -> Client:
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000")),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123"),
        database=os.getenv("CLICKHOUSE_DATABASE", "default"),
        secure=_env_bool("CLICKHOUSE_SECURE"),
        send_receive_timeout=3_600,
    )


def reconcile_auair_clickhouse_run(
    *,
    batch_id: str,
    dagster_run_id: str,
    commit: bool,
) -> ClickHouseWorkflowResult:
    """Publish a successful run or remove one failed/canceled run.

    The operation is idempotent. A missing storage table or workflow column is
    a valid no-op for runs that ended before the ClickHouse asset began.
    """

    if not batch_id.strip():
        raise ValueError("batch_id cannot be empty")
    if not dagster_run_id.strip():
        raise ValueError("dagster_run_id cannot be empty")

    database, table, visible_view, commit_table = _configured_names()
    database_name = _quote_identifier(database)
    table_name = f"{database_name}.{_quote_identifier(table)}"
    visible_view_name = f"{database_name}.{_quote_identifier(visible_view)}"
    commit_table_name = f"{database_name}.{_quote_identifier(commit_table)}"
    parameters = {
        "database": database,
        "table": table,
        "batch_id": batch_id,
        "dagster_run_id": dagster_run_id,
    }
    client = _client()
    try:
        if not client.execute(f"EXISTS TABLE {table_name}")[0][0]:
            return ClickHouseWorkflowResult(
                action="commit" if commit else "rollback",
                batch_id=batch_id,
                dagster_run_id=dagster_run_id,
                row_count=0,
                skipped=True,
            )

        workflow_columns = {
            row[0]
            for row in client.execute(
                "SELECT name FROM system.columns "
                "WHERE database = %(database)s AND table = %(table)s "
                "AND name IN ('source_batch_id', 'dagster_run_id')",
                parameters,
            )
        }
        if workflow_columns != {"source_batch_id", WORKFLOW_RUN_ID_COLUMN}:
            return ClickHouseWorkflowResult(
                action="commit" if commit else "rollback",
                batch_id=batch_id,
                dagster_run_id=dagster_run_id,
                row_count=0,
                skipped=True,
            )

        row_count = int(
            client.execute(
                f"SELECT count() FROM {table_name} "
                "WHERE source_batch_id = %(batch_id)s "
                "AND dagster_run_id = %(dagster_run_id)s",
                parameters,
            )[0][0]
        )

        if commit:
            if row_count <= 0:
                raise RuntimeError(
                    "Successful AU-AIR workflow has no ClickHouse rows for "
                    f"batch={batch_id}, run={dagster_run_id}."
                )
            if not client.execute(f"EXISTS TABLE {commit_table_name}")[0][0]:
                raise RuntimeError(
                    f"ClickHouse workflow commit table is missing: {commit_table_name}"
                )
            if not client.execute(f"EXISTS TABLE {visible_view_name}")[0][0]:
                raise RuntimeError(
                    f"ClickHouse committed view is missing: {visible_view_name}"
                )

            already_committed = int(
                client.execute(
                    f"SELECT count() FROM {commit_table_name} "
                    "WHERE source_batch_id = %(batch_id)s "
                    "AND dagster_run_id = %(dagster_run_id)s",
                    parameters,
                )[0][0]
            )
            if not already_committed:
                client.execute(
                    f"INSERT INTO {commit_table_name} "
                    "(source_batch_id, dagster_run_id) VALUES",
                    [(batch_id, dagster_run_id)],
                )

            # The registry switch makes the new run visible immediately. Old
            # physical versions are then reclaimed asynchronously and cannot
            # appear through the committed view.
            client.execute(
                f"ALTER TABLE {table_name} DELETE "
                "WHERE source_batch_id = %(batch_id)s "
                "AND dagster_run_id != %(dagster_run_id)s",
                parameters,
                settings={"mutations_sync": 0},
            )
            return ClickHouseWorkflowResult(
                action="commit",
                batch_id=batch_id,
                dagster_run_id=dagster_run_id,
                row_count=row_count,
            )

        # Remove a registry row first, if one was ever written, so this run is
        # hidden before its wide-table rows are reclaimed.
        if client.execute(f"EXISTS TABLE {commit_table_name}")[0][0]:
            client.execute(
                f"ALTER TABLE {commit_table_name} DELETE "
                "WHERE source_batch_id = %(batch_id)s "
                "AND dagster_run_id = %(dagster_run_id)s",
                parameters,
                settings={"mutations_sync": 1},
            )
        if row_count:
            client.execute(
                f"ALTER TABLE {table_name} DELETE "
                "WHERE source_batch_id = %(batch_id)s "
                "AND dagster_run_id = %(dagster_run_id)s",
                parameters,
                settings={"mutations_sync": 1},
            )

        remaining = int(
            client.execute(
                f"SELECT count() FROM {table_name} "
                "WHERE source_batch_id = %(batch_id)s "
                "AND dagster_run_id = %(dagster_run_id)s",
                parameters,
            )[0][0]
        )
        if remaining:
            raise RuntimeError(
                "ClickHouse rollback verification failed: "
                f"batch={batch_id}, run={dagster_run_id}, rows={remaining}."
            )
        return ClickHouseWorkflowResult(
            action="rollback",
            batch_id=batch_id,
            dagster_run_id=dagster_run_id,
            row_count=row_count,
        )
    finally:
        client.disconnect()
