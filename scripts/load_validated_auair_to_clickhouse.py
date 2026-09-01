"""Bulk-load one validated 10K-50K-column AU-AIR batch into ClickHouse.

The Dagster asset contract stays unchanged: validation completes first, this
loader writes the validated batch to ClickHouse, and only then may the DVC
publication asset run. The write follows the high-column strategy proven on
``working_pipeline_yusuf``: row-axis physical splitting, temporary MinIO
transport objects, ClickHouse ``s3()`` bulk inserts, compact MergeTree parts,
merge-pressure throttling, and an OOM query watchdog.

Temporary ingest objects are deleted after the ClickHouse attempt. They are
not the durable DVC publication.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import signal
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import BinaryIO, Sequence
from urllib.parse import quote

import zstandard as zstd
from clickhouse_driver import Client
from minio import Minio
from minio.error import S3Error


SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
STRING_BASE_COLUMNS = {"image_name", "platform"}
IMAGE_DIMENSION_BASE_COLUMNS = {"image_width", "image_height"}
COUNT_BASE_COLUMNS = {
    "num_objects",
    "obj_human",
    "obj_car",
    "obj_truck",
    "obj_van",
    "obj_motorbike",
    "obj_bicycle",
    "obj_bus",
    "obj_trailer",
}
DOUBLE_BASE_COLUMNS = {
    "longtitude",
    "latitude",
    "altitude",
    "linear_x",
    "linear_y",
    "linear_z",
    "angle_phi",
    "angle_theta",
    "angle_psi",
}

# Yusuf's wide-table experiments found about one billion source cells per
# physical object/query to be the safe boundary on the 12 GB WSL budget.
DEFAULT_WIDE_SAFE_CELL_LIMIT = 1_000_000_000
DEFAULT_MAX_ROWS_PER_CHUNK = 200_000
DEFAULT_ROW_CHUNK_ZSTD_LEVEL = 6
DEFAULT_SCHEMA_ALTER_CHUNK_COLUMNS = 1_000
WORKFLOW_RUN_ID_COLUMN = "dagster_run_id"

# Parallel parsing of 10K-50K-column TSV data multiplies peak memory. These are
# the high-column ClickHouse settings used on Yusuf's branch.
CLICKHOUSE_SETTINGS = {
    "max_execution_time": 0,
    "max_query_size": 300_000_000,
    "max_ast_elements": 10_000_000,
    "max_expanded_ast_elements": 10_000_000,
    "input_format_parallel_parsing": 0,
    "max_threads": 2,
    "max_insert_threads": 1,
    "max_block_size": 2_048,
    "max_insert_block_size": 2_048,
    "date_time_input_format": "best_effort",
}

# Force compact parts so ClickHouse does not open a separate part stream for
# each of tens of thousands of columns.
WIDE_PART_SETTINGS = {
    "min_bytes_for_wide_part": 10_737_418_240_000,
    "min_rows_for_wide_part": 1_000_000_000,
}

MEMORY_SAFE_FLOOR_GB = float(os.getenv("CLICKHOUSE_MEMORY_SAFE_FLOOR_GB", "2.5"))
MEMORY_WATCHDOG_CRITICAL_GB = float(
    os.getenv("CLICKHOUSE_MEMORY_WATCHDOG_CRITICAL_GB", "2.0")
)
MEMORY_WATCHDOG_POLL_SECONDS = float(
    os.getenv("CLICKHOUSE_MEMORY_WATCHDOG_POLL_SECONDS", "1")
)
MAX_ACTIVE_PARTS_BEFORE_CHUNK = int(
    os.getenv("CLICKHOUSE_MAX_ACTIVE_PARTS", "30")
)
PARTS_SETTLE_POLL_SECONDS = float(
    os.getenv("CLICKHOUSE_PARTS_SETTLE_POLL_SECONDS", "5")
)
PARTS_SETTLE_MAX_WAIT_SECONDS = float(
    os.getenv("CLICKHOUSE_PARTS_SETTLE_MAX_WAIT_SECONDS", "300")
)
CHECKPOINT_INTERVAL_CHUNKS = int(
    os.getenv("CLICKHOUSE_CHECKPOINT_INTERVAL_CHUNKS", "2")
)
CHECKPOINT_TARGET_GB = float(os.getenv("CLICKHOUSE_CHECKPOINT_TARGET_GB", "1.2"))
CHECKPOINT_POLL_SECONDS = float(
    os.getenv("CLICKHOUSE_CHECKPOINT_POLL_SECONDS", "5")
)
CHECKPOINT_MAX_WAIT_SECONDS = float(
    os.getenv("CLICKHOUSE_CHECKPOINT_MAX_WAIT_SECONDS", "180")
)


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _quote_identifier(value: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe ClickHouse identifier: {value!r}")
    return f"`{value}`"


def _quote_sql_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _safe_object_segment(value: str) -> str:
    segment = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._-")
    return segment or "batch"


def _base_column_name(column_name: str) -> str:
    return re.sub(r"\d+$", "", column_name)


def _source_type(column_name: str) -> str:
    """Return the type ClickHouse's s3() parser expects from the TSV."""

    if column_name == "flight_id":
        return "String"
    if column_name == "time":
        return "DateTime64(3, 'UTC')"

    base_name = _base_column_name(column_name)
    if base_name in STRING_BASE_COLUMNS:
        return "String"
    if base_name in IMAGE_DIMENSION_BASE_COLUMNS:
        return "UInt16"
    if base_name in COUNT_BASE_COLUMNS:
        return "UInt8"
    if base_name in DOUBLE_BASE_COLUMNS:
        return "Float64"
    raise ValueError(f"Unknown generated AU-AIR column: {column_name!r}")


def _table_type(column_name: str) -> str:
    if column_name == "flight_id" or _base_column_name(column_name) == "platform":
        return "LowCardinality(String)"
    return _source_type(column_name)


def _column_definition(column_name: str) -> str:
    column_type = _table_type(column_name)
    base_name = _base_column_name(column_name)
    if column_name == "time":
        codec = "CODEC(DoubleDelta, ZSTD(3))"
    elif base_name in IMAGE_DIMENSION_BASE_COLUMNS or base_name in COUNT_BASE_COLUMNS:
        codec = "CODEC(T64, ZSTD(3))"
    else:
        codec = "CODEC(ZSTD(3))"
    return f"{_quote_identifier(column_name)} {column_type} {codec}"


def _source_structure(source_columns: Sequence[str]) -> str:
    return ", ".join(
        f"{_quote_identifier(column_name)} {_source_type(column_name)}"
        for column_name in source_columns
    )


def _clickhouse_client() -> Client:
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000")),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123"),
        database=os.getenv("CLICKHOUSE_DATABASE", "default"),
        secure=_env_bool("CLICKHOUSE_SECURE"),
        send_receive_timeout=3_600,
        settings=CLICKHOUSE_SETTINGS,
    )


def _minio_endpoint() -> str:
    endpoint = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000").strip().rstrip("/")
    return re.sub(r"^https?://", "", endpoint)


def _minio_client() -> Minio:
    return Minio(
        _minio_endpoint(),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        secure=_env_bool("MINIO_SECURE"),
    )


def _ensure_bucket(client: Minio, bucket: str) -> None:
    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
    except S3Error as error:
        raise RuntimeError(f"MinIO ingest bucket could not be prepared: {error}") from error


def _part_files(input_path: Path) -> list[Path]:
    resolved = input_path.resolve()
    if resolved.is_file() and resolved.name.endswith((".tab.zst", ".tab.zstd")):
        return [resolved]
    if not resolved.is_dir():
        raise FileNotFoundError(f"Processed AU-AIR input not found: {resolved}")
    parts = sorted(
        path
        for path in resolved.iterdir()
        if path.is_file() and path.name.endswith((".tab.zst", ".tab.zstd"))
    )
    if not parts:
        raise FileNotFoundError(f"No processed AU-AIR ZSTD parts found: {resolved}")
    return parts


def _parse_header_line(header_line: bytes, part_file: Path) -> list[str]:
    if not header_line:
        raise ValueError(f"Empty AU-AIR part: {part_file}")
    try:
        header = header_line.decode("utf-8-sig").rstrip("\r\n").split("\t")
    except UnicodeDecodeError as error:
        raise ValueError(f"Invalid UTF-8 AU-AIR header: {part_file}") from error
    if header[:2] != ["flight_id", "time"]:
        raise ValueError(
            "Processed AU-AIR input must start with flight_id and time; "
            f"received {header[:2]} from {part_file}."
        )
    if len(header) < 17:
        raise ValueError(
            f"Processed AU-AIR input must contain at least 17 columns: {part_file}"
        )
    if len(header) != len(set(header)):
        raise ValueError(f"Duplicate AU-AIR columns in {part_file}")
    for column_name in header:
        _source_type(column_name)
    return header


def _read_header(part_file: Path) -> list[str]:
    with part_file.open("rb") as raw_file:
        with zstd.ZstdDecompressor().stream_reader(raw_file) as decompressed:
            return _parse_header_line(io.BufferedReader(decompressed).readline(), part_file)


def _type_is_compatible(expected: str, actual: str) -> bool:
    """Accept the old loader's wider nullable types during migration."""

    if actual == expected:
        return True
    compatibility = {
        "String": {"Nullable(String)"},
        "LowCardinality(String)": {"String", "Nullable(String)"},
        "UInt8": {"UInt16", "UInt32", "UInt64", "Int64", "Nullable(Int64)"},
        "UInt16": {"UInt32", "UInt64", "Int64", "Nullable(Int64)"},
        "Float64": {"Nullable(Float64)"},
    }
    return actual in compatibility.get(expected, set())


def _ensure_table_schema(
    client: Client,
    *,
    database: str,
    table: str,
    visible_view: str,
    commit_table: str,
    source_columns: Sequence[str],
    alter_chunk_columns: int,
) -> str:
    database_name = _quote_identifier(database)
    table_name = f"{database_name}.{_quote_identifier(table)}"
    visible_view_name = f"{database_name}.{_quote_identifier(visible_view)}"
    commit_table_name = f"{database_name}.{_quote_identifier(commit_table)}"
    if visible_view == table:
        raise ValueError("ClickHouse visible view and storage table must differ.")
    if commit_table in {table, visible_view}:
        raise ValueError("ClickHouse workflow commit table must have a unique name.")
    client.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    exists = bool(client.execute(f"EXISTS TABLE {table_name}")[0][0])
    if not exists:
        source_definitions = ",\n                ".join(
            _column_definition(column_name) for column_name in source_columns
        )
        client.execute(
            f"""
            CREATE TABLE {table_name}
            (
                {source_definitions},
                source_batch_id LowCardinality(String) CODEC(ZSTD(3)),
                dagster_run_id LowCardinality(String) DEFAULT '' CODEC(ZSTD(3)),
                ingested_at DateTime64(3, 'UTC')
                    DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(3))
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(time)
            ORDER BY (source_batch_id, flight_id, time)
            SETTINGS
                min_bytes_for_wide_part = {WIDE_PART_SETTINGS['min_bytes_for_wide_part']},
                min_rows_for_wide_part = {WIDE_PART_SETTINGS['min_rows_for_wide_part']}
            """
        )

    described = {row[0]: row[1] for row in client.execute(f"DESCRIBE TABLE {table_name}")}
    required_definitions = {
        **{column: _column_definition(column) for column in source_columns},
        "source_batch_id": "source_batch_id LowCardinality(String) CODEC(ZSTD(3))",
        WORKFLOW_RUN_ID_COLUMN: (
            "dagster_run_id LowCardinality(String) DEFAULT '' CODEC(ZSTD(3))"
        ),
        "ingested_at": (
            "ingested_at DateTime64(3, 'UTC') DEFAULT now64(3) "
            "CODEC(DoubleDelta, ZSTD(3))"
        ),
    }
    missing_columns = [name for name in required_definitions if name not in described]
    for offset in range(0, len(missing_columns), alter_chunk_columns):
        column_chunk = missing_columns[offset : offset + alter_chunk_columns]
        additions = ", ".join(
            f"ADD COLUMN IF NOT EXISTS {required_definitions[column]}"
            for column in column_chunk
        )
        client.execute(f"ALTER TABLE {table_name} {additions}")

    if missing_columns:
        described = {
            row[0]: row[1] for row in client.execute(f"DESCRIBE TABLE {table_name}")
        }
    for column_name in source_columns:
        expected_type = _table_type(column_name)
        actual_type = described.get(column_name)
        if actual_type is None or not _type_is_compatible(expected_type, actual_type):
            raise ValueError(
                f"ClickHouse type mismatch for {column_name}: "
                f"expected {expected_type}, found {actual_type}."
            )

    workflow_run_type = described.get(WORKFLOW_RUN_ID_COLUMN)
    if workflow_run_type not in {"LowCardinality(String)", "String"}:
        raise ValueError(
            "ClickHouse type mismatch for dagster_run_id: expected "
            f"LowCardinality(String), found {workflow_run_type}."
        )

    # Apply compact-part settings to tables created by the previous loader too.
    client.execute(
        f"ALTER TABLE {table_name} MODIFY SETTING "
        f"min_bytes_for_wide_part = {WIDE_PART_SETTINGS['min_bytes_for_wide_part']}, "
        f"min_rows_for_wide_part = {WIDE_PART_SETTINGS['min_rows_for_wide_part']}"
    )

    # Workflow visibility is controlled by a tiny registry instead of an
    # UPDATE mutation over the 10K-50K-column compact parts. Existing rows
    # receive the empty run id and remain visible until the first committed
    # replacement for their batch is registered.
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {commit_table_name}
        (
            source_batch_id String,
            dagster_run_id String,
            committed_at DateTime64(6, 'UTC') DEFAULT now64(6)
        )
        ENGINE = MergeTree
        ORDER BY (source_batch_id, committed_at, dagster_run_id)
        """
    )
    client.execute(
        f"""
        CREATE OR REPLACE VIEW {visible_view_name} AS
        SELECT telemetry.*
        FROM {table_name} AS telemetry
        LEFT JOIN
        (
            SELECT
                source_batch_id,
                argMax(dagster_run_id, committed_at) AS active_dagster_run_id
            FROM {commit_table_name}
            GROUP BY source_batch_id
        ) AS committed
            ON committed.source_batch_id = telemetry.source_batch_id
        WHERE telemetry.dagster_run_id = ifNull(
            committed.active_dagster_run_id,
            ''
        )
        """
    )
    return table_name


def _get_available_memory_gb() -> float | None:
    try:
        with open("/proc/meminfo", "r", encoding="ascii") as meminfo:
            for line in meminfo:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / (1024 * 1024)
    except (OSError, ValueError, IndexError):
        return None
    return None


def _ensure_memory_headroom(stage_label: str) -> None:
    available_gb = _get_available_memory_gb()
    if available_gb is not None and available_gb < MEMORY_SAFE_FLOOR_GB:
        raise MemoryError(
            f"Available memory is below the safe floor ({available_gb:.2f}GB < "
            f"{MEMORY_SAFE_FLOOR_GB}GB); '{stage_label}' was not started."
        )


def _wait_for_merge_pressure_to_settle(
    client: Client,
    *,
    database: str,
    table: str,
    stage_label: str,
) -> None:
    waited = 0.0
    while True:
        active_parts = client.execute(
            "SELECT count() FROM system.parts WHERE database = %(database)s "
            "AND table = %(table)s AND active",
            {"database": database, "table": table},
        )[0][0]
        if active_parts <= MAX_ACTIVE_PARTS_BEFORE_CHUNK:
            return
        if waited >= PARTS_SETTLE_MAX_WAIT_SECONDS:
            raise RuntimeError(
                f"{stage_label}: {active_parts} active MergeTree parts remained after "
                f"{PARTS_SETTLE_MAX_WAIT_SECONDS:.0f}s; load stopped before adding "
                "more merge pressure."
            )
        print(
            f"{stage_label}: active parts {active_parts} > "
            f"{MAX_ACTIVE_PARTS_BEFORE_CHUNK}; waiting "
            f"{PARTS_SETTLE_POLL_SECONDS:.0f}s for merges.",
            flush=True,
        )
        time.sleep(PARTS_SETTLE_POLL_SECONDS)
        waited += PARTS_SETTLE_POLL_SECONDS


def _get_clickhouse_resident_gb(client: Client) -> float | None:
    try:
        rows = client.execute(
            "SELECT value FROM system.asynchronous_metrics "
            "WHERE metric = 'jemalloc.resident'"
        )
        return rows[0][0] / (1024**3) if rows else None
    except Exception:
        return None


def _checkpoint_cooldown(client: Client, stage_label: str) -> None:
    resident_gb = _get_clickhouse_resident_gb(client)
    if resident_gb is None or resident_gb <= CHECKPOINT_TARGET_GB:
        return
    waited = 0.0
    print(
        f"{stage_label}: ClickHouse resident memory is {resident_gb:.2f}GB; "
        f"waiting for {CHECKPOINT_TARGET_GB:.2f}GB cooldown.",
        flush=True,
    )
    while waited < CHECKPOINT_MAX_WAIT_SECONDS:
        time.sleep(CHECKPOINT_POLL_SECONDS)
        waited += CHECKPOINT_POLL_SECONDS
        resident_gb = _get_clickhouse_resident_gb(client)
        if resident_gb is None or resident_gb <= CHECKPOINT_TARGET_GB:
            return
    print(
        f"{stage_label}: cooldown limit reached after {waited:.0f}s; continuing "
        f"at {resident_gb:.2f}GB under the query watchdog.",
        flush=True,
    )


def _execute_with_oom_watchdog(
    client: Client,
    query: str,
    *,
    stage_label: str,
) -> None:
    if not _env_bool("CLICKHOUSE_OOM_WATCHDOG_ENABLED", True):
        client.execute(query, settings=CLICKHOUSE_SETTINGS)
        return

    query_id = uuid.uuid4().hex
    killed = threading.Event()
    stop_watching = threading.Event()

    def watch() -> None:
        while not stop_watching.is_set():
            available_gb = _get_available_memory_gb()
            if available_gb is not None and available_gb < MEMORY_WATCHDOG_CRITICAL_GB:
                print(
                    f"{stage_label}: memory fell to {available_gb:.2f}GB; "
                    "cancelling the ClickHouse query.",
                    flush=True,
                )
                kill_client = None
                try:
                    kill_client = _clickhouse_client()
                    kill_client.execute(
                        f"KILL QUERY WHERE query_id = {_quote_sql_string(query_id)} SYNC"
                    )
                except Exception as error:
                    print(f"OOM watchdog could not cancel query: {error}", flush=True)
                finally:
                    if kill_client is not None:
                        kill_client.disconnect()
                killed.set()
                return
            stop_watching.wait(MEMORY_WATCHDOG_POLL_SECONDS)

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    try:
        client.execute(query, settings=CLICKHOUSE_SETTINGS, query_id=query_id)
    except Exception as error:
        if killed.is_set():
            raise MemoryError(
                f"{stage_label} was cancelled after available memory crossed "
                f"the {MEMORY_WATCHDOG_CRITICAL_GB}GB critical threshold."
            ) from error
        raise
    finally:
        stop_watching.set()
        watcher.join(timeout=2)


def _compute_row_chunk_size(
    column_count: int,
    *,
    safe_cell_limit: int,
    max_rows_per_chunk: int,
) -> int:
    return max(1, min(max_rows_per_chunk, safe_cell_limit // max(column_count, 1)))


def _compress_next_row_chunk(
    source: BinaryIO,
    destination: Path,
    *,
    row_limit: int,
    zstd_level: int,
) -> int:
    row_count = 0
    with destination.open("wb") as compressed_file:
        with zstd.ZstdCompressor(level=zstd_level).stream_writer(
            compressed_file
        ) as compressor:
            while row_count < row_limit:
                line = source.readline()
                if not line:
                    break
                if not line.strip():
                    continue
                compressor.write(line)
                row_count += 1
    return row_count


def _split_and_upload_parts(
    part_files: Sequence[Path],
    *,
    expected_header: Sequence[str],
    row_chunk_size: int,
    zstd_level: int,
    minio_client: Minio,
    bucket: str,
    object_prefix: str,
    temporary_directory: Path,
    uploaded: list[dict[str, object]],
) -> int:
    total_rows = 0
    chunk_index = 0

    for part_file in part_files:
        with part_file.open("rb") as raw_file:
            with zstd.ZstdDecompressor().stream_reader(raw_file) as decompressed:
                source = io.BufferedReader(decompressed)
                header = _parse_header_line(source.readline(), part_file)
                if header != list(expected_header):
                    raise ValueError(f"AU-AIR part schema differs: {part_file}")

                while True:
                    chunk_index += 1
                    local_chunk = temporary_directory / f"chunk-{chunk_index:05d}.tsv.zst"
                    row_count = _compress_next_row_chunk(
                        source,
                        local_chunk,
                        row_limit=row_chunk_size,
                        zstd_level=zstd_level,
                    )
                    if row_count == 0:
                        local_chunk.unlink(missing_ok=True)
                        chunk_index -= 1
                        break

                    object_key = f"{object_prefix}/chunk-{chunk_index:05d}.tsv.zst"
                    try:
                        minio_client.fput_object(
                            bucket,
                            object_key,
                            str(local_chunk),
                            content_type="application/zstd",
                        )
                    finally:
                        local_chunk.unlink(missing_ok=True)
                    uploaded.append({"object_key": object_key, "row_count": row_count})
                    total_rows += row_count
                    print(
                        f"Prepared ingest chunk {chunk_index}: {row_count:,} rows "
                        f"({bucket}/{object_key}).",
                        flush=True,
                    )

    return total_rows


def _remove_ingest_objects(
    client: Minio,
    bucket: str,
    uploaded_chunks: Sequence[dict[str, object]],
) -> None:
    for chunk in uploaded_chunks:
        object_key = str(chunk["object_key"])
        try:
            client.remove_object(bucket, object_key)
        except Exception as error:
            print(
                "Warning: temporary MinIO ingest object could not be removed: "
                f"{bucket}/{object_key}: {error}",
                flush=True,
            )


def _bulk_insert_chunks(
    client: Client,
    *,
    table_name: str,
    database: str,
    table: str,
    source_columns: Sequence[str],
    batch_id: str,
    dagster_run_id: str,
    minio_bucket: str,
    uploaded_chunks: Sequence[dict[str, object]],
) -> float:
    insert_columns = ", ".join(
        _quote_identifier(column_name)
        for column_name in [
            *source_columns,
            "source_batch_id",
            WORKFLOW_RUN_ID_COLUMN,
        ]
    )
    structure = _source_structure(source_columns)
    protocol = "https" if _env_bool("MINIO_SECURE") else "http"
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    started = time.time()

    for chunk_index, chunk in enumerate(uploaded_chunks, start=1):
        stage_label = f"ClickHouse chunk {chunk_index}/{len(uploaded_chunks)}"
        _ensure_memory_headroom(stage_label)
        _wait_for_merge_pressure_to_settle(
            client,
            database=database,
            table=table,
            stage_label=stage_label,
        )
        if (
            CHECKPOINT_INTERVAL_CHUNKS > 0
            and chunk_index > 1
            and (chunk_index - 1) % CHECKPOINT_INTERVAL_CHUNKS == 0
        ):
            _checkpoint_cooldown(client, stage_label)

        object_key = str(chunk["object_key"])
        s3_url = f"{protocol}://{_minio_endpoint()}/{minio_bucket}/{object_key}"
        query = f"""
            INSERT INTO {table_name} ({insert_columns})
            SELECT
                *,
                {_quote_sql_string(batch_id)},
                {_quote_sql_string(dagster_run_id)}
            FROM s3(
                {_quote_sql_string(s3_url)},
                {_quote_sql_string(access_key)},
                {_quote_sql_string(secret_key)},
                'TabSeparated',
                {_quote_sql_string(structure)}
            )
        """
        chunk_started = time.time()
        _execute_with_oom_watchdog(client, query, stage_label=stage_label)
        print(
            f"{stage_label} completed: {int(chunk['row_count']):,} rows in "
            f"{time.time() - chunk_started:.1f}s.",
            flush=True,
        )

    return time.time() - started


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--dagster-run-id", required=True)
    parser.add_argument("--source-etag", required=True)
    parser.add_argument("--validation-report-uri", required=True)
    parser.add_argument("--expected-row-count", required=True, type=int)
    parser.add_argument("--expected-column-count", required=True, type=int)
    parser.add_argument("--metadata-out", required=True, type=Path)
    parser.add_argument("--database", default=os.getenv("CLICKHOUSE_DATABASE", "default"))
    parser.add_argument(
        "--table",
        default=os.getenv("CLICKHOUSE_AUAIR_TABLE", "auair_telemetry"),
    )
    parser.add_argument(
        "--visible-view",
        default=os.getenv(
            "CLICKHOUSE_AUAIR_VISIBLE_VIEW",
            "auair_telemetry_committed",
        ),
    )
    parser.add_argument(
        "--commit-table",
        default=os.getenv(
            "CLICKHOUSE_AUAIR_COMMIT_TABLE",
            "auair_telemetry_workflow_commits",
        ),
    )
    parser.add_argument(
        "--safe-cell-limit",
        type=int,
        default=int(
            os.getenv("CLICKHOUSE_WIDE_SAFE_CELL_LIMIT", str(DEFAULT_WIDE_SAFE_CELL_LIMIT))
        ),
    )
    parser.add_argument(
        "--max-rows-per-chunk",
        type=int,
        default=int(
            os.getenv("CLICKHOUSE_WIDE_MAX_ROWS_PER_CHUNK", str(DEFAULT_MAX_ROWS_PER_CHUNK))
        ),
    )
    parser.add_argument(
        "--row-chunk-zstd-level",
        type=int,
        default=int(
            os.getenv("CLICKHOUSE_ROW_CHUNK_ZSTD_LEVEL", str(DEFAULT_ROW_CHUNK_ZSTD_LEVEL))
        ),
    )
    parser.add_argument(
        "--ingest-bucket",
        default=os.getenv("CLICKHOUSE_INGEST_BUCKET", "pipeline-artifacts"),
    )
    parser.add_argument(
        "--ingest-prefix",
        default=os.getenv("CLICKHOUSE_INGEST_PREFIX", "_clickhouse-ingest/auair"),
    )
    parser.add_argument(
        "--schema-alter-chunk-columns",
        type=int,
        default=int(
            os.getenv(
                "CLICKHOUSE_SCHEMA_ALTER_CHUNK_COLUMNS",
                str(DEFAULT_SCHEMA_ALTER_CHUNK_COLUMNS),
            )
        ),
    )
    args = parser.parse_args()
    if args.expected_row_count <= 0:
        parser.error("--expected-row-count must be greater than zero")
    if args.expected_column_count < 17:
        parser.error("--expected-column-count must be at least 17")
    if args.safe_cell_limit <= 0:
        parser.error("--safe-cell-limit must be greater than zero")
    if args.max_rows_per_chunk <= 0:
        parser.error("--max-rows-per-chunk must be greater than zero")
    if not 1 <= args.row_chunk_zstd_level <= 22:
        parser.error("--row-chunk-zstd-level must be between 1 and 22")
    if args.schema_alter_chunk_columns <= 0:
        parser.error("--schema-alter-chunk-columns must be greater than zero")
    for label, value in (
        ("--database", args.database),
        ("--table", args.table),
        ("--visible-view", args.visible_view),
        ("--commit-table", args.commit_table),
    ):
        if not SAFE_IDENTIFIER.fullmatch(value):
            parser.error(f"{label} must be a safe ClickHouse identifier")
    return args


def main() -> None:
    args = parse_args()
    previous_sigterm_handler = signal.getsignal(signal.SIGTERM)

    def _cancel_on_sigterm(signum, _frame) -> None:
        raise KeyboardInterrupt(
            f"ClickHouse AU-AIR load canceled by signal {signum}."
        )

    signal.signal(signal.SIGTERM, _cancel_on_sigterm)
    started = time.time()
    part_files = _part_files(args.input)
    source_columns = _read_header(part_files[0])
    if len(source_columns) != args.expected_column_count:
        raise ValueError(
            f"Expected {args.expected_column_count} AU-AIR columns, "
            f"received {len(source_columns)}."
        )
    for part_file in part_files[1:]:
        if _read_header(part_file) != source_columns:
            raise ValueError(f"AU-AIR part schema differs: {part_file}")

    row_chunk_size = _compute_row_chunk_size(
        len(source_columns),
        safe_cell_limit=args.safe_cell_limit,
        max_rows_per_chunk=args.max_rows_per_chunk,
    )
    print(
        f"High-column AU-AIR load: {len(source_columns):,} columns, "
        f"maximum {row_chunk_size:,} source rows/query "
        f"({args.safe_cell_limit:,} safe cells).",
        flush=True,
    )

    minio_client = _minio_client()
    _ensure_bucket(minio_client, args.ingest_bucket)
    object_prefix = "/".join(
        segment.strip("/")
        for segment in (
            args.ingest_prefix,
            _safe_object_segment(args.dataset_id),
            _safe_object_segment(args.batch_id),
            uuid.uuid4().hex,
        )
        if segment.strip("/")
    )
    uploaded_chunks: list[dict[str, object]] = []
    client: Client | None = None
    table_name: str | None = None
    query_parameters = {
        "batch_id": args.batch_id,
        "dagster_run_id": args.dagster_run_id,
    }

    try:
        with tempfile.TemporaryDirectory(prefix="dpm-auair-ch-ingest-") as temp:
            source_row_count = _split_and_upload_parts(
                part_files,
                expected_header=source_columns,
                row_chunk_size=row_chunk_size,
                zstd_level=args.row_chunk_zstd_level,
                minio_client=minio_client,
                bucket=args.ingest_bucket,
                object_prefix=object_prefix,
                temporary_directory=Path(temp),
                uploaded=uploaded_chunks,
            )
        if source_row_count != args.expected_row_count:
            raise RuntimeError(
                "Validated AU-AIR row count changed before ClickHouse load: "
                f"expected {args.expected_row_count:,}, found {source_row_count:,}."
            )

        client = _clickhouse_client()
        table_name = _ensure_table_schema(
            client,
            database=args.database,
            table=args.table,
            visible_view=args.visible_view,
            commit_table=args.commit_table,
            source_columns=source_columns,
            alter_chunk_columns=args.schema_alter_chunk_columns,
        )
        # A retry of the same Dagster run starts cleanly, but an older
        # committed run for the logical batch remains queryable until this
        # whole workflow reaches SUCCESS.
        retry_rows = client.execute(
            f"SELECT count() FROM {table_name} "
            "WHERE source_batch_id = %(batch_id)s "
            "AND dagster_run_id = %(dagster_run_id)s",
            query_parameters,
        )[0][0]
        if retry_rows:
            client.execute(
                f"ALTER TABLE {table_name} DELETE "
                "WHERE source_batch_id = %(batch_id)s "
                "AND dagster_run_id = %(dagster_run_id)s",
                query_parameters,
                settings={**CLICKHOUSE_SETTINGS, "mutations_sync": 1},
            )

        clickhouse_load_seconds = _bulk_insert_chunks(
            client,
            table_name=table_name,
            database=args.database,
            table=args.table,
            source_columns=source_columns,
            batch_id=args.batch_id,
            dagster_run_id=args.dagster_run_id,
            minio_bucket=args.ingest_bucket,
            uploaded_chunks=uploaded_chunks,
        )
        stored_rows = client.execute(
            f"SELECT count() FROM {table_name} "
            "WHERE source_batch_id = %(batch_id)s "
            "AND dagster_run_id = %(dagster_run_id)s",
            query_parameters,
        )[0][0]
        if stored_rows != args.expected_row_count:
            raise RuntimeError(
                "ClickHouse AU-AIR row count mismatch: "
                f"expected {args.expected_row_count:,}, stored {stored_rows:,}."
            )
        flight_ids = [
            row[0]
            for row in client.execute(
                f"SELECT DISTINCT flight_id FROM {table_name} "
                "WHERE source_batch_id = %(batch_id)s "
                "AND dagster_run_id = %(dagster_run_id)s ORDER BY flight_id",
                query_parameters,
            )
        ]
        disk_bytes = client.execute(
            "SELECT sum(bytes_on_disk) FROM system.parts "
            "WHERE database = %(database)s AND table = %(table)s AND active",
            {"database": args.database, "table": args.table},
        )[0][0] or 0

        metadata = {
            "database": args.database,
            "table": args.table,
            "visible_view": args.visible_view,
            "workflow_commit_table": args.commit_table,
            "workflow_visibility": "pending-until-run-success",
            "dataset_id": args.dataset_id,
            "batch_id": args.batch_id,
            "dagster_run_id": args.dagster_run_id,
            "source_etag": args.source_etag,
            "validation_report_uri": args.validation_report_uri,
            "source_row_count": source_row_count,
            "source_column_count": len(source_columns),
            "stored_row_count": stored_rows,
            "flight_count": len(flight_ids),
            "flight_ids": flight_ids,
            "input_part_count": len(part_files),
            "insert_chunk_count": len(uploaded_chunks),
            "row_chunk_size": row_chunk_size,
            "safe_cell_limit": args.safe_cell_limit,
            "storage_layout": "dynamic-wide-compact-parts",
            "storage_codec": "type-aware T64/DoubleDelta/ZSTD(3)",
            "load_method": "temporary-minio-s3-bulk",
            "clickhouse_load_seconds": round(clickhouse_load_seconds, 3),
            "elapsed_seconds": round(time.time() - started, 3),
            "clickhouse_table_disk_bytes": int(disk_bytes),
            "temporary_ingest_bucket": args.ingest_bucket,
            "temporary_ingest_prefix": object_prefix,
            "temporary_ingest_cleanup": True,
            "output_uri": (
                f"clickhouse://{args.database}/{args.table}"
                f"?source_batch_id={quote(args.batch_id)}"
            ),
        }
        args.metadata_out.parent.mkdir(parents=True, exist_ok=True)
        args.metadata_out.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(json.dumps(metadata, ensure_ascii=False), flush=True)
    except BaseException:
        # Graceful Dagster cancellation reaches the loader as SIGTERM. Remove
        # only this run's pending rows here; the terminal run-status sensor is
        # an idempotent safety net for hard termination.
        if client is not None and table_name is not None:
            try:
                client.execute(
                    f"ALTER TABLE {table_name} DELETE "
                    "WHERE source_batch_id = %(batch_id)s "
                    "AND dagster_run_id = %(dagster_run_id)s",
                    query_parameters,
                    settings={**CLICKHOUSE_SETTINGS, "mutations_sync": 1},
                )
            except Exception as cleanup_error:
                print(
                    "Warning: interrupted ClickHouse run could not be rolled "
                    f"back immediately: {cleanup_error}",
                    flush=True,
                )
        raise
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm_handler)
        _remove_ingest_objects(minio_client, args.ingest_bucket, uploaded_chunks)
        if client is not None:
            client.disconnect()


if __name__ == "__main__":
    main()
