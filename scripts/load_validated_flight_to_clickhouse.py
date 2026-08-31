"""Load a validated flight telemetry batch into dashboard-serving ClickHouse."""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import zstandard as zstd
from clickhouse_driver import Client


FLIGHT_COLUMNS = [
    "time",
    "latitude",
    "longitude",
    "altitude",
    "velocity_x",
    "velocity_y",
    "velocity_z",
    "roll",
    "pitch",
    "yaw",
    "image_name",
    "box_x",
    "box_y",
    "box_w",
    "box_h",
    "class",
    "flight_id",
]
NUMERIC_COLUMNS = {
    "latitude",
    "longitude",
    "altitude",
    "velocity_x",
    "velocity_y",
    "velocity_z",
    "roll",
    "pitch",
    "yaw",
    "box_x",
    "box_y",
    "box_w",
    "box_h",
}
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
CLICKHOUSE_SETTINGS = {
    "max_execution_time": 0,
}


def _quote_identifier(value: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe ClickHouse identifier: {value!r}")
    return f"`{value}`"


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _clickhouse_client() -> Client:
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000")),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123"),
        database="default",
        secure=_env_bool("CLICKHOUSE_SECURE"),
        send_receive_timeout=600,
        settings=CLICKHOUSE_SETTINGS,
    )


def _create_table(client: Client, database: str, table: str) -> str:
    database_name = _quote_identifier(database)
    table_name = f"{database_name}.{_quote_identifier(table)}"
    client.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            time DateTime64(3, 'UTC') CODEC(ZSTD(3)),
            latitude Float64 CODEC(ZSTD(3)),
            longitude Float64 CODEC(ZSTD(3)),
            altitude Float64 CODEC(ZSTD(3)),
            velocity_x Float64 CODEC(ZSTD(3)),
            velocity_y Float64 CODEC(ZSTD(3)),
            velocity_z Float64 CODEC(ZSTD(3)),
            roll Float64 CODEC(ZSTD(3)),
            pitch Float64 CODEC(ZSTD(3)),
            yaw Float64 CODEC(ZSTD(3)),
            image_name String CODEC(ZSTD(3)),
            box_x Float64 CODEC(ZSTD(3)),
            box_y Float64 CODEC(ZSTD(3)),
            box_w Float64 CODEC(ZSTD(3)),
            box_h Float64 CODEC(ZSTD(3)),
            class LowCardinality(String) CODEC(ZSTD(3)),
            flight_id LowCardinality(String) CODEC(ZSTD(3)),
            source_batch_id LowCardinality(String) CODEC(ZSTD(3)),
            ingested_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(ZSTD(3))
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(time)
        ORDER BY (flight_id, time, image_name)
        """
    )
    return table_name


def _parse_time(value: str) -> datetime:
    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_row(row: dict[str, str], batch_id: str) -> tuple:
    missing = [column for column in FLIGHT_COLUMNS if row.get(column) in {None, ""}]
    if missing:
        raise ValueError(f"Flight row contains blank required fields: {missing}")

    values: list[object] = []
    for column in FLIGHT_COLUMNS:
        value = row[column]
        if column == "time":
            values.append(_parse_time(value))
        elif column in NUMERIC_COLUMNS:
            values.append(float(value))
        else:
            values.append(value.strip())
    values.append(batch_id)
    return tuple(values)


def _part_files(input_path: Path) -> list[Path]:
    resolved = input_path.resolve()
    if resolved.is_file() and resolved.name.endswith((".tab.zst", ".tab.zstd")):
        return [resolved]
    if not resolved.is_dir():
        raise FileNotFoundError(f"Processed flight input not found: {resolved}")
    parts = sorted(
        path
        for path in resolved.iterdir()
        if path.is_file() and path.name.endswith((".tab.zst", ".tab.zstd"))
    )
    if not parts:
        raise FileNotFoundError(f"No processed flight ZSTD parts found: {resolved}")
    return parts


def _read_part(part_file: Path, batch_id: str):
    with part_file.open("rb") as raw_file:
        with zstd.ZstdDecompressor().stream_reader(raw_file) as decompressed:
            with io.TextIOWrapper(
                decompressed,
                encoding="utf-8",
                newline="",
            ) as text_stream:
                reader = csv.DictReader(text_stream, delimiter="\t")
                if reader.fieldnames != FLIGHT_COLUMNS:
                    raise ValueError(
                        f"Unexpected flight schema in {part_file}: "
                        f"{reader.fieldnames}"
                    )
                for row in reader:
                    yield _parse_row(row, batch_id)


def _insert_batch(
    client: Client,
    *,
    table_name: str,
    part_files: list[Path],
    batch_id: str,
    chunk_rows: int,
) -> tuple[int, int, list[str]]:
    insert_columns = [*FLIGHT_COLUMNS, "source_batch_id"]
    column_sql = ", ".join(_quote_identifier(column) for column in insert_columns)
    query = f"INSERT INTO {table_name} ({column_sql}) VALUES"
    chunk: list[tuple] = []
    inserted_rows = 0
    insert_count = 0
    flight_ids: set[str] = set()

    for part_file in part_files:
        for row in _read_part(part_file, batch_id):
            chunk.append(row)
            flight_ids.add(str(row[FLIGHT_COLUMNS.index("flight_id")]))
            if len(chunk) >= chunk_rows:
                client.execute(query, chunk, types_check=True)
                inserted_rows += len(chunk)
                insert_count += 1
                chunk.clear()
    if chunk:
        client.execute(query, chunk, types_check=True)
        inserted_rows += len(chunk)
        insert_count += 1
    return inserted_rows, insert_count, sorted(flight_ids)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--dagster-run-id", required=True)
    parser.add_argument("--source-etag", required=True)
    parser.add_argument("--validation-report-uri", required=True)
    parser.add_argument("--expected-row-count", required=True, type=int)
    parser.add_argument("--metadata-out", required=True, type=Path)
    parser.add_argument(
        "--database",
        default=os.getenv("CLICKHOUSE_DATABASE", "default"),
    )
    parser.add_argument(
        "--table",
        default=os.getenv("CLICKHOUSE_TABLE", "telemetry"),
    )
    parser.add_argument(
        "--insert-chunk-rows",
        type=int,
        default=int(os.getenv("CLICKHOUSE_INSERT_CHUNK_ROWS", "10000")),
    )
    args = parser.parse_args()
    if args.expected_row_count <= 0:
        parser.error("--expected-row-count must be greater than zero")
    if args.insert_chunk_rows <= 0:
        parser.error("--insert-chunk-rows must be greater than zero")
    return args


def main() -> None:
    args = parse_args()
    part_files = _part_files(args.input)
    client = _clickhouse_client()
    table_name = _create_table(client, args.database, args.table)
    query_parameters = {"batch_id": args.batch_id}

    existing_rows = client.execute(
        f"SELECT count() FROM {table_name} "
        "WHERE source_batch_id = %(batch_id)s",
        query_parameters,
    )[0][0]
    if existing_rows:
        client.execute(
            f"ALTER TABLE {table_name} DELETE WHERE "
            "source_batch_id = %(batch_id)s",
            query_parameters,
            settings={**CLICKHOUSE_SETTINGS, "mutations_sync": 1},
        )

    inserted_rows, insert_count, flight_ids = _insert_batch(
        client,
        table_name=table_name,
        part_files=part_files,
        batch_id=args.batch_id,
        chunk_rows=args.insert_chunk_rows,
    )
    stored_rows = client.execute(
        f"SELECT count() FROM {table_name} "
        "WHERE source_batch_id = %(batch_id)s",
        query_parameters,
    )[0][0]
    if inserted_rows != args.expected_row_count or stored_rows != args.expected_row_count:
        raise RuntimeError(
            "ClickHouse flight row count mismatch: "
            f"expected {args.expected_row_count:,}, parsed {inserted_rows:,}, "
            f"stored {stored_rows:,}."
        )

    metadata = {
        "database": args.database,
        "table": args.table,
        "dataset_id": args.dataset_id,
        "batch_id": args.batch_id,
        "dagster_run_id": args.dagster_run_id,
        "source_etag": args.source_etag,
        "validation_report_uri": args.validation_report_uri,
        "source_row_count": inserted_rows,
        "source_column_count": len(FLIGHT_COLUMNS),
        "telemetry_row_count": stored_rows,
        "flight_count": len(flight_ids),
        "flight_ids": flight_ids,
        "input_part_count": len(part_files),
        "insert_chunk_count": insert_count,
        "storage_codec": "ZSTD(3)",
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
    print(json.dumps(metadata, ensure_ascii=False))


if __name__ == "__main__":
    main()
