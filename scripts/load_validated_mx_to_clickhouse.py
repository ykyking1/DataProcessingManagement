"""Load one validated MX batch into a query-oriented ClickHouse table.

The validated dataset remains a wide collection of ``part-*.tab.zst`` files
for DVC/MinIO publication.  ClickHouse receives a long representation with one
row per source-row/feature pair so datasets with 10K-50K dynamic columns can
share one stable table schema.

The loader deliberately avoids pandas.  It splits the processed ZSTD files by
source rows, uploads small temporary objects to MinIO, and lets ClickHouse read
and pivot each object natively through ``s3()`` and ``ARRAY JOIN``.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Iterator, Sequence
from urllib.parse import quote

import zstandard as zstd
from clickhouse_driver import Client
from minio import Minio


DEFAULT_TARGET_OUTPUT_ROWS_PER_CHUNK = 10_000_000
DEFAULT_MEMORY_SAFE_FLOOR_GB = 2.5
DEFAULT_ZSTD_LEVEL = 12
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

CLICKHOUSE_SETTINGS = {
    "max_query_size": 300_000_000,
    "max_ast_elements": 10_000_000,
    "max_expanded_ast_elements": 10_000_000,
    "input_format_parallel_parsing": 0,
    "max_threads": 2,
    "max_insert_threads": 1,
    "max_block_size": 8192,
    "max_insert_block_size": 8192,
}


def _environment_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _validated_identifier(value: str, label: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(
            f"{label} must contain only letters, digits, and underscores "
            f"and cannot start with a digit: {value!r}"
        )
    return value


def _quote_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


def _quote_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _safe_object_component(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-.")
    return safe or "unknown"


def _available_memory_gb() -> float | None:
    try:
        with Path("/proc/meminfo").open("r", encoding="utf-8") as meminfo:
            for line in meminfo:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / (1024 * 1024)
    except (OSError, ValueError, IndexError):
        return None
    return None


def _ensure_memory_headroom(stage: str, minimum_gb: float) -> None:
    if minimum_gb <= 0:
        return
    available_gb = _available_memory_gb()
    if available_gb is not None and available_gb < minimum_gb:
        raise MemoryError(
            f"Available memory is below the ClickHouse safety floor "
            f"({available_gb:.2f}GB < {minimum_gb:.2f}GB) before {stage}."
        )


@contextmanager
def _zstd_reader(path: Path) -> Iterator[BinaryIO]:
    with path.open("rb") as compressed:
        with zstd.ZstdDecompressor().stream_reader(compressed) as stream:
            with io.BufferedReader(stream) as buffered:
                yield buffered


def _read_header(path: Path) -> list[str]:
    with _zstd_reader(path) as source:
        raw_header = source.readline()
    if not raw_header:
        raise ValueError(f"Processed part has no header: {path}")
    header = raw_header.decode("utf-8-sig").rstrip("\r\n").split("\t")
    if header[:2] != ["timestamp", "aircraft_type"]:
        raise ValueError(
            "Processed ClickHouse input must start with timestamp and "
            f"aircraft_type; received {header[:2]} from {path}."
        )
    if len(header) < 3:
        raise ValueError(f"Processed part has no feature columns: {path}")
    if len(header) != len(set(header)):
        raise ValueError(f"Processed part contains duplicate columns: {path}")
    return header


def _ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def _remove_prefix(client: Minio, bucket: str, prefix: str) -> None:
    for item in client.list_objects(bucket, prefix=f"{prefix}/", recursive=True):
        client.remove_object(bucket, item.object_name)


def _row_chunk_size(feature_count: int, target_output_rows: int) -> int:
    if feature_count <= 0:
        raise ValueError("feature_count must be greater than zero")
    if target_output_rows <= 0:
        raise ValueError("target_output_rows must be greater than zero")
    return max(1, min(target_output_rows // feature_count, 200_000))


def _upload_row_chunks(
    *,
    input_parts: Sequence[Path],
    expected_header: Sequence[str],
    minio_client: Minio,
    bucket: str,
    object_prefix: str,
    rows_per_chunk: int,
    zstd_level: int,
) -> tuple[list[tuple[str, int]], int]:
    compressor = zstd.ZstdCompressor(level=zstd_level)
    uploaded: list[tuple[str, int]] = []
    buffered_rows: list[bytes] = []
    chunk_index = 0
    total_source_rows = 0

    def flush() -> None:
        nonlocal chunk_index, total_source_rows
        if not buffered_rows:
            return
        chunk_index += 1
        raw = b"".join(buffered_rows)
        compressed = compressor.compress(raw)
        object_key = f"{object_prefix}/chunk-{chunk_index:05d}.tab.zst"
        minio_client.put_object(
            bucket,
            object_key,
            io.BytesIO(compressed),
            length=len(compressed),
            content_type="application/zstd",
        )
        row_count = len(buffered_rows)
        uploaded.append((object_key, row_count))
        total_source_rows += row_count
        buffered_rows.clear()

    for input_part in input_parts:
        part_header = _read_header(input_part)
        if part_header != list(expected_header):
            raise ValueError(f"Processed part schema differs: {input_part}")
        with _zstd_reader(input_part) as source:
            source.readline()
            for line in source:
                if not line.strip():
                    continue
                buffered_rows.append(line)
                if len(buffered_rows) >= rows_per_chunk:
                    flush()
    flush()
    return uploaded, total_source_rows


def _minio_client() -> Minio:
    return Minio(
        os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        secure=_environment_flag("MINIO_SECURE"),
    )


def _clickhouse_client(database: str) -> Client:
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000")),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=database,
        settings=CLICKHOUSE_SETTINGS,
    )


def _create_measurement_table(client: Client, database: str, table: str) -> None:
    database_name = _quote_identifier(database)
    table_name = f"{database_name}.{_quote_identifier(table)}"
    client.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            dataset_id LowCardinality(String) CODEC(ZSTD(3)),
            batch_id LowCardinality(String) CODEC(ZSTD(3)),
            record_id UInt64 CODEC(ZSTD(3)),
            timestamp DateTime64(3, 'UTC') CODEC(ZSTD(3)),
            aircraft_type LowCardinality(String) CODEC(ZSTD(3)),
            sensor_name LowCardinality(String) CODEC(ZSTD(3)),
            value Nullable(Float64) CODEC(ZSTD(3)),
            ingested_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(ZSTD(3))
        )
        ENGINE = MergeTree
        PARTITION BY (dataset_id, toYYYYMM(timestamp))
        ORDER BY (dataset_id, batch_id, sensor_name, timestamp, record_id)
        """
    )


def _source_structure(columns: Sequence[str]) -> str:
    definitions = [
        f"{_quote_identifier('timestamp')} String",
        f"{_quote_identifier('aircraft_type')} String",
    ]
    definitions.extend(
        f"{_quote_identifier(column)} Float64" for column in columns[2:]
    )
    return ", ".join(definitions)


def _sensor_pairs(feature_columns: Sequence[str]) -> str:
    return ", ".join(
        "tuple("
        f"{_quote_string(column)}, "
        f"toNullable({_quote_identifier(column)})"
        ")"
        for column in feature_columns
    )


def _s3_url(bucket: str, object_key: str) -> str:
    endpoint = os.getenv(
        "CLICKHOUSE_MINIO_ENDPOINT",
        os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000"),
    ).strip().rstrip("/")
    secure = _environment_flag("MINIO_SECURE")
    protocol = "https" if secure else "http"
    return f"{protocol}://{endpoint}/{bucket}/{quote(object_key, safe='/')}"


def _insert_chunk(
    client: Client,
    *,
    database: str,
    table: str,
    dataset_id: str,
    batch_id: str,
    chunk_index: int,
    bucket: str,
    object_key: str,
    structure: str,
    pairs_sql: str,
) -> None:
    table_name = f"{_quote_identifier(database)}.{_quote_identifier(table)}"
    query = f"""
        INSERT INTO {table_name}
            (dataset_id, batch_id, record_id, timestamp,
             aircraft_type, sensor_name, value)
        SELECT
            %(dataset_id)s,
            %(batch_id)s,
            toUInt64(%(chunk_index)s) * 1000000000000
                + toUInt64(source_row_number),
            parseDateTime64BestEffort(timestamp, 3, 'UTC'),
            upperUTF8(trim(aircraft_type)),
            tupleElement(sensor_pair, 1),
            tupleElement(sensor_pair, 2)
        FROM
        (
            SELECT rowNumberInAllBlocks() AS source_row_number, *
            FROM s3(
                %(source_url)s,
                %(access_key)s,
                %(secret_key)s,
                'TabSeparated',
                %(source_structure)s,
                'zstd'
            )
        )
        ARRAY JOIN [{pairs_sql}] AS sensor_pair
    """
    client.execute(
        query,
        {
            "dataset_id": dataset_id,
            "batch_id": batch_id,
            "chunk_index": chunk_index,
            "source_url": _s3_url(bucket, object_key),
            "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
            "source_structure": structure,
        },
        settings=CLICKHOUSE_SETTINGS,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load a validated MX .tab.zst batch into ClickHouse."
    )
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
        default=os.getenv("CLICKHOUSE_TABLE", "mx_measurements"),
    )
    parser.add_argument(
        "--ingest-bucket",
        default=os.getenv("CLICKHOUSE_INGEST_BUCKET", "clickhouse-ingest"),
    )
    parser.add_argument(
        "--target-output-rows-per-chunk",
        type=int,
        default=int(
            os.getenv(
                "CLICKHOUSE_TARGET_OUTPUT_ROWS_PER_CHUNK",
                str(DEFAULT_TARGET_OUTPUT_ROWS_PER_CHUNK),
            )
        ),
    )
    parser.add_argument(
        "--memory-safe-floor-gb",
        type=float,
        default=float(
            os.getenv(
                "CLICKHOUSE_MEMORY_SAFE_FLOOR_GB",
                str(DEFAULT_MEMORY_SAFE_FLOOR_GB),
            )
        ),
    )
    parser.add_argument(
        "--zstd-level",
        type=int,
        default=int(os.getenv("CLICKHOUSE_INGEST_ZSTD_LEVEL", str(DEFAULT_ZSTD_LEVEL))),
    )
    parser.add_argument(
        "--keep-ingest-objects",
        action="store_true",
        default=_environment_flag("CLICKHOUSE_KEEP_INGEST_OBJECTS"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    database = _validated_identifier(args.database, "ClickHouse database")
    table = _validated_identifier(args.table, "ClickHouse table")
    input_directory = args.input.resolve()
    if not input_directory.is_dir():
        raise FileNotFoundError(f"Validated MX directory not found: {input_directory}")

    input_parts = sorted(input_directory.glob("part-*.tab.zst"))
    if not input_parts:
        raise FileNotFoundError(
            f"Validated MX directory has no part-*.tab.zst files: {input_directory}"
        )

    header = _read_header(input_parts[0])
    feature_columns = header[2:]
    feature_count = len(feature_columns)
    rows_per_chunk = _row_chunk_size(
        feature_count,
        args.target_output_rows_per_chunk,
    )
    object_prefix = "/".join(
        (
            "validated-mx",
            _safe_object_component(args.dataset_id),
            _safe_object_component(args.batch_id),
            _safe_object_component(args.dagster_run_id),
        )
    )

    minio_client = _minio_client()
    _ensure_bucket(minio_client, args.ingest_bucket)
    _remove_prefix(minio_client, args.ingest_bucket, object_prefix)
    _ensure_memory_headroom("temporary MinIO chunk creation", args.memory_safe_floor_gb)

    uploaded_chunks: list[tuple[str, int]] = []
    load_succeeded = False
    try:
        uploaded_chunks, source_row_count = _upload_row_chunks(
            input_parts=input_parts,
            expected_header=header,
            minio_client=minio_client,
            bucket=args.ingest_bucket,
            object_prefix=object_prefix,
            rows_per_chunk=rows_per_chunk,
            zstd_level=args.zstd_level,
        )
        if source_row_count != args.expected_row_count:
            raise RuntimeError(
                "Validated source row count changed before ClickHouse load: "
                f"expected {args.expected_row_count:,}, read {source_row_count:,}."
            )

        client = _clickhouse_client(database)
        _create_measurement_table(client, database, table)
        table_name = f"{_quote_identifier(database)}.{_quote_identifier(table)}"
        query_parameters = {
            "dataset_id": args.dataset_id,
            "batch_id": args.batch_id,
        }
        existing_rows = client.execute(
            f"SELECT count() FROM {table_name} "
            "WHERE dataset_id = %(dataset_id)s AND batch_id = %(batch_id)s",
            query_parameters,
            settings=CLICKHOUSE_SETTINGS,
        )[0][0]
        if existing_rows:
            _ensure_memory_headroom("idempotent batch cleanup", args.memory_safe_floor_gb)
            client.execute(
                f"ALTER TABLE {table_name} DELETE WHERE "
                "dataset_id = %(dataset_id)s AND batch_id = %(batch_id)s",
                query_parameters,
                settings={**CLICKHOUSE_SETTINGS, "mutations_sync": 1},
            )

        structure = _source_structure(header)
        pairs_sql = _sensor_pairs(feature_columns)
        for chunk_index, (object_key, _) in enumerate(uploaded_chunks, start=1):
            _ensure_memory_headroom(
                f"ClickHouse insert {chunk_index}/{len(uploaded_chunks)}",
                args.memory_safe_floor_gb,
            )
            _insert_chunk(
                client,
                database=database,
                table=table,
                dataset_id=args.dataset_id,
                batch_id=args.batch_id,
                chunk_index=chunk_index,
                bucket=args.ingest_bucket,
                object_key=object_key,
                structure=structure,
                pairs_sql=pairs_sql,
            )

        measurement_row_count = client.execute(
            f"SELECT count() FROM {table_name} "
            "WHERE dataset_id = %(dataset_id)s AND batch_id = %(batch_id)s",
            query_parameters,
            settings=CLICKHOUSE_SETTINGS,
        )[0][0]
        expected_measurement_rows = source_row_count * feature_count
        if measurement_row_count != expected_measurement_rows:
            raise RuntimeError(
                "ClickHouse measurement count mismatch: "
                f"expected {expected_measurement_rows:,}, "
                f"received {measurement_row_count:,}."
            )

        metadata = {
            "database": database,
            "table": table,
            "dataset_id": args.dataset_id,
            "batch_id": args.batch_id,
            "dagster_run_id": args.dagster_run_id,
            "source_etag": args.source_etag,
            "validation_report_uri": args.validation_report_uri,
            "source_row_count": source_row_count,
            "source_column_count": len(header),
            "feature_count": feature_count,
            "measurement_row_count": measurement_row_count,
            "input_part_count": len(input_parts),
            "ingest_chunk_count": len(uploaded_chunks),
            "source_rows_per_ingest_chunk": rows_per_chunk,
            "target_output_rows_per_chunk": args.target_output_rows_per_chunk,
            "storage_codec": "ZSTD(3)",
            "output_uri": (
                f"clickhouse://{database}/{table}"
                f"?dataset_id={quote(args.dataset_id)}&batch_id={quote(args.batch_id)}"
            ),
        }
        args.metadata_out.parent.mkdir(parents=True, exist_ok=True)
        args.metadata_out.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        load_succeeded = True
        print(json.dumps(metadata, ensure_ascii=False), flush=True)
    finally:
        if not args.keep_ingest_objects:
            _remove_prefix(minio_client, args.ingest_bucket, object_prefix)
        elif not load_succeeded:
            print(
                f"Temporary ingest objects retained after failure: "
                f"s3://{args.ingest_bucket}/{object_prefix}/",
                flush=True,
            )


if __name__ == "__main__":
    main()
