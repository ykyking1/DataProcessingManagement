
"""
The default run creates five datasets. Each dataset contains 5,000 data rows
and respectively 10k, 20k, 30k, 40k, or 50k total columns. The first two
columns are ``timestamp`` and ``aircraft_type``; the remainder are synthetic
measurement columns.

Files are generated one at a time under the ignored ``local_data`` directory.
A local file is removed only after MinIO confirms that the uploaded object has
the same size. ``--batch-suffix`` creates a new object for repeated end-to-end
runs without overwriting an earlier batch. Existing MinIO objects are skipped
unless ``--overwrite`` is supplied.
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from minio import Minio
from minio.error import S3Error


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TOTAL_COLUMN_COUNTS = (10_000, 20_000, 30_000, 40_000, 50_000)
DEFAULT_ROW_COUNT = 5_000
DEFAULT_SEED = 42
DEFAULT_START_TIMESTAMP = pd.Timestamp("2026-01-01T00:00:00Z")
# Must stay aligned with Spark's ``yyyy-MM-dd'T'HH:mm:ss.SSSXXX`` input
# contract. Generated timestamps advance by whole seconds, so milliseconds are
# written explicitly as ``000`` and UTC is represented by ``Z``.
OUTPUT_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"
SAFE_BATCH_SUFFIX = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _normalise_endpoint(endpoint: str, secure: bool) -> tuple[str, bool]:
    """Return the host:port form expected by the MinIO Python client."""

    if "://" not in endpoint:
        return endpoint.rstrip("/"), secure

    parsed = urlparse(endpoint)
    if not parsed.netloc:
        raise ValueError(f"Invalid MinIO endpoint: {endpoint}")
    return parsed.netloc, parsed.scheme.lower() == "https"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate 5k-row MX .tab files and upload them to MinIO."
    )
    parser.add_argument(
        "--endpoint",
        default=os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000"),
        help="MinIO endpoint. Host execution default: 127.0.0.1:9000.",
    )
    parser.add_argument(
        "--access-key",
        default=os.getenv("MINIO_ROOT_USER", "minioadmin"),
    )
    parser.add_argument(
        "--secret-key",
        default=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
    )
    parser.add_argument(
        "--secure",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("MINIO_SECURE"),
        help="Use HTTPS when connecting to MinIO.",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("MINIO_RAW_BUCKET", "data-raw"),
    )
    parser.add_argument(
        "--prefix",
        default=os.getenv("MINIO_RAW_PREFIX", "mx-tab/inbox"),
    )
    parser.add_argument("--rows", type=int, default=DEFAULT_ROW_COUNT)
    parser.add_argument(
        "--column-counts",
        type=int,
        nargs="+",
        default=list(DEFAULT_TOTAL_COLUMN_COUNTS),
        metavar="N",
        help="Total MX column counts, including timestamp and aircraft_type.",
    )
    parser.add_argument(
        "--chunk-rows",
        type=int,
        default=100,
        help="Rows generated per in-memory block.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
    )
    parser.add_argument(
        "--batch-suffix",
        default="",
        help=(
            "Optional unique suffix appended to every generated batch name, "
            "for example e2e02."
        ),
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=PROJECT_ROOT / "local_data" / "minio_seed",
        help="Ignored staging directory used while generating each file.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Regenerate and replace objects that already exist in MinIO.",
    )
    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Keep generated local files after successful upload.",
    )
    args = parser.parse_args()

    if args.rows <= 0:
        parser.error("--rows must be greater than zero")
    if args.chunk_rows <= 0:
        parser.error("--chunk-rows must be greater than zero")
    if any(count <= 0 for count in args.column_counts):
        parser.error("--column-counts values must be greater than zero")
    args.batch_suffix = args.batch_suffix.strip()
    if args.batch_suffix and not SAFE_BATCH_SUFFIX.fullmatch(args.batch_suffix):
        parser.error(
            "--batch-suffix must start with an alphanumeric character and "
            "contain only letters, numbers, underscores, or hyphens"
        )

    return args


def aircraft_label(total_column_count: int) -> str:
    return f"MX{total_column_count}"


def object_exists(client: Minio, bucket: str, object_name: str) -> bool:
    try:
        client.stat_object(bucket, object_name)
        return True
    except S3Error as error:
        if error.code in {"NoSuchKey", "NoSuchObject", "NotFound"}:
            return False
        raise


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Created bucket: {bucket}", flush=True)


def generate_tab_file(
    output_path: Path,
    total_column_count: int,
    row_count: int,
    chunk_rows: int,
    seed: int,
) -> None:
    """Generate one deterministic, memory-bounded synthetic MX dataset."""

    if total_column_count < 3:
        raise ValueError("An MX dataset must contain at least three columns.")

    aircraft_type = aircraft_label(total_column_count)
    measurement_column_count = total_column_count - 2
    rng = np.random.default_rng(seed + total_column_count + row_count)

    float_count = round(measurement_column_count * 0.10)
    mixed_count = round(measurement_column_count * 0.10)
    zero_count = round(measurement_column_count * 0.40)
    one_count = measurement_column_count - float_count - mixed_count - zero_count

    float_columns = [f"f{index}" for index in range(float_count)]
    binary_names = (
        [f"m{index}" for index in range(mixed_count)]
        + [f"z{index}" for index in range(zero_count)]
        + [f"o{index}" for index in range(one_count)]
    )
    binary_types = np.array(
        ["mixed"] * mixed_count + ["zero"] * zero_count + ["one"] * one_count
    )

    permutation = rng.permutation(len(binary_names))
    shuffled_binary_names = [binary_names[index] for index in permutation]
    shuffled_binary_types = binary_types[permutation]
    mixed_positions = np.flatnonzero(shuffled_binary_types == "mixed")
    one_positions = np.flatnonzero(shuffled_binary_types == "one")

    columns = ["timestamp", "aircraft_type", *float_columns, *shuffled_binary_names]
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="\n") as output:
        output.write("\t".join(columns) + "\n")

        for start in range(0, row_count, chunk_rows):
            current_rows = min(chunk_rows, row_count - start)
            row_offsets = np.arange(start, start + current_rows)
            timestamps = DEFAULT_START_TIMESTAMP + pd.to_timedelta(
                row_offsets, unit="s"
            )
            timestamp_frame = pd.DataFrame(
                {
                    "timestamp": timestamps.strftime(OUTPUT_TIMESTAMP_FORMAT)
                }
            )
            aircraft_frame = pd.DataFrame(
                {"aircraft_type": [aircraft_type] * current_rows}
            )
            float_frame = pd.DataFrame(
                rng.normal(0, 100, size=(current_rows, float_count)).round(6),
                columns=float_columns,
            )

            binary_values = np.zeros(
                (current_rows, len(shuffled_binary_names)), dtype=np.uint8
            )
            binary_values[:, one_positions] = 1
            binary_values[:, mixed_positions] = rng.integers(
                0,
                2,
                size=(current_rows, len(mixed_positions)),
                dtype=np.uint8,
            )
            binary_frame = pd.DataFrame(
                binary_values, columns=shuffled_binary_names
            )

            frame = pd.concat(
                [timestamp_frame, aircraft_frame, float_frame, binary_frame],
                axis=1,
            )
            frame.to_csv(
                output,
                sep="\t",
                header=False,
                index=False,
                lineterminator="\n",
            )


def generate_upload_and_verify(
    client: Minio,
    bucket: str,
    prefix: str,
    work_dir: Path,
    total_column_count: int,
    row_count: int,
    chunk_rows: int,
    seed: int,
    batch_suffix: str,
    overwrite: bool,
    keep_local: bool,
) -> None:
    batch_id = f"mx{total_column_count}_{row_count}rows"
    if batch_suffix:
        batch_id = f"{batch_id}_{batch_suffix}"
    file_name = f"{batch_id}.tab"
    object_name = "/".join(part for part in (prefix.strip("/"), file_name) if part)

    if not overwrite and object_exists(client, bucket, object_name):
        existing = client.stat_object(bucket, object_name)
        print(
            f"Skipped existing object: s3://{bucket}/{object_name} "
            f"({existing.size:,} bytes)",
            flush=True,
        )
        return

    local_path = work_dir / file_name
    print(
        f"Generating {file_name}: {row_count:,} rows, "
        f"{total_column_count:,} total columns",
        flush=True,
    )
    generate_tab_file(
        output_path=local_path,
        total_column_count=total_column_count,
        row_count=row_count,
        chunk_rows=chunk_rows,
        seed=seed,
    )

    local_size = local_path.stat().st_size
    client.fput_object(
        bucket,
        object_name,
        str(local_path),
        content_type="text/tab-separated-values",
        metadata={
            "rows": str(row_count),
            "measurement-columns": str(total_column_count - 2),
            "total-columns": str(total_column_count),
            "aircraft-type": aircraft_label(total_column_count),
            "generator-seed": str(seed),
            "batch-id": batch_id,
        },
    )

    uploaded = client.stat_object(bucket, object_name)
    if uploaded.size != local_size:
        raise RuntimeError(
            f"Upload size mismatch for {object_name}: "
            f"local={local_size}, MinIO={uploaded.size}"
        )

    print(
        f"Verified: s3://{bucket}/{object_name} "
        f"({uploaded.size:,} bytes, etag={uploaded.etag})",
        flush=True,
    )
    if not keep_local:
        local_path.unlink()


def main() -> None:
    args = parse_args()
    endpoint, secure = _normalise_endpoint(args.endpoint, args.secure)
    client = Minio(
        endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        secure=secure,
    )

    ensure_bucket(client, args.bucket)
    args.work_dir.mkdir(parents=True, exist_ok=True)

    for total_column_count in args.column_counts:
        generate_upload_and_verify(
            client=client,
            bucket=args.bucket,
            prefix=args.prefix,
            work_dir=args.work_dir,
            total_column_count=total_column_count,
            row_count=args.rows,
            chunk_rows=args.chunk_rows,
            seed=args.seed,
            batch_suffix=args.batch_suffix,
            overwrite=args.overwrite,
            keep_local=args.keep_local,
        )

    print("All requested MinIO objects are ready and verified.", flush=True)


if __name__ == "__main__":
    main()
