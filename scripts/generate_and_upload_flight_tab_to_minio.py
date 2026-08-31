"""Generate deterministic dashboard-ready flight telemetry and upload it to MinIO.

The generated TAB schema is the contract consumed by the Streamlit dashboard:

``time, latitude, longitude, altitude, velocity_x, velocity_y, velocity_z,
roll, pitch, yaw, image_name, box_x, box_y, box_w, box_h, class, flight_id``

Files are produced one at a time in the ignored ``local_data`` directory. The
local file is removed only after MinIO confirms the uploaded object size.
Existing objects are skipped unless ``--overwrite`` is supplied.
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
DATASET_ID = "flightdemo"
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
DEFAULT_FLIGHT_COUNT = 6
DEFAULT_ROWS_PER_FLIGHT = 1_000
DEFAULT_INTERVAL_SECONDS = 10
DEFAULT_SEED = 42
DEFAULT_START_TIMESTAMP = pd.Timestamp("2026-08-27T05:00:00Z")
OUTPUT_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"
SAFE_BATCH_SUFFIX = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
ROUTE_CENTERS = (
    ("ERZ", 39.9043, 41.2679),
    ("ANK", 39.9334, 32.8597),
    ("IST", 41.0082, 28.9784),
    ("IZM", 38.4237, 27.1428),
    ("KON", 37.8746, 32.4932),
    ("ANT", 36.8969, 30.7133),
)
DETECTION_CLASSES = np.array(["car", "truck", "person", "van"])


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _normalise_endpoint(endpoint: str, secure: bool) -> tuple[str, bool]:
    if "://" not in endpoint:
        return endpoint.rstrip("/"), secure

    parsed = urlparse(endpoint)
    if not parsed.netloc:
        raise ValueError(f"Invalid MinIO endpoint: {endpoint}")
    return parsed.netloc, parsed.scheme.lower() == "https"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate dashboard-ready flight telemetry and upload it to MinIO."
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
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("MINIO_RAW_BUCKET", "data-raw"),
    )
    parser.add_argument(
        "--prefix",
        default=os.getenv("MINIO_RAW_PREFIX", "flight-tab/inbox"),
    )
    parser.add_argument("--flights", type=int, default=DEFAULT_FLIGHT_COUNT)
    parser.add_argument(
        "--rows-per-flight", type=int, default=DEFAULT_ROWS_PER_FLIGHT
    )
    parser.add_argument(
        "--interval-seconds", type=int, default=DEFAULT_INTERVAL_SECONDS
    )
    parser.add_argument("--chunk-rows", type=int, default=5_000)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument(
        "--batch-suffix",
        default="",
        help="Optional unique suffix, for example demo01.",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=PROJECT_ROOT / "local_data" / "minio_seed",
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--keep-local", action="store_true")
    args = parser.parse_args()

    if args.flights <= 0:
        parser.error("--flights must be greater than zero")
    if args.rows_per_flight <= 0:
        parser.error("--rows-per-flight must be greater than zero")
    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be greater than zero")
    if args.chunk_rows <= 0:
        parser.error("--chunk-rows must be greater than zero")
    args.batch_suffix = args.batch_suffix.strip()
    if args.batch_suffix and not SAFE_BATCH_SUFFIX.fullmatch(args.batch_suffix):
        parser.error(
            "--batch-suffix must start with an alphanumeric character and "
            "contain only letters, numbers, underscores, or hyphens"
        )
    return args


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


def _flight_chunk(
    *,
    rng: np.random.Generator,
    batch_id: str,
    batch_token: str,
    flight_index: int,
    start_row: int,
    row_count: int,
    rows_per_flight: int,
    interval_seconds: int,
) -> pd.DataFrame:
    route_code, center_latitude, center_longitude = ROUTE_CENTERS[
        flight_index % len(ROUTE_CENTERS)
    ]
    positions = np.arange(start_row, start_row + row_count)
    denominator = max(rows_per_flight - 1, 1)
    progress = positions / denominator
    phase = flight_index * 0.65

    flight_start = DEFAULT_START_TIMESTAMP + pd.Timedelta(
        hours=flight_index * 7,
        days=flight_index // len(ROUTE_CENTERS),
    )
    timestamps = flight_start + pd.to_timedelta(
        positions * interval_seconds,
        unit="s",
    )

    latitude = (
        center_latitude
        + 0.22 * (progress - 0.5)
        + 0.018 * np.sin(progress * 4 * np.pi + phase)
    )
    longitude = (
        center_longitude
        + 0.32 * (progress - 0.5)
        + 0.024 * np.cos(progress * 3 * np.pi + phase)
    )
    altitude = (
        850
        + flight_index * 140
        + 1_100 * np.sin(np.pi * progress)
        + rng.normal(0, 8, row_count)
    )
    velocity_x = 52 + 9 * np.sin(progress * 2 * np.pi + phase)
    velocity_y = 38 + 7 * np.cos(progress * 2 * np.pi + phase)
    velocity_z = 4.5 * np.sin(progress * 4 * np.pi)
    roll = 8 * np.sin(progress * 5 * np.pi + phase)
    pitch = 5 * np.cos(progress * 4 * np.pi + phase)
    yaw = np.mod(35 + flight_index * 41 + progress * 230, 360)

    box_x = 120 + np.mod(positions * 7 + flight_index * 83, 1_420)
    box_y = 80 + np.mod(positions * 5 + flight_index * 59, 760)
    box_w = 80 + np.mod(positions * 3 + flight_index * 17, 220)
    box_h = 60 + np.mod(positions * 2 + flight_index * 23, 180)
    class_indexes = rng.integers(0, len(DETECTION_CLASSES), size=row_count)
    flight_id = f"{route_code}-{batch_token}-{flight_index + 1:02d}"

    frame = pd.DataFrame(
        {
            "time": timestamps.strftime(OUTPUT_TIMESTAMP_FORMAT),
            "latitude": latitude.round(6),
            "longitude": longitude.round(6),
            "altitude": altitude.round(3),
            "velocity_x": velocity_x.round(3),
            "velocity_y": velocity_y.round(3),
            "velocity_z": velocity_z.round(3),
            "roll": roll.round(3),
            "pitch": pitch.round(3),
            "yaw": yaw.round(3),
            "image_name": [
                f"{batch_id}_{flight_index + 1:02d}_{row_number:06d}.jpg"
                for row_number in positions
            ],
            "box_x": box_x.astype(float),
            "box_y": box_y.astype(float),
            "box_w": box_w.astype(float),
            "box_h": box_h.astype(float),
            "class": DETECTION_CLASSES[class_indexes],
            "flight_id": [flight_id] * row_count,
        },
        columns=FLIGHT_COLUMNS,
    )
    return frame


def generate_tab_file(
    output_path: Path,
    *,
    batch_id: str,
    batch_suffix: str,
    flight_count: int,
    rows_per_flight: int,
    interval_seconds: int,
    chunk_rows: int,
    seed: int,
) -> None:
    """Write deterministic, memory-bounded flight telemetry."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    batch_token = batch_suffix or "base"
    with output_path.open("w", encoding="utf-8", newline="\n") as output:
        output.write("\t".join(FLIGHT_COLUMNS) + "\n")
        for flight_index in range(flight_count):
            rng = np.random.default_rng(seed + flight_index * 10_007)
            for start_row in range(0, rows_per_flight, chunk_rows):
                current_rows = min(chunk_rows, rows_per_flight - start_row)
                frame = _flight_chunk(
                    rng=rng,
                    batch_id=batch_id,
                    batch_token=batch_token,
                    flight_index=flight_index,
                    start_row=start_row,
                    row_count=current_rows,
                    rows_per_flight=rows_per_flight,
                    interval_seconds=interval_seconds,
                )
                frame.to_csv(
                    output,
                    sep="\t",
                    header=False,
                    index=False,
                    lineterminator="\n",
                )


def generate_upload_and_verify(client: Minio, args: argparse.Namespace) -> None:
    total_rows = args.flights * args.rows_per_flight
    batch_id = f"{DATASET_ID}_{total_rows}rows"
    if args.batch_suffix:
        batch_id = f"{batch_id}_{args.batch_suffix}"
    file_name = f"{batch_id}.tab"
    object_name = "/".join(
        part for part in (args.prefix.strip("/"), file_name) if part
    )

    if not args.overwrite and object_exists(client, args.bucket, object_name):
        existing = client.stat_object(args.bucket, object_name)
        print(
            f"Skipped existing object: s3://{args.bucket}/{object_name} "
            f"({existing.size:,} bytes)",
            flush=True,
        )
        return

    local_path = args.work_dir / file_name
    print(
        f"Generating {file_name}: {args.flights} flights, "
        f"{total_rows:,} rows, {len(FLIGHT_COLUMNS)} columns",
        flush=True,
    )
    generate_tab_file(
        local_path,
        batch_id=batch_id,
        batch_suffix=args.batch_suffix,
        flight_count=args.flights,
        rows_per_flight=args.rows_per_flight,
        interval_seconds=args.interval_seconds,
        chunk_rows=args.chunk_rows,
        seed=args.seed,
    )

    local_size = local_path.stat().st_size
    client.fput_object(
        args.bucket,
        object_name,
        str(local_path),
        content_type="text/tab-separated-values",
        metadata={
            "dataset-id": DATASET_ID,
            "batch-id": batch_id,
            "flights": str(args.flights),
            "rows-per-flight": str(args.rows_per_flight),
            "rows": str(total_rows),
            "total-columns": str(len(FLIGHT_COLUMNS)),
            "generator-seed": str(args.seed),
        },
    )
    uploaded = client.stat_object(args.bucket, object_name)
    if uploaded.size != local_size:
        raise RuntimeError(
            f"Upload size mismatch for {object_name}: "
            f"local={local_size}, MinIO={uploaded.size}"
        )

    print(
        f"Verified: s3://{args.bucket}/{object_name} "
        f"({uploaded.size:,} bytes, etag={uploaded.etag})",
        flush=True,
    )
    if not args.keep_local:
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
    generate_upload_and_verify(client, args)
    print("Flight telemetry MinIO object is ready and verified.", flush=True)


if __name__ == "__main__":
    main()
