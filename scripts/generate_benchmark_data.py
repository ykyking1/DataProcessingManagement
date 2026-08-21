"""Generate deterministic Parquet versions for DVC and lakeFS benchmarks."""

from __future__ import annotations

import argparse
import gc
import math
import os
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "benchmark" / "source_versions"
BYTES_PER_GIB = 1024**3
BYTES_PER_MIB = 1024**2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create deterministic, uncompressed Parquet files containing random "
            "float64 values for DVC and lakeFS storage benchmarks."
        )
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--size-gb",
        type=float,
        default=10.0,
        help="Minimum raw numeric data size per version in GiB (default: 10).",
    )
    parser.add_argument(
        "--versions",
        type=int,
        default=2,
        help="Number of dataset versions to generate (default: 2).",
    )
    parser.add_argument(
        "--columns",
        type=int,
        default=16,
        help="Number of float64 columns (default: 16).",
    )
    parser.add_argument(
        "--seed-start",
        type=int,
        default=20260821,
        help="Seed for v1; each following version increments it by one.",
    )
    parser.add_argument(
        "--row-group-mib",
        type=int,
        default=128,
        help="Approximate uncompressed row-group size in MiB (default: 128).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing generated versions and interrupted .part files.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.size_gb <= 0:
        raise ValueError("--size-gb must be greater than zero.")
    if args.versions <= 0:
        raise ValueError("--versions must be greater than zero.")
    if args.columns <= 0:
        raise ValueError("--columns must be greater than zero.")
    if args.row_group_mib <= 0:
        raise ValueError("--row-group-mib must be greater than zero.")


def output_paths(output_dir: Path, versions: int) -> list[Path]:
    return [
        output_dir / f"random_numbers_v{version}.parquet"
        for version in range(1, versions + 1)
    ]


def finalize_file(temporary_path: Path, output_path: Path) -> None:
    for attempt in range(30):
        try:
            os.replace(temporary_path, output_path)
            return
        except PermissionError:
            if attempt == 29:
                raise
            time.sleep(1)


def generate_version(
    output_path: Path,
    version: int,
    seed: int,
    size_gib: float,
    columns: int,
    row_group_mib: int,
) -> tuple[int, int, float]:
    temporary_path = output_path.with_suffix(f"{output_path.suffix}.part")
    dtype = np.dtype("float64")
    row_bytes = columns * dtype.itemsize
    target_bytes = math.ceil(size_gib * BYTES_PER_GIB)
    row_count = math.ceil(target_bytes / row_bytes)
    raw_data_bytes = row_count * row_bytes
    row_group_rows = max(1, row_group_mib * BYTES_PER_MIB // row_bytes)

    fields = [
        pa.field(f"feature_{index:02d}", pa.float64())
        for index in range(1, columns + 1)
    ]
    metadata = {
        b"benchmark.version": str(version).encode(),
        b"benchmark.seed": str(seed).encode(),
        b"benchmark.target_gib": f"{size_gib:.3f}".encode(),
    }
    schema = pa.schema(fields, metadata=metadata)
    random_generator = np.random.default_rng(seed)

    print(
        f"v{version}: {output_path} | seed={seed} | "
        f"shape={row_count:,}x{columns} | target={size_gib:.2f} GiB",
        flush=True,
    )

    started_at = time.perf_counter()
    next_progress_bytes = BYTES_PER_GIB
    writer = pq.ParquetWriter(
        temporary_path,
        schema,
        compression="NONE",
        use_dictionary=False,
        write_statistics=False,
        data_page_version="2.0",
    )

    try:
        for start_row in range(0, row_count, row_group_rows):
            current_rows = min(row_group_rows, row_count - start_row)
            values = random_generator.random(
                (columns, current_rows), dtype=np.float64
            )
            arrays = [
                pa.array(values[index], type=pa.float64())
                for index in range(columns)
            ]
            table = pa.Table.from_arrays(arrays, schema=schema)
            writer.write_table(table, row_group_size=current_rows)

            completed_rows = start_row + current_rows
            completed_bytes = completed_rows * row_bytes
            if completed_bytes >= next_progress_bytes or completed_rows == row_count:
                elapsed = time.perf_counter() - started_at
                throughput = completed_bytes / BYTES_PER_MIB / elapsed
                print(
                    f"v{version}: generated {completed_bytes / BYTES_PER_GIB:.2f}/"
                    f"{raw_data_bytes / BYTES_PER_GIB:.2f} GiB "
                    f"({throughput:.1f} MiB/s)",
                    flush=True,
                )
                next_progress_bytes += BYTES_PER_GIB

            del table, arrays, values
    finally:
        writer.close()

    gc.collect()
    finalize_file(temporary_path, output_path)

    elapsed = time.perf_counter() - started_at
    actual_size = output_path.stat().st_size
    parquet_file = pq.ParquetFile(output_path)
    if parquet_file.metadata.num_rows != row_count:
        raise RuntimeError(f"Unexpected row count in {output_path}.")
    if parquet_file.metadata.num_columns != columns:
        raise RuntimeError(f"Unexpected column count in {output_path}.")
    if actual_size < target_bytes:
        raise RuntimeError(
            f"Generated file is smaller than requested: "
            f"{actual_size / BYTES_PER_GIB:.3f} GiB"
        )

    print(
        f"v{version}: completed {actual_size / BYTES_PER_GIB:.3f} GiB in "
        f"{elapsed:.2f}s ({actual_size / BYTES_PER_MIB / elapsed:.1f} MiB/s)",
        flush=True,
    )
    return row_count, actual_size, elapsed


def main() -> None:
    args = parse_args()
    validate_args(args)

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = output_paths(output_dir, args.versions)

    for path in paths:
        temporary_path = path.with_suffix(f"{path.suffix}.part")
        existing = [
            candidate
            for candidate in (path, temporary_path)
            if candidate.exists()
        ]
        if existing and not args.overwrite:
            names = ", ".join(str(candidate) for candidate in existing)
            raise FileExistsError(f"Output already exists: {names}. Use --overwrite.")
        if args.overwrite and temporary_path.exists():
            temporary_path.unlink()

    results = []
    for version, path in enumerate(paths, start=1):
        results.append(
            generate_version(
                output_path=path,
                version=version,
                seed=args.seed_start + version - 1,
                size_gib=args.size_gb,
                columns=args.columns,
                row_group_mib=args.row_group_mib,
            )
        )

    print("\nGenerated benchmark versions:", flush=True)
    for version, (path, result) in enumerate(zip(paths, results), start=1):
        rows, size_bytes, elapsed = result
        print(
            f"v{version}: {path.name} | {rows:,} rows | "
            f"{size_bytes / BYTES_PER_GIB:.3f} GiB | {elapsed:.2f}s",
            flush=True,
        )


if __name__ == "__main__":
    main()
