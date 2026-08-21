"""Benchmark lakeFS versioning and restore operations against a MinIO blockstore."""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.error import URLError
from urllib.request import urlopen

import pyarrow.parquet as pq
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_V1 = (
    PROJECT_ROOT
    / "data"
    / "benchmark"
    / "source_versions"
    / "random_numbers_v1.parquet"
)
DEFAULT_V2 = (
    PROJECT_ROOT
    / "data"
    / "benchmark"
    / "source_versions"
    / "random_numbers_v2.parquet"
)
DEFAULT_LAKECTL = PROJECT_ROOT / ".venv" / "Scripts" / "lakectl.exe"
DEFAULT_RESULTS_DIR = PROJECT_ROOT / "reports" / "benchmarks"
DEFAULT_RUNS_DIR = PROJECT_ROOT / "tmp" / "lakefs_benchmark" / "runs"
BYTES_PER_MIB = 1024**2


def default_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}_{uuid.uuid4().hex[:8]}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure lakeFS V1/V2 upload and commit, zero-copy branch restore, "
            "metadata access and optional local materialization against MinIO."
        )
    )
    parser.add_argument("--v1", type=Path, default=DEFAULT_V1)
    parser.add_argument("--v2", type=Path, default=DEFAULT_V2)
    parser.add_argument("--lakectl", type=Path, default=DEFAULT_LAKECTL)
    parser.add_argument("--lakefs-endpoint", default="http://127.0.0.1:8002")
    parser.add_argument("--minio-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--bucket", default="lakefs-benchmark")
    parser.add_argument("--repository")
    parser.add_argument("--run-id", default=default_run_id())
    parser.add_argument("--runs-dir", type=Path, default=DEFAULT_RUNS_DIR)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip materializing restored V1 to local disk.",
    )
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def command_text(command: list[str]) -> str:
    return subprocess.list2cmdline(command)


def run_checked(
    command: list[str],
    cwd: Path,
    *,
    show_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    process = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if show_output and process.stdout:
        print(process.stdout.rstrip(), flush=True)
    if process.returncode != 0:
        raise RuntimeError(
            f"Command failed ({process.returncode}): {command_text(command)}\n"
            f"{process.stdout.rstrip()}"
        )
    return process


def parquet_info(path: Path) -> dict[str, Any]:
    parquet_file = pq.ParquetFile(path)
    schema_metadata = parquet_file.schema_arrow.metadata or {}
    return {
        "path": str(path),
        "size_bytes": path.stat().st_size,
        "rows": parquet_file.metadata.num_rows,
        "columns": parquet_file.metadata.num_columns,
        "row_groups": parquet_file.metadata.num_row_groups,
        "version": schema_metadata.get(b"benchmark.version", b"").decode(),
        "seed": schema_metadata.get(b"benchmark.seed", b"").decode(),
    }


def check_sources(v1: Path, v2: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    for path in (v1, v2):
        if not path.is_file():
            raise FileNotFoundError(f"Benchmark source was not found: {path}")

    v1_info = parquet_info(v1)
    v2_info = parquet_info(v2)
    comparable_keys = ("rows", "columns", "row_groups", "size_bytes")
    differences = [
        key for key in comparable_keys if v1_info[key] != v2_info[key]
    ]
    if differences:
        raise ValueError(
            "V1 and V2 are not structurally comparable: " + ", ".join(differences)
        )
    if v1_info["seed"] and v1_info["seed"] == v2_info["seed"]:
        raise ValueError("V1 and V2 use the same benchmark seed.")
    return v1_info, v2_info


def required_credentials() -> tuple[str, str, str, str]:
    lakefs_access_key = os.getenv("LAKEFS_ACCESS_KEY_ID")
    lakefs_secret_key = os.getenv("LAKEFS_SECRET_ACCESS_KEY")
    minio_access_key = os.getenv("MINIO_ROOT_USER") or os.getenv(
        "AWS_ACCESS_KEY_ID"
    )
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD") or os.getenv(
        "AWS_SECRET_ACCESS_KEY"
    )
    if not lakefs_access_key or not lakefs_secret_key:
        raise RuntimeError(
            "lakeFS credentials are missing. Set LAKEFS_ACCESS_KEY_ID and "
            "LAKEFS_SECRET_ACCESS_KEY."
        )
    if not minio_access_key or not minio_secret_key:
        raise RuntimeError(
            "MinIO credentials are missing. Set MINIO_ROOT_USER and "
            "MINIO_ROOT_PASSWORD."
        )
    return (
        lakefs_access_key,
        lakefs_secret_key,
        minio_access_key,
        minio_secret_key,
    )


def check_health(endpoint: str, path: str, service: str) -> None:
    health_url = f"{endpoint.rstrip('/')}{path}"
    try:
        with urlopen(health_url, timeout=5) as response:
            if response.status != 200:
                raise RuntimeError(
                    f"{service} health check returned HTTP {response.status}."
                )
    except URLError as error:
        raise RuntimeError(f"{service} is not reachable at {endpoint}.") from error


def ensure_bucket(
    endpoint: str,
    bucket: str,
    access_key: str,
    secret_key: str,
) -> None:
    try:
        from botocore.exceptions import ClientError
        from botocore.session import get_session
    except ImportError as error:
        raise RuntimeError(
            "MinIO dependencies are missing. Install the project's S3 support."
        ) from error

    client = get_session().create_client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = error.response.get("Error", {}).get("Code")
        if status == 404 or code in {"404", "NoSuchBucket", "NotFound"}:
            client.create_bucket(Bucket=bucket)
        else:
            raise


def write_lakectl_config(
    path: Path,
    endpoint: str,
    access_key: str,
    secret_key: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    config = {
        "server": {"endpoint_url": endpoint},
        "credentials": {
            "access_key_id": access_key,
            "secret_access_key": secret_key,
        },
    }
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")


def parse_commit_id(output: str) -> str:
    match = re.search(r"(?m)^ID:\s*([0-9a-f]{64})\s*$", output)
    if not match:
        raise RuntimeError("Could not parse lakeFS commit ID from lakectl output.")
    return match.group(1)


def parse_branch_commit(output: str) -> str:
    match = re.search(r"(?m)^Commit ID:\s*([0-9a-f]{64})\s*$", output)
    if not match:
        raise RuntimeError("Could not parse branch commit ID from lakectl output.")
    return match.group(1)


def parse_object_stat(output: str) -> dict[str, Any]:
    size_match = re.search(r"(?m)^Size:\s*(\d+) bytes\s*$", output)
    checksum_match = re.search(r"(?m)^Checksum:\s*(\S+)\s*$", output)
    if not size_match or not checksum_match:
        raise RuntimeError("Could not parse object metadata from lakectl fs stat.")
    return {
        "size_bytes": int(size_match.group(1)),
        "checksum": checksum_match.group(1),
    }


class Recorder:
    def __init__(self, run_id: str, metadata: dict[str, Any]) -> None:
        self.run_id = run_id
        self.metadata = metadata
        self.steps: list[dict[str, Any]] = []
        self.started_at = utc_now()
        self.wall_started = time.perf_counter()
        self.status = "running"
        self.error: str | None = None

    def measure(
        self,
        name: str,
        category: str,
        operation: Callable[[], Any],
        *,
        size_bytes: int | None = None,
        command: str | None = None,
    ) -> Any:
        print(f"[{name}] started", flush=True)
        started_at = utc_now()
        started = time.perf_counter()
        status = "success"
        error_message = None
        try:
            result = operation()
            return result
        except Exception as error:
            status = "failed"
            error_message = str(error)
            raise
        finally:
            duration = time.perf_counter() - started
            throughput = None
            if size_bytes is not None and duration > 0:
                throughput = size_bytes / BYTES_PER_MIB / duration
            self.steps.append(
                {
                    "name": name,
                    "category": category,
                    "started_at": started_at,
                    "duration_seconds": round(duration, 6),
                    "size_bytes": size_bytes,
                    "throughput_mib_s": (
                        round(throughput, 3) if throughput is not None else None
                    ),
                    "status": status,
                    "command": command,
                    "error": error_message,
                }
            )
            print(f"[{name}] {status} in {duration:.3f}s", flush=True)

    def duration(self, step_name: str) -> float:
        for step in self.steps:
            if step["name"] == step_name and step["status"] == "success":
                return float(step["duration_seconds"])
        return 0.0

    def totals(self) -> dict[str, float]:
        version_write = sum(
            self.duration(name)
            for name in (
                "upload_v1",
                "commit_v1",
                "upload_v2",
                "commit_v2",
            )
        )
        branch_restore = self.duration("create_restore_branch_v1")
        stat_restore = self.duration("stat_restored_v1")
        download_restore = self.duration("download_restored_v1")
        base = version_write + branch_restore
        return {
            "version_write_seconds": round(version_write, 6),
            "zero_copy_restore_seconds": round(branch_restore + stat_restore, 6),
            "materialized_restore_seconds": round(
                branch_restore + download_restore, 6
            ),
            "tool_flow_zero_copy_seconds": round(base + stat_restore, 6),
            "tool_flow_materialized_seconds": round(base + download_restore, 6),
            "measured_wall_clock_seconds": round(
                time.perf_counter() - self.wall_started, 6
            ),
        }

    def write(self, results_dir: Path) -> tuple[Path, Path, dict[str, float]]:
        results_dir.mkdir(parents=True, exist_ok=True)
        json_path = results_dir / f"lakefs_minio_benchmark_{self.run_id}.json"
        csv_path = results_dir / f"lakefs_minio_benchmark_{self.run_id}.csv"
        total_values = self.totals()

        payload = {
            "run_id": self.run_id,
            "status": self.status,
            "error": self.error,
            "started_at": self.started_at,
            "finished_at": utc_now(),
            "metadata": self.metadata,
            "steps": self.steps,
            "totals": total_values,
        }
        json_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        fieldnames = [
            "run_id",
            "name",
            "category",
            "started_at",
            "duration_seconds",
            "size_bytes",
            "throughput_mib_s",
            "status",
            "command",
            "error",
        ]
        with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for step in self.steps:
                writer.writerow({"run_id": self.run_id, **step})
            for total_name, duration in total_values.items():
                writer.writerow(
                    {
                        "run_id": self.run_id,
                        "name": total_name,
                        "category": "total",
                        "duration_seconds": duration,
                        "status": self.status,
                    }
                )
        return json_path, csv_path, total_values


def timed_command(
    recorder: Recorder,
    name: str,
    category: str,
    command: list[str],
    cwd: Path,
    *,
    size_bytes: int | None = None,
) -> subprocess.CompletedProcess[str]:
    return recorder.measure(
        name,
        category,
        lambda: run_checked(command, cwd, show_output=True),
        size_bytes=size_bytes,
        command=command_text(command),
    )


def main() -> None:
    args = parse_args()
    v1 = args.v1.resolve()
    v2 = args.v2.resolve()
    lakectl_path = args.lakectl.resolve()
    results_dir = args.results_dir.resolve()
    run_root = args.runs_dir.resolve() / args.run_id
    config_path = run_root / "lakectl.yaml"
    download_path = run_root / "downloads" / "random_numbers_v1.parquet"
    repository = args.repository or f"benchmark-{args.run_id.lower().replace('_', '-')}"
    branch_uri = f"lakefs://{repository}/main"
    restore_branch_uri = f"lakefs://{repository}/restore-v1"
    object_path = "data/random_numbers.parquet"
    main_object_uri = f"{branch_uri}/{object_path}"
    restore_object_uri = f"{restore_branch_uri}/{object_path}"
    storage_namespace = f"s3://{args.bucket}/runs/{args.run_id}"

    if run_root.exists():
        raise FileExistsError(f"Benchmark run directory already exists: {run_root}")
    if not lakectl_path.is_file():
        raise FileNotFoundError(f"lakectl executable was not found: {lakectl_path}")

    v1_info, v2_info = check_sources(v1, v2)
    (
        lakefs_access_key,
        lakefs_secret_key,
        minio_access_key,
        minio_secret_key,
    ) = required_credentials()
    check_health(args.lakefs_endpoint, "/_health", "lakeFS")
    check_health(args.minio_endpoint, "/minio/health/live", "MinIO")
    ensure_bucket(
        args.minio_endpoint,
        args.bucket,
        minio_access_key,
        minio_secret_key,
    )
    write_lakectl_config(
        config_path,
        args.lakefs_endpoint,
        lakefs_access_key,
        lakefs_secret_key,
    )

    lakectl = str(lakectl_path)
    base_command = [lakectl, "--config", str(config_path)]
    lakefs_version = run_checked(
        [lakectl, "--version"], PROJECT_ROOT
    ).stdout.strip()
    metadata = {
        "platform": platform.platform(),
        "python_version": sys.version.split()[0],
        "lakefs_version": lakefs_version,
        "lakefs_endpoint": args.lakefs_endpoint,
        "minio_endpoint": args.minio_endpoint,
        "bucket": args.bucket,
        "storage_namespace": storage_namespace,
        "repository": repository,
        "object_path": object_path,
        "run_directory": str(run_root),
        "v1": v1_info,
        "v2": v2_info,
        "download_skipped": args.skip_download,
    }
    recorder = Recorder(args.run_id, metadata)

    try:
        timed_command(
            recorder,
            "create_repository",
            "benchmark_setup",
            base_command
            + ["repo", "create", f"lakefs://{repository}", storage_namespace],
            PROJECT_ROOT,
        )

        timed_command(
            recorder,
            "upload_v1",
            "lakefs_upload",
            base_command
            + [
                "fs",
                "upload",
                main_object_uri,
                "--source",
                str(v1),
                "--no-progress",
            ],
            PROJECT_ROOT,
            size_bytes=v1_info["size_bytes"],
        )
        commit_v1_result = timed_command(
            recorder,
            "commit_v1",
            "lakefs_commit",
            base_command
            + [
                "commit",
                branch_uri,
                "-m",
                "data(benchmark): add parquet v1",
                "--meta",
                "benchmark.version=v1",
                "--meta",
                f"benchmark.seed={v1_info['seed']}",
            ],
            PROJECT_ROOT,
        )
        v1_commit = parse_commit_id(commit_v1_result.stdout)
        v1_stat_result = run_checked(
            base_command + ["fs", "stat", main_object_uri, "--pre-sign=false"],
            PROJECT_ROOT,
        )
        v1_stat = parse_object_stat(v1_stat_result.stdout)

        timed_command(
            recorder,
            "upload_v2",
            "lakefs_upload",
            base_command
            + [
                "fs",
                "upload",
                main_object_uri,
                "--source",
                str(v2),
                "--no-progress",
            ],
            PROJECT_ROOT,
            size_bytes=v2_info["size_bytes"],
        )
        commit_v2_result = timed_command(
            recorder,
            "commit_v2",
            "lakefs_commit",
            base_command
            + [
                "commit",
                branch_uri,
                "-m",
                "data(benchmark): add parquet v2",
                "--meta",
                "benchmark.version=v2",
                "--meta",
                f"benchmark.seed={v2_info['seed']}",
            ],
            PROJECT_ROOT,
        )
        v2_commit = parse_commit_id(commit_v2_result.stdout)
        v2_stat_result = run_checked(
            base_command + ["fs", "stat", main_object_uri, "--pre-sign=false"],
            PROJECT_ROOT,
        )
        v2_stat = parse_object_stat(v2_stat_result.stdout)
        if v1_stat["checksum"] == v2_stat["checksum"]:
            raise RuntimeError("lakeFS V1 and V2 object checksums are identical.")

        timed_command(
            recorder,
            "create_restore_branch_v1",
            "lakefs_zero_copy_restore",
            base_command
            + [
                "branch",
                "create",
                restore_branch_uri,
                "--source",
                f"lakefs://{repository}/{v1_commit}",
            ],
            PROJECT_ROOT,
        )
        branch_result = run_checked(
            base_command + ["branch", "show", restore_branch_uri],
            PROJECT_ROOT,
        )
        if parse_branch_commit(branch_result.stdout) != v1_commit:
            raise RuntimeError("Restore branch does not point to the V1 commit.")

        restored_stat_result = timed_command(
            recorder,
            "stat_restored_v1",
            "lakefs_direct_read",
            base_command
            + ["fs", "stat", restore_object_uri, "--pre-sign=false"],
            PROJECT_ROOT,
        )
        restored_stat = parse_object_stat(restored_stat_result.stdout)
        if restored_stat != v1_stat:
            raise RuntimeError("Restored V1 object metadata does not match V1.")

        if not args.skip_download:
            download_path.parent.mkdir(parents=True, exist_ok=True)
            timed_command(
                recorder,
                "download_restored_v1",
                "lakefs_materialized_read",
                base_command
                + [
                    "fs",
                    "download",
                    restore_object_uri,
                    str(download_path),
                    "--no-progress",
                ],
                PROJECT_ROOT,
                size_bytes=v1_info["size_bytes"],
            )
            downloaded_info = recorder.measure(
                "validate_downloaded_v1",
                "validation",
                lambda: parquet_info(download_path),
            )
            if downloaded_info["seed"] != v1_info["seed"]:
                raise RuntimeError("Downloaded object is not benchmark Parquet V1.")

        recorder.metadata.update(
            {
                "v1_commit": v1_commit,
                "v2_commit": v2_commit,
                "v1_object": v1_stat,
                "v2_object": v2_stat,
                "restore_branch": "restore-v1",
            }
        )
        recorder.status = "success"
    except Exception as error:
        recorder.status = "failed"
        recorder.error = str(error)
        raise
    finally:
        json_path, csv_path, total_values = recorder.write(results_dir)
        print(f"JSON results: {json_path}", flush=True)
        print(f"CSV results:  {csv_path}", flush=True)
        print(json.dumps(total_values, indent=2), flush=True)


if __name__ == "__main__":
    main()
