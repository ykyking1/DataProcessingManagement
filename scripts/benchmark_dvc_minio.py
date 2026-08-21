"""Benchmark DVC versioning and restore operations against a MinIO remote."""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import shutil
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
DEFAULT_DVC = PROJECT_ROOT / ".venv" / "Scripts" / "dvc.exe"
DEFAULT_RESULTS_DIR = PROJECT_ROOT / "reports" / "benchmarks"
DEFAULT_RUNS_DIR = PROJECT_ROOT / "tmp" / "dvc_benchmark"
BYTES_PER_MIB = 1024**2


def default_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}_{uuid.uuid4().hex[:8]}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure DVC add, MinIO push, pointer commit, warm checkout and "
            "cold pull times for two Parquet versions."
        )
    )
    parser.add_argument("--v1", type=Path, default=DEFAULT_V1)
    parser.add_argument("--v2", type=Path, default=DEFAULT_V2)
    parser.add_argument("--dvc", type=Path, default=DEFAULT_DVC)
    parser.add_argument("--git", default="git")
    parser.add_argument("--minio-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--bucket", default="dvc-benchmark")
    parser.add_argument("--remote-name", default="benchmark")
    parser.add_argument("--run-id", default=default_run_id())
    parser.add_argument("--runs-dir", type=Path, default=DEFAULT_RUNS_DIR)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
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
    comparable_keys = ("rows", "columns", "row_groups")
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


def replace_with_copy(source: Path, destination: Path) -> None:
    """Replace a tracked workspace file without overwriting a DVC cache link."""
    destination.unlink(missing_ok=True)
    shutil.copy2(source, destination)


def minio_credentials() -> tuple[str, str]:
    access_key = os.getenv("MINIO_ROOT_USER") or os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD") or os.getenv(
        "AWS_SECRET_ACCESS_KEY"
    )
    if not access_key or not secret_key:
        raise RuntimeError(
            "MinIO credentials are missing. Set MINIO_ROOT_USER and "
            "MINIO_ROOT_PASSWORD before running the benchmark."
        )
    return access_key, secret_key


def check_minio_health(endpoint: str) -> None:
    health_url = f"{endpoint.rstrip('/')}/minio/health/live"
    try:
        with urlopen(health_url, timeout=5) as response:
            if response.status != 200:
                raise RuntimeError(
                    f"MinIO health check returned HTTP {response.status}."
                )
    except URLError as error:
        raise RuntimeError(f"MinIO is not reachable at {endpoint}.") from error


def ensure_bucket(
    endpoint: str,
    bucket: str,
    access_key: str,
    secret_key: str,
) -> None:
    try:
        from botocore.session import get_session
        from botocore.exceptions import ClientError
    except ImportError as error:
        raise RuntimeError(
            "DVC S3 dependencies are missing. Install the project's S3 support "
            "before running this benchmark."
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
        base_steps = [
            "dvc_add_v1",
            "dvc_push_v1",
            "git_commit_v1",
            "dvc_add_v2",
            "dvc_push_v2",
            "git_commit_v2",
            "restore_v1_pointer",
        ]
        base = sum(self.duration(name) for name in base_steps)
        staging = self.duration("stage_v1") + self.duration("stage_v2")
        return {
            "dvc_git_base_seconds": round(base, 6),
            "tool_flow_warm_seconds": round(
                base + self.duration("warm_checkout_v1"), 6
            ),
            "tool_flow_cold_seconds": round(
                base + self.duration("cold_pull_v1"), 6
            ),
            "end_to_end_warm_with_staging_seconds": round(
                staging + base + self.duration("warm_checkout_v1"), 6
            ),
            "end_to_end_cold_with_staging_seconds": round(
                staging + base + self.duration("cold_pull_v1"), 6
            ),
            "measured_wall_clock_seconds": round(
                time.perf_counter() - self.wall_started, 6
            ),
        }

    def write(self, results_dir: Path) -> tuple[Path, Path, dict[str, float]]:
        results_dir.mkdir(parents=True, exist_ok=True)
        json_path = results_dir / f"dvc_minio_benchmark_{self.run_id}.json"
        csv_path = results_dir / f"dvc_minio_benchmark_{self.run_id}.csv"
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


def configure_repository(
    repo_dir: Path,
    cache_dir: Path,
    dvc: str,
    git: str,
    remote_name: str,
    remote_url: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
) -> None:
    repo_dir.mkdir(parents=True)
    run_checked([git, "init", "-b", "main"], repo_dir)
    run_checked([git, "config", "user.name", "DVC Benchmark"], repo_dir)
    run_checked(
        [git, "config", "user.email", "dvc-benchmark@localhost"], repo_dir
    )
    run_checked([dvc, "init", "-q", "--subdir"], repo_dir)
    run_checked([dvc, "config", "--local", "cache.dir", str(cache_dir)], repo_dir)
    run_checked(
        [dvc, "remote", "add", "--local", "-d", remote_name, remote_url],
        repo_dir,
    )
    run_checked(
        [
            dvc,
            "remote",
            "modify",
            "--local",
            remote_name,
            "endpointurl",
            endpoint,
        ],
        repo_dir,
    )
    run_checked(
        [
            dvc,
            "remote",
            "modify",
            "--local",
            remote_name,
            "access_key_id",
            access_key,
        ],
        repo_dir,
    )
    run_checked(
        [
            dvc,
            "remote",
            "modify",
            "--local",
            remote_name,
            "secret_access_key",
            secret_key,
        ],
        repo_dir,
    )
    run_checked([git, "add", "-A"], repo_dir)
    run_checked(
        [git, "commit", "-q", "-m", "chore: initialize benchmark repository"],
        repo_dir,
    )


def main() -> None:
    args = parse_args()
    v1 = args.v1.resolve()
    v2 = args.v2.resolve()
    dvc_path = args.dvc.resolve()
    results_dir = args.results_dir.resolve()
    run_root = args.runs_dir.resolve() / args.run_id
    repo_dir = run_root / "repo"
    cache_dir = run_root / "cache"
    warm_cache_backup = run_root / "warm_cache_backup"
    tracked_data = repo_dir / "data" / "random_numbers.parquet"
    pointer = Path("data/random_numbers.parquet.dvc")
    pointer_path = repo_dir / pointer
    remote_prefix = f"runs/{args.run_id}"
    remote_url = f"s3://{args.bucket}/{remote_prefix}"

    if run_root.exists():
        raise FileExistsError(f"Benchmark run directory already exists: {run_root}")
    if not dvc_path.is_file():
        raise FileNotFoundError(f"DVC executable was not found: {dvc_path}")

    v1_info, v2_info = check_sources(v1, v2)
    access_key, secret_key = minio_credentials()
    dvc_version = run_checked([str(dvc_path), "--version"], PROJECT_ROOT).stdout.strip()
    metadata = {
        "platform": platform.platform(),
        "python_version": sys.version.split()[0],
        "dvc_version": dvc_version,
        "minio_endpoint": args.minio_endpoint,
        "bucket": args.bucket,
        "remote_prefix": remote_prefix,
        "benchmark_repo": str(repo_dir),
        "v1": v1_info,
        "v2": v2_info,
    }
    recorder = Recorder(args.run_id, metadata)

    try:
        check_minio_health(args.minio_endpoint)
        ensure_bucket(
            args.minio_endpoint,
            args.bucket,
            access_key,
            secret_key,
        )
        configure_repository(
            repo_dir=repo_dir,
            cache_dir=cache_dir,
            dvc=str(dvc_path),
            git=args.git,
            remote_name=args.remote_name,
            remote_url=remote_url,
            endpoint=args.minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
        )
        tracked_data.parent.mkdir(parents=True, exist_ok=True)

        recorder.measure(
            "stage_v1",
            "staging",
            lambda: replace_with_copy(v1, tracked_data),
            size_bytes=v1_info["size_bytes"],
        )
        timed_command(
            recorder,
            "dvc_add_v1",
            "dvc",
            [str(dvc_path), "add", "-q", tracked_data.relative_to(repo_dir).as_posix()],
            repo_dir,
            size_bytes=v1_info["size_bytes"],
        )
        timed_command(
            recorder,
            "dvc_push_v1",
            "dvc_remote",
            [str(dvc_path), "push", "-q", "-r", args.remote_name, pointer.as_posix()],
            repo_dir,
            size_bytes=v1_info["size_bytes"],
        )

        def commit_v1() -> None:
            run_checked([args.git, "add", "-A"], repo_dir)
            run_checked(
                [args.git, "commit", "-q", "-m", "data(benchmark): add parquet v1"],
                repo_dir,
            )

        recorder.measure("git_commit_v1", "git", commit_v1)
        v1_commit = run_checked(
            [args.git, "rev-parse", "HEAD"], repo_dir
        ).stdout.strip()

        recorder.measure(
            "stage_v2",
            "staging",
            lambda: replace_with_copy(v2, tracked_data),
            size_bytes=v2_info["size_bytes"],
        )
        timed_command(
            recorder,
            "dvc_add_v2",
            "dvc",
            [str(dvc_path), "add", "-q", tracked_data.relative_to(repo_dir).as_posix()],
            repo_dir,
            size_bytes=v2_info["size_bytes"],
        )
        timed_command(
            recorder,
            "dvc_push_v2",
            "dvc_remote",
            [str(dvc_path), "push", "-q", "-r", args.remote_name, pointer.as_posix()],
            repo_dir,
            size_bytes=v2_info["size_bytes"],
        )

        def commit_v2() -> None:
            run_checked([args.git, "add", "-A"], repo_dir)
            run_checked(
                [args.git, "commit", "-q", "-m", "data(benchmark): add parquet v2"],
                repo_dir,
            )

        recorder.measure("git_commit_v2", "git", commit_v2)
        v2_commit = run_checked(
            [args.git, "rev-parse", "HEAD"], repo_dir
        ).stdout.strip()

        timed_command(
            recorder,
            "restore_v1_pointer",
            "git",
            [args.git, "restore", "--source", v1_commit, "--", pointer.as_posix()],
            repo_dir,
        )
        timed_command(
            recorder,
            "warm_checkout_v1",
            "dvc_restore_warm",
            [str(dvc_path), "checkout", "-q", "--force", pointer.as_posix()],
            repo_dir,
            size_bytes=v1_info["size_bytes"],
        )
        warm_info = parquet_info(tracked_data)
        if warm_info["seed"] != v1_info["seed"]:
            raise RuntimeError("Warm checkout did not restore Parquet v1.")

        def prepare_cold_cache() -> None:
            if cache_dir.exists():
                shutil.move(str(cache_dir), str(warm_cache_backup))
            cache_dir.mkdir(parents=True, exist_ok=True)
            if tracked_data.exists():
                tracked_data.unlink()

        recorder.measure(
            "prepare_cold_cache",
            "benchmark_setup",
            prepare_cold_cache,
        )
        timed_command(
            recorder,
            "cold_pull_v1",
            "dvc_restore_cold",
            [
                str(dvc_path),
                "pull",
                "-q",
                "--force",
                "-r",
                args.remote_name,
                pointer.as_posix(),
            ],
            repo_dir,
            size_bytes=v1_info["size_bytes"],
        )
        cold_info = parquet_info(tracked_data)
        if cold_info["seed"] != v1_info["seed"]:
            raise RuntimeError("Cold pull did not restore Parquet v1.")

        recorder.metadata["v1_commit"] = v1_commit
        recorder.metadata["v2_commit"] = v2_commit
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
