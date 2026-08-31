"""Publish one validated processed batch through a stable DVC dataset pointer.

Each logical dataset owns one stable pointer, for example
``data/processed/auair.dvc``. Individual batches live below the tracked
dataset directory and do not create their own DVC pointer files.

The module exposes ``publish_processed_batch`` for Dagster assets and also has
a CLI for manual use. It runs ``dvc add`` and ``dvc push`` but deliberately
does not run any Git command.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from minio import Minio
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RELEASE_ROOT = Path("data/processed")
DEFAULT_DVC_REMOTE = "minio"
DEFAULT_DVC_REMOTE_URL = "s3://dvc-cache"
DEFAULT_DVC_ENDPOINT_URL = "http://127.0.0.1:9000"
SAFE_BATCH_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
SAFE_DATASET_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


@dataclass(frozen=True)
class DvcPublishResult:
    """Structured publication details returned to the Dagster asset."""

    dataset_id: str
    batch_id: str
    batch_path: Path
    dataset_path: Path
    pointer_path: Path
    dvc_remote: str
    dvc_remote_url: str
    dvc_hash_name: str
    dvc_hash: str
    size_bytes: int | None
    file_count: int | None


def _resolve_repo_root(repo_root: Path | str) -> Path:
    resolved = Path(repo_root).expanduser().resolve()
    if not resolved.is_dir():
        raise FileNotFoundError(f"DVC repository root not found: {resolved}")
    if not (resolved / ".dvc").is_dir():
        raise ValueError(f"DVC repository is not initialized: {resolved}")
    return resolved


def _resolve_inside_repo(
    path: Path | str,
    *,
    repo_root: Path,
    label: str,
) -> Path:
    candidate = Path(path)
    resolved = (
        candidate.resolve()
        if candidate.is_absolute()
        else (repo_root / candidate).resolve()
    )
    try:
        resolved.relative_to(repo_root)
    except ValueError as error:
        raise ValueError(f"{label} must be inside the DVC repository.") from error
    return resolved


def _validate_batch_id(batch_id: str) -> str:
    value = batch_id.strip()
    if not SAFE_BATCH_ID.fullmatch(value):
        raise ValueError(
            "batch_id must start with an alphanumeric character and contain "
            "only letters, digits, '.', '_' or '-'."
        )
    return value


def _validate_dataset_id(dataset_id: str) -> str:
    value = dataset_id.strip().lower()
    if not SAFE_DATASET_ID.fullmatch(value):
        raise ValueError(
            "dataset_id must start with an alphanumeric character and contain "
            "only letters, digits, underscores, or hyphens."
        )
    return value


def dataset_id_from_column_count(column_count: int) -> str:
    """Return the stable logical dataset name for an MX column count."""

    if column_count <= 0:
        raise ValueError("column_count must be greater than zero.")
    return f"mx{column_count}"


def expected_batch_path(
    *,
    repo_root: Path,
    release_root: Path | str,
    dataset_id: str | None = None,
    column_count: int | None = None,
    batch_id: str,
) -> tuple[str, Path, Path]:
    """Return dataset id, tracked dataset path, and expected batch path."""

    resolved_dataset_id = (
        _validate_dataset_id(dataset_id)
        if dataset_id is not None
        else dataset_id_from_column_count(column_count or 0)
    )
    safe_batch_id = _validate_batch_id(batch_id)
    resolved_release_root = _resolve_inside_repo(
        release_root,
        repo_root=repo_root,
        label="release_root",
    )
    if resolved_release_root == repo_root:
        raise ValueError("release_root cannot be the repository root.")

    dataset_path = resolved_release_root / resolved_dataset_id
    batch_path = dataset_path / "batches" / safe_batch_id
    return resolved_dataset_id, dataset_path, batch_path


def _run_dvc(repo_root: Path, arguments: list[str]) -> None:
    subprocess.run(
        [sys.executable, "-m", "dvc", *arguments],
        cwd=repo_root,
        check=True,
    )


def _environment_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _remote_bucket_name(remote_url: str) -> str:
    parsed = urlparse(remote_url)
    if parsed.scheme.lower() != "s3" or not parsed.netloc:
        raise ValueError(
            "DVC remote URL must use the s3:// scheme and include a bucket."
        )
    return parsed.netloc


def _ensure_dvc_bucket(remote_url: str) -> None:
    bucket_name = _remote_bucket_name(remote_url)
    configured_endpoint = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000").strip()
    secure = _environment_flag("MINIO_SECURE")
    if "://" in configured_endpoint:
        parsed_endpoint = urlparse(configured_endpoint)
        if not parsed_endpoint.netloc:
            raise ValueError(f"Invalid MINIO_ENDPOINT: {configured_endpoint}")
        endpoint = parsed_endpoint.netloc
        secure = parsed_endpoint.scheme.lower() == "https"
    else:
        endpoint = configured_endpoint.rstrip("/")

    access_key = os.getenv(
        "MINIO_ACCESS_KEY",
        os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    )
    secret_key = os.getenv(
        "MINIO_SECRET_KEY",
        os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    )
    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def _configure_dvc_remote(
    *,
    repo_root: Path,
    remote_name: str,
    remote_url: str,
    endpoint_url: str,
) -> None:
    """Create an ignored, machine-local DVC remote configuration."""

    _remote_bucket_name(remote_url)
    if not endpoint_url.strip():
        raise ValueError("DVC S3 endpoint URL cannot be empty.")

    _run_dvc(
        repo_root,
        ["remote", "add", "--local", "--force", remote_name, remote_url],
    )
    _run_dvc(
        repo_root,
        [
            "remote",
            "modify",
            "--local",
            remote_name,
            "endpointurl",
            endpoint_url,
        ],
    )


def _read_dvc_pointer(
    pointer_path: Path,
    *,
    expected_output_name: str,
) -> tuple[str, str, int | None, int | None]:
    """Read the hash DVC already calculated without touching dataset files."""

    payload = yaml.safe_load(pointer_path.read_text(encoding="utf-8"))
    outputs = payload.get("outs") if isinstance(payload, dict) else None
    if not isinstance(outputs, list) or len(outputs) != 1:
        raise ValueError(
            f"DVC pointer must contain exactly one output: {pointer_path}"
        )
    output = outputs[0]
    if not isinstance(output, dict):
        raise ValueError(f"Invalid DVC pointer output: {pointer_path}")
    if output.get("path") != expected_output_name:
        raise ValueError(
            "DVC pointer output path mismatch: "
            f"expected {expected_output_name!r}, got {output.get('path')!r}."
        )

    hash_name = output.get("hash")
    if not isinstance(hash_name, str) or not hash_name:
        hash_name = next(
            (
                candidate
                for candidate in ("md5", "sha256", "etag")
                if isinstance(output.get(candidate), str)
            ),
            None,
        )
    if not hash_name or not isinstance(output.get(hash_name), str):
        raise ValueError(f"DVC pointer hash is missing: {pointer_path}")

    size_bytes = output.get("size")
    file_count = output.get("nfiles")
    return (
        hash_name,
        output[hash_name],
        int(size_bytes) if size_bytes is not None else None,
        int(file_count) if file_count is not None else None,
    )


def publish_processed_batch(
    data_path: Path | str,
    *,
    repo_root: Path | str,
    dataset_id: str | None = None,
    column_count: int | None = None,
    row_count: int,
    batch_id: str,
    release_root: Path | str = DEFAULT_RELEASE_ROOT,
    dvc_remote: str = DEFAULT_DVC_REMOTE,
    dvc_remote_url: str = DEFAULT_DVC_REMOTE_URL,
    dvc_endpoint_url: str = DEFAULT_DVC_ENDPOINT_URL,
) -> DvcPublishResult:
    """Track and push one validated batch through its logical dataset pointer.

    Processing must write the batch directly to the returned logical location
    (``.../<dataset_id>/batches/<batch_id>``). Requiring that layout avoids another
    full copy of a potentially very large processed batch.
    """

    if row_count < 0:
        raise ValueError("row_count cannot be negative.")
    remote_name = dvc_remote.strip()
    if not remote_name:
        raise ValueError("dvc_remote cannot be empty.")
    remote_url = dvc_remote_url.strip()
    endpoint_url = dvc_endpoint_url.strip()

    resolved_repo_root = _resolve_repo_root(repo_root)
    safe_batch_id = _validate_batch_id(batch_id)
    resolved_dataset_id, dataset_path, required_batch_path = expected_batch_path(
        repo_root=resolved_repo_root,
        release_root=release_root,
        dataset_id=dataset_id,
        column_count=column_count,
        batch_id=safe_batch_id,
    )
    resolved_data_path = _resolve_inside_repo(
        data_path,
        repo_root=resolved_repo_root,
        label="data_path",
    )
    if resolved_data_path != required_batch_path:
        raise ValueError(
            "Processed batch must be written directly to its logical DVC "
            f"location: expected {required_batch_path}, got {resolved_data_path}."
        )
    if not resolved_data_path.exists():
        raise FileNotFoundError(
            f"Validated processed batch not found: {resolved_data_path}"
        )

    _ensure_dvc_bucket(remote_url)
    _configure_dvc_remote(
        repo_root=resolved_repo_root,
        remote_name=remote_name,
        remote_url=remote_url,
        endpoint_url=endpoint_url,
    )

    relative_dataset_path = dataset_path.relative_to(resolved_repo_root)
    _run_dvc(resolved_repo_root, ["add", relative_dataset_path.as_posix()])

    pointer_path = dataset_path.with_name(f"{dataset_path.name}.dvc")
    if not pointer_path.is_file():
        raise FileNotFoundError(f"DVC pointer was not created: {pointer_path}")
    relative_pointer_path = pointer_path.relative_to(resolved_repo_root)
    dvc_hash_name, dvc_hash, size_bytes, file_count = _read_dvc_pointer(
        pointer_path,
        expected_output_name=dataset_path.name,
    )

    _run_dvc(
        resolved_repo_root,
        [
            "push",
            "--remote",
            remote_name,
            relative_pointer_path.as_posix(),
        ],
    )

    result = DvcPublishResult(
        dataset_id=resolved_dataset_id,
        batch_id=safe_batch_id,
        batch_path=resolved_data_path,
        dataset_path=dataset_path,
        pointer_path=pointer_path,
        dvc_remote=remote_name,
        dvc_remote_url=remote_url,
        dvc_hash_name=dvc_hash_name,
        dvc_hash=dvc_hash,
        size_bytes=size_bytes,
        file_count=file_count,
    )

    print("Validated processed batch published with DVC:")
    print(f"  Dataset: {resolved_dataset_id}")
    print(f"  Batch: {safe_batch_id}")
    print(f"  DVC pointer: {relative_pointer_path.as_posix()}")
    print(f"  DVC hash: {dvc_hash_name}:{dvc_hash}")
    print(f"  DVC remote: {remote_name} ({remote_url})")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-path", required=True, type=Path)
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(os.getenv("DVC_REPO_ROOT", PROJECT_ROOT)),
    )
    parser.add_argument("--release-root", type=Path, default=DEFAULT_RELEASE_ROOT)
    dataset_group = parser.add_mutually_exclusive_group(required=True)
    dataset_group.add_argument("--dataset-id")
    dataset_group.add_argument("--column-count", type=int)
    parser.add_argument("--row-count", required=True, type=int)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument(
        "--dvc-remote",
        default=os.getenv("DVC_REMOTE_NAME", DEFAULT_DVC_REMOTE),
    )
    parser.add_argument(
        "--dvc-remote-url",
        default=os.getenv("DVC_REMOTE_URL", DEFAULT_DVC_REMOTE_URL),
    )
    parser.add_argument(
        "--dvc-endpoint-url",
        default=os.getenv("DVC_S3_ENDPOINT_URL", DEFAULT_DVC_ENDPOINT_URL),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    publish_processed_batch(
        args.data_path,
        repo_root=args.repo_root,
        release_root=args.release_root,
        dataset_id=args.dataset_id,
        column_count=args.column_count,
        row_count=args.row_count,
        batch_id=args.batch_id,
        dvc_remote=args.dvc_remote,
        dvc_remote_url=args.dvc_remote_url,
        dvc_endpoint_url=args.dvc_endpoint_url,
    )


if __name__ == "__main__":
    main()
