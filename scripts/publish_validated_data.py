import argparse
import shutil
import subprocess
import sys
import uuid
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Receive a validated data output and its pipeline metadata. "
            "The data output is tracked with DVC; no Git commands are run."
        )
    )
    parser.add_argument(
        "--data-path",
        required=True,
        type=Path,
        help="Path to the validated data file or directory.",
    )
    parser.add_argument(
        "--release-path",
        type=Path,
        help=(
            "Stable DVC target. When supplied, validated data is copied here "
            "before dvc add; otherwise data-path itself is tracked."
        ),
    )
    parser.add_argument(
        "--pipeline-version",
        required=True,
        help="Version of the pipeline that produced the data.",
    )
    parser.add_argument(
        "--pipeline-git-sha",
        required=True,
        help="Git commit SHA of the pipeline that produced the data.",
    )
    parser.add_argument(
        "--raw-batches",
        required=True,
        nargs="+",
        help="One or more raw batch identifiers.",
    )
    return parser.parse_args()


def resolve_data_path(data_path: Path) -> Path:
    candidate = data_path if data_path.is_absolute() else PROJECT_ROOT / data_path
    resolved_path = candidate.resolve()

    try:
        resolved_path.relative_to(PROJECT_ROOT)
    except ValueError as error:
        raise ValueError("Data path must be inside the project directory.") from error

    if not resolved_path.exists():
        raise FileNotFoundError(f"Validated data output not found: {resolved_path}")

    return resolved_path


def resolve_release_path(release_path: Path) -> Path:
    candidate = release_path if release_path.is_absolute() else PROJECT_ROOT / release_path
    resolved_path = candidate.resolve()

    try:
        resolved_path.relative_to(PROJECT_ROOT)
    except ValueError as error:
        raise ValueError("Release path must be inside the project directory.") from error

    if resolved_path == PROJECT_ROOT:
        raise ValueError("Release path cannot be the project root.")
    return resolved_path


def _remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def stage_release_data(source_path: Path, release_path: Path) -> Path:
    """Replace the stable release target only after a complete local copy."""

    if source_path == release_path:
        return release_path

    if release_path.is_relative_to(source_path) or source_path.is_relative_to(
        release_path
    ):
        raise ValueError("Data path and release path cannot contain each other.")

    release_path.parent.mkdir(parents=True, exist_ok=True)
    operation_id = uuid.uuid4().hex
    staging_path = release_path.with_name(
        f".{release_path.name}.staging-{operation_id}"
    )
    backup_path = release_path.with_name(
        f".{release_path.name}.backup-{operation_id}"
    )

    previous_release_moved = False
    release_installed = False
    try:
        if source_path.is_dir():
            shutil.copytree(source_path, staging_path, copy_function=shutil.copy2)
        else:
            shutil.copy2(source_path, staging_path)

        if release_path.exists():
            release_path.replace(backup_path)
            previous_release_moved = True
        staging_path.replace(release_path)
        release_installed = True
    except BaseException:
        if previous_release_moved and not release_path.exists():
            backup_path.replace(release_path)
        raise
    finally:
        _remove_path(staging_path)
        if release_installed:
            _remove_path(backup_path)

    return release_path


def track_with_dvc(data_path: Path) -> Path:
    relative_data_path = data_path.relative_to(PROJECT_ROOT)

    subprocess.run(
        [sys.executable, "-m", "dvc", "add", relative_data_path.as_posix()],
        cwd=PROJECT_ROOT,
        check=True,
    )

    pointer_path = data_path.with_name(f"{data_path.name}.dvc")
    if not pointer_path.is_file():
        raise FileNotFoundError(f"DVC pointer was not created: {pointer_path}")

    return pointer_path


def build_commit_message(
    pipeline_version: str,
    pipeline_git_sha: str,
    raw_batches: list[str],
    dvc_target: Path,
) -> str:
    if len(raw_batches) == 1:
        batch_summary = raw_batches[0]
    else:
        batch_summary = f"{len(raw_batches)} validated batches"

    return "\n".join(
        [
            f"data(processed): publish {batch_summary}",
            "",
            f"Pipeline-Version: {pipeline_version}",
            f"Pipeline-Git-SHA: {pipeline_git_sha}",
            f"Raw-Batches: {', '.join(raw_batches)}",
            f"DVC-Target: {dvc_target.as_posix()}",
        ]
    )


def main() -> None:
    args = parse_args()
    data_path = resolve_data_path(args.data_path)
    relative_data_path = data_path.relative_to(PROJECT_ROOT)

    print("Validated data release request received:")
    print(f"  Data path: {relative_data_path.as_posix()}")
    print(f"  Pipeline version: {args.pipeline_version}")
    print(f"  Pipeline Git SHA: {args.pipeline_git_sha}")
    print(f"  Raw batches: {', '.join(args.raw_batches)}")

    dvc_target = data_path
    if args.release_path is not None:
        release_path = resolve_release_path(args.release_path)
        dvc_target = stage_release_data(data_path, release_path)
        print(
            "Validated data staged for release: "
            f"{dvc_target.relative_to(PROJECT_ROOT).as_posix()}"
        )

    pointer_path = track_with_dvc(dvc_target)
    relative_pointer_path = pointer_path.relative_to(PROJECT_ROOT)
    print(f"DVC pointer updated: {relative_pointer_path.as_posix()}")

    commit_message = build_commit_message(
        pipeline_version=args.pipeline_version,
        pipeline_git_sha=args.pipeline_git_sha,
        raw_batches=args.raw_batches,
        dvc_target=relative_pointer_path,
    )
    print("\nProposed Git commit message:\n")
    print(commit_message)
    print()
    print("No Git commit or push was performed.")


if __name__ == "__main__":
    main()
