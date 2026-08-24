import os
import subprocess
import sys
from pathlib import Path

from dagster import MaterializeResult, MetadataValue, asset

from partitions import daily_partitions
from metadata_store import record_asset_metadata


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PUBLISH_SCRIPT = PROJECT_ROOT / "scripts" / "publish_validated_data.py"
PROCESSED_DATA_DIR = PROJECT_ROOT / "dagster" / "data" / "processed"
PIPELINE_GIT_PATHS = [
    "dagster",
    "scripts",
    "dvc.yaml",
    "params.yaml",
    "requirements*.txt",
    "pyproject.toml",
    "uv.lock",
    "poetry.lock",
    ":(exclude)dagster/data",
]


def get_pipeline_git_sha() -> str:
    configured_sha = os.getenv("PIPELINE_GIT_SHA")
    if configured_sha:
        return configured_sha

    result = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def get_pipeline_version(pipeline_git_sha: str) -> str:
    configured_version = os.getenv("PIPELINE_VERSION")
    if configured_version:
        return configured_version

    result = subprocess.run(
        [
            "git",
            "describe",
            "--tags",
            "--match",
            "pipeline-v*",
            "--abbrev=0",
            "HEAD",
        ],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        return f"unreleased-{pipeline_git_sha}"

    pipeline_tag = result.stdout.strip()
    committed_changes = subprocess.run(
        [
            "git",
            "diff",
            "--quiet",
            f"{pipeline_tag}..HEAD",
            "--",
            *PIPELINE_GIT_PATHS,
        ],
        cwd=PROJECT_ROOT,
        check=False,
    )
    working_tree_changes = subprocess.run(
        [
            "git",
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            *PIPELINE_GIT_PATHS,
        ],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    if committed_changes.returncode not in (0, 1):
        raise RuntimeError(
            f"Could not compare pipeline code with {pipeline_tag}."
        )

    if committed_changes.returncode == 1 or working_tree_changes.stdout.strip():
        return f"unreleased-{pipeline_git_sha}"

    return pipeline_tag


@asset(
    group_name="publishing",
    partitions_def=daily_partitions,
    description=(
        "Track the successful Dagster processed output with DVC and prepare "
        "its version commit metadata."
    ),
)
def dvc_published_telemetry(context, processed_telemetry):
    """Version the processed Dagster output without running dvc repro."""
    pipeline_git_sha = get_pipeline_git_sha()
    pipeline_version = get_pipeline_version(pipeline_git_sha)
    batch_id = context.partition_key

    command = [
        sys.executable,
        str(PUBLISH_SCRIPT),
        "--data-path",
        str(PROCESSED_DATA_DIR),
        "--pipeline-version",
        pipeline_version,
        "--pipeline-git-sha",
        pipeline_git_sha,
        "--raw-batches",
        batch_id,
    ]

    context.log.info(
        "Starting DVC data versioning for Dagster partition %s.",
        batch_id,
    )
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)

    record_asset_metadata(
        context,
        group_name="publishing",
        flight_id=None,
        row_count=None,
        metadata={
            "partition": batch_id,
            "data_path": str(PROCESSED_DATA_DIR),
            "pipeline_version": pipeline_version,
            "pipeline_git_sha": pipeline_git_sha,
            "raw_batches": batch_id,
            "git_commit_created": False,
        },
    )

    return MaterializeResult(
        metadata={
            "data_path": MetadataValue.path(str(PROCESSED_DATA_DIR)),
            "pipeline_version": pipeline_version,
            "pipeline_git_sha": pipeline_git_sha,
            "raw_batches": batch_id,
            "git_commit_created": False,
        }
    )
