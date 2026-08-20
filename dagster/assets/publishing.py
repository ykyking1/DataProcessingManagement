import os
import subprocess
import sys
from pathlib import Path

from dagster import MaterializeResult, MetadataValue, asset

from partitions import daily_partitions


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PUBLISH_SCRIPT = PROJECT_ROOT / "scripts" / "publish_validated_data.py"
PROCESSED_DATA_DIR = PROJECT_ROOT / "dagster" / "data" / "processed"


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
            "tag",
            "--sort=-v:refname",
            "--points-at",
            "HEAD",
            "--list",
            "pipeline-v*",
        ],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    pipeline_tags = result.stdout.splitlines()

    if pipeline_tags:
        return pipeline_tags[0]

    return f"untagged-{pipeline_git_sha}"


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

    return MaterializeResult(
        metadata={
            "data_path": MetadataValue.path(str(PROCESSED_DATA_DIR)),
            "pipeline_version": pipeline_version,
            "pipeline_git_sha": pipeline_git_sha,
            "raw_batches": batch_id,
            "git_commit_created": False,
        }
    )
