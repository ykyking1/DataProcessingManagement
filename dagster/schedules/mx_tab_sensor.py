import hashlib
import json
import os
import re
import time
from pathlib import Path

from dagster import RunRequest, SensorEvaluationContext, sensor


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "cleaned" / "mx_tab_inbox"
INPUT_PATTERNS = ("*.tab.zst", "*.tab.zstd")
AIRCRAFT_TYPE_PATTERN = re.compile(r"(?i)(mx\d+)")


def _input_directory() -> Path:
    configured_path = os.getenv("MX_TAB_INPUT_DIR")
    if not configured_path:
        return DEFAULT_INPUT_DIR

    path = Path(configured_path)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _minimum_file_age_seconds() -> float:
    configured_value = os.getenv("MX_TAB_MIN_FILE_AGE_SECONDS", "10")
    try:
        value = float(configured_value)
    except ValueError as error:
        raise ValueError(
            "MX_TAB_MIN_FILE_AGE_SECONDS must be a number."
        ) from error
    if value < 0:
        raise ValueError("MX_TAB_MIN_FILE_AGE_SECONDS cannot be negative.")
    return value


def _load_cursor(cursor: str | None) -> dict[str, str]:
    if not cursor:
        return {}
    try:
        value = json.loads(cursor)
    except json.JSONDecodeError:
        return {}
    if not isinstance(value, dict):
        return {}
    return {
        str(path): str(fingerprint)
        for path, fingerprint in value.items()
    }


def _file_fingerprint(file_path: Path) -> str:
    file_stat = file_path.stat()
    identity = f"{file_path.resolve()}|{file_stat.st_size}|{file_stat.st_mtime_ns}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _aircraft_type_from_name(file_path: Path) -> str | None:
    match = AIRCRAFT_TYPE_PATTERN.search(file_path.name)
    return match.group(1).upper() if match else None


def _artifact_key(file_path: Path, fingerprint: str) -> str:
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", file_path.name).strip("._")
    return f"{safe_name or 'mx_input'}-{fingerprint[:12]}"


@sensor(
    job_name="mx_tab_quality_job",
    minimum_interval_seconds=10,
)
def mx_tab_sensor(context: SensorEvaluationContext):
    """Launch the Spark/GE job for new or updated MX tab input files."""

    input_directory = _input_directory().resolve()
    input_directory.mkdir(parents=True, exist_ok=True)

    files = sorted(
        {
            file_path.resolve()
            for pattern in INPUT_PATTERNS
            for file_path in input_directory.glob(pattern)
            if file_path.is_file()
        }
    )
    if not files:
        context.log.info("No MX tab input found: %s", input_directory)
        return

    processed_fingerprints = _load_cursor(context.cursor)
    minimum_age = _minimum_file_age_seconds()
    now = time.time()
    cursor_changed = False

    for file_path in files:
        file_stat = file_path.stat()
        if now - file_stat.st_mtime < minimum_age:
            context.log.info(
                "Waiting for MX input to become stable: %s", file_path
            )
            continue

        fingerprint = _file_fingerprint(file_path)
        file_key = str(file_path)
        if processed_fingerprints.get(file_key) == fingerprint:
            continue

        aircraft_type = _aircraft_type_from_name(file_path)
        artifact_key = _artifact_key(file_path, fingerprint)
        output_path = (
            PROJECT_ROOT
            / "tmp"
            / "dagster_spark_ge"
            / artifact_key
            / "processed"
        )
        report_path = (
            PROJECT_ROOT
            / "reports"
            / "validation"
            / "mx_tab"
            / f"{artifact_key}.json"
        )
        context.log.info(
            "New or updated MX input detected: %s (aircraft_type=%s)",
            file_path,
            aircraft_type or "not inferred",
        )

        yield RunRequest(
            run_key=fingerprint,
            run_config={
                "ops": {
                    "spark_processed_tab": {
                        "config": {
                            "input_path": str(file_path),
                            "output_path": str(output_path),
                        }
                    },
                    "spark_validated_tab": {
                        "config": {
                            "expected_aircraft_type": aircraft_type,
                            "report_path": str(report_path),
                        }
                    },
                }
            },
            tags={
                "mx_input_file": file_path.name,
                "mx_aircraft_type": aircraft_type or "unknown",
            },
        )
        processed_fingerprints[file_key] = fingerprint
        cursor_changed = True

    if cursor_changed:
        context.update_cursor(
            json.dumps(processed_fingerprints, sort_keys=True)
        )
