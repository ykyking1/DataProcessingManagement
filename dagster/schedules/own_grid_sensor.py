"""own_data_quality_job'u yeni .tab dosyaları için otomatik tetikler.

schedules/mx_tab_sensor.py ile AYNI desen (dosya parmak izi/hash ile
aynı dosyayı iki kez işlememe, cursor'a kaydetme, minimum dosya yaşı
bekleme) -- ama arkadaşımın MX inbox'ına DEĞİL, bizim sentetik grid
verimiz için ayrı bir inbox klasörüne bakar. Kendi verimizle GE/DVC
denemesi (own_tab_validated/own_tab_dvc_published) bu sensor'le
otomatikleşir -- 2026-08-28, "bu job dosya klasöre düştüğünde otomatik
tetiklenebilir mi" sorusuna cevaben eklendi.

Neden ayrı bir inbox (local_data/synthetic_grid/'in kendisi DEĞİL):
o klasörde zaten 11+ dosya kalıcı olarak duruyor -- sensor ilk
çalıştığında hepsini "yeni" sanıp aynı anda run patlatırdı. Kullanıcı
yeni/tekrar doğrulanacak bir dosyayı buraya kopyaladığında tetiklenir.
"""

import hashlib
import json
import os
import time
from pathlib import Path

from dagster import RunRequest, SensorEvaluationContext, sensor


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_DIR = PROJECT_ROOT / "local_data" / "own_grid_inbox"
INPUT_PATTERN = "*.tab"

# scripts/load_extended_telemetry.py::KNOWN_AIRCRAFT_COLUMN_COUNTS ile
# AYNI eşleme -- dosya adından ("synthetic_20k_5000.tab" -> "20k")
# içerideki gerçek aircraft_type değerine ("AIRCRAFT_20K") çeviriyor.
_TIER_TO_AIRCRAFT_TYPE = {
    "10k": "AIRCRAFT_10K",
    "20k": "AIRCRAFT_20K",
    "30k": "AIRCRAFT_30K",
    "40k": "AIRCRAFT_40K",
    "50k": "AIRCRAFT_50K",
}


def _input_directory() -> Path:
    configured_path = os.getenv("OWN_GRID_INPUT_DIR")
    if not configured_path:
        return DEFAULT_INPUT_DIR
    path = Path(configured_path)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _minimum_file_age_seconds() -> float:
    configured_value = os.getenv("OWN_GRID_MIN_FILE_AGE_SECONDS", "10")
    try:
        value = float(configured_value)
    except ValueError as error:
        raise ValueError(
            "OWN_GRID_MIN_FILE_AGE_SECONDS must be a number."
        ) from error
    if value < 0:
        raise ValueError("OWN_GRID_MIN_FILE_AGE_SECONDS cannot be negative.")
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
    return {str(path): str(fingerprint) for path, fingerprint in value.items()}


def _file_fingerprint(file_path: Path) -> str:
    file_stat = file_path.stat()
    identity = f"{file_path.resolve()}|{file_stat.st_size}|{file_stat.st_mtime_ns}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _aircraft_type_from_name(file_path: Path) -> str | None:
    # "synthetic_20k_5000.tab" -> "20k" -> "AIRCRAFT_20K"
    stem_parts = file_path.stem.split("_")
    for part in stem_parts:
        if part.lower() in _TIER_TO_AIRCRAFT_TYPE:
            return _TIER_TO_AIRCRAFT_TYPE[part.lower()]
    return None


@sensor(
    job_name="own_data_quality_job",
    minimum_interval_seconds=10,
)
def own_grid_sensor(context: SensorEvaluationContext):
    """Launch own_data_quality_job for new or updated own grid .tab files."""

    input_directory = _input_directory().resolve()
    input_directory.mkdir(parents=True, exist_ok=True)

    files = sorted(
        {
            file_path.resolve()
            for file_path in input_directory.glob(INPUT_PATTERN)
            if file_path.is_file()
        }
    )
    if not files:
        context.log.info("No own grid input found: %s", input_directory)
        return

    processed_fingerprints = _load_cursor(context.cursor)
    minimum_age = _minimum_file_age_seconds()
    now = time.time()
    cursor_changed = False

    for file_path in files:
        file_stat = file_path.stat()
        if now - file_stat.st_mtime < minimum_age:
            context.log.info(
                "Waiting for own grid input to become stable: %s", file_path
            )
            continue

        fingerprint = _file_fingerprint(file_path)
        file_key = str(file_path)
        if processed_fingerprints.get(file_key) == fingerprint:
            continue

        aircraft_type = _aircraft_type_from_name(file_path)
        context.log.info(
            "New or updated own grid input detected: %s (aircraft_type=%s)",
            file_path,
            aircraft_type or "not inferred",
        )

        run_config = {
            "ops": {
                "own_tab_validated": {
                    "config": {
                        "input_path": str(file_path),
                    }
                }
            }
        }
        if aircraft_type:
            run_config["ops"]["own_tab_validated"]["config"][
                "expected_aircraft_type"
            ] = aircraft_type

        yield RunRequest(
            run_key=fingerprint,
            run_config=run_config,
            tags={
                "own_grid_input_file": file_path.name,
                "own_grid_aircraft_type": aircraft_type or "unknown",
            },
        )
        processed_fingerprints[file_key] = fingerprint
        cursor_changed = True

    if cursor_changed:
        context.update_cursor(json.dumps(processed_fingerprints, sort_keys=True))
