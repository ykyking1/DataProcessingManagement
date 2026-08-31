"""Convert a wide AU-AIR ``.tab`` export into the flight telemetry contract.

The AU-AIR export has one anchor frame per row followed by a repeating
``image_name, image_width, ...`` block (hundreds of copies, ~10k columns total).
Only the first (base) frame block is used here. The telemetry schema is now the
native AU-AIR frame schema, so this is almost a straight projection -- the only
change is fixing the ``longtitude`` header typo to ``longitude`` and appending
``Z`` to timezone-less timestamps.

Output columns (order matters -- preprocess_flight_dataframe rejects any
deviation):

    flight_id, time, image_name, image_width, image_height, platform,
    longitude, latitude, altitude, linear_x, linear_y, linear_z,
    angle_phi, angle_theta, angle_psi, num_objects, obj_human, obj_car,
    obj_truck, obj_van, obj_motorbike, obj_bicycle, obj_bus, obj_trailer

The output file name encodes the surviving row count
(``flightdemo_<N>rows_<label>.tab``) because the staged sensor parses it and both
Great Expectations and the ClickHouse loader require an exact match.
"""

from __future__ import annotations

import argparse
import re
from datetime import datetime
from pathlib import Path


DATASET_ID = "flightdemo"

# Output telemetry contract.
FLIGHT_COLUMNS = [
    "flight_id",
    "time",
    "image_name",
    "image_width",
    "image_height",
    "platform",
    "longitude",
    "latitude",
    "altitude",
    "linear_x",
    "linear_y",
    "linear_z",
    "angle_phi",
    "angle_theta",
    "angle_psi",
    "num_objects",
    "obj_human",
    "obj_car",
    "obj_truck",
    "obj_van",
    "obj_motorbike",
    "obj_bicycle",
    "obj_bus",
    "obj_trailer",
]

# output column -> AU-AIR source column (base name, first occurrence in header).
SOURCE_BY_OUTPUT = {
    column: ("longtitude" if column == "longitude" else column)
    for column in FLIGHT_COLUMNS
}

FLOAT_COLUMNS = (
    "longitude",
    "latitude",
    "altitude",
    "linear_x",
    "linear_y",
    "linear_z",
    "angle_phi",
    "angle_theta",
    "angle_psi",
)
INTEGER_COLUMNS = (
    "image_width",
    "image_height",
    "num_objects",
    "obj_human",
    "obj_car",
    "obj_truck",
    "obj_van",
    "obj_motorbike",
    "obj_bicycle",
    "obj_bus",
    "obj_trailer",
)
STRING_COLUMNS = ("flight_id", "image_name", "platform")
RANGE_LIMITS = {
    "latitude": (-90.0, 90.0),
    "longitude": (-180.0, 180.0),
    # Matches the Great Expectations altitude bound in
    # validate_flight_tab_spark_ge.py.
    "altitude": (0.0, 30_000.0),
}

_TZ_SUFFIX = re.compile(r"(Z|[+-]\d{2}:?\d{2})$")
_SAFE_LABEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


def _normalise_timestamp(value: str, *, assume_utc: bool) -> str | None:
    text = value.strip()
    if not text:
        return None
    if not _TZ_SUFFIX.search(text):
        if not assume_utc:
            return None
        text = f"{text}Z"
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return text


def _header_index(header: list[str], name: str) -> int:
    try:
        return header.index(name)
    except ValueError as error:
        raise SystemExit(
            f"AU-AIR header is missing the required column {name!r}."
        ) from error


def convert(
    input_path: Path,
    output_dir: Path,
    *,
    label: str,
    assume_utc: bool,
    limit: int | None,
) -> Path:
    if not _SAFE_LABEL.fullmatch(label):
        raise SystemExit(
            f"--label must match [A-Za-z0-9][A-Za-z0-9_-]*; got {label!r}."
        )
    if not input_path.is_file():
        raise SystemExit(f"Input file not found: {input_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    temp_path = output_dir / f"{DATASET_ID}_{label}.tab.partial"

    kept = 0
    skipped = 0

    with input_path.open("r", encoding="utf-8", newline="") as source, \
            temp_path.open("w", encoding="utf-8", newline="") as sink:
        raw_header = source.readline()
        if not raw_header:
            raise SystemExit("Input file is empty.")
        header = raw_header.rstrip("\r\n").split("\t")

        index_by_output = {
            output_name: _header_index(header, source_name)
            for output_name, source_name in SOURCE_BY_OUTPUT.items()
        }
        max_index = max(index_by_output.values())

        sink.write("\t".join(FLIGHT_COLUMNS) + "\n")

        for raw_line in source:
            line = raw_line.rstrip("\r\n")
            if not line.strip():
                continue
            if limit is not None and kept >= limit:
                break

            fields = line.split("\t")
            if len(fields) <= max_index:
                skipped += 1
                continue

            row = {
                output_name: fields[position].strip()
                for output_name, position in index_by_output.items()
            }

            timestamp = _normalise_timestamp(row["time"], assume_utc=assume_utc)
            if timestamp is None:
                skipped += 1
                continue
            row["time"] = timestamp

            if any(not row[column] for column in STRING_COLUMNS):
                skipped += 1
                continue

            valid = True
            for column in (*FLOAT_COLUMNS, *INTEGER_COLUMNS):
                try:
                    numeric = float(row[column])
                except (TypeError, ValueError):
                    valid = False
                    break
                low_high = RANGE_LIMITS.get(column)
                if low_high is not None and not (
                    low_high[0] <= numeric <= low_high[1]
                ):
                    valid = False
                    break
            if not valid:
                skipped += 1
                continue

            # Normalise integer-typed fields ("8.0" -> "8").
            for column in INTEGER_COLUMNS:
                row[column] = str(int(float(row[column])))

            sink.write(
                "\t".join(row[column] for column in FLIGHT_COLUMNS) + "\n"
            )
            kept += 1

    if kept == 0:
        temp_path.unlink(missing_ok=True)
        raise SystemExit("No AU-AIR rows survived validation; nothing written.")

    final_path = output_dir / f"{DATASET_ID}_{kept}rows_{label}.tab"
    final_path.unlink(missing_ok=True)
    temp_path.replace(final_path)

    print(f"Wrote {final_path} ({kept:,} rows kept, {skipped:,} skipped).")
    object_name = f"flight-tab/inbox/{final_path.name}"
    print(
        "\nUpload to trigger the pipeline:\n"
        f'  docker cp "{final_path}" dpm-minio:/tmp/{final_path.name}\n'
        f'  docker exec dpm-minio sh -c "mc alias set local '
        f'http://127.0.0.1:9000 minioadmin minioadmin123 >/dev/null; '
        f'mc cp /tmp/{final_path.name} local/data-raw/{object_name}"'
    )
    return final_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "local_data" / "auair_seed",
    )
    parser.add_argument("--label", default="auair")
    parser.add_argument(
        "--assume-utc",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Append 'Z' to timestamps that carry no timezone (default: on).",
    )
    parser.add_argument("--limit", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        raise SystemExit("--limit must be greater than zero.")
    convert(
        args.input,
        args.output_dir,
        label=args.label,
        assume_utc=args.assume_utc,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
