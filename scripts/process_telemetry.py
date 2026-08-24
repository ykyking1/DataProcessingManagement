import argparse
import json
from pathlib import Path

import pandas as pd


NUMERIC_COLUMNS = [
    "latitude",
    "longitude",
    "altitude",
    "velocity_x",
    "velocity_y",
    "velocity_z",
    "roll",
    "pitch",
    "yaw",
    "box_x",
    "box_y",
    "box_w",
    "box_h",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Raw telemetri verisini işleyerek curated katmana hazırlar."
    )
    parser.add_argument("--input-path", required=True, type=Path)
    parser.add_argument("--partition-date", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--metadata-out", required=True, type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    df = pd.read_parquet(args.input_path)

    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    for column in NUMERIC_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.dropna(subset=["time", "latitude", "longitude"])
    df = df.drop_duplicates()

    schema = {column: str(dtype) for column, dtype in df.dtypes.items()}

    flights = (
        sorted(df["flight_id"].unique().tolist())
        if "flight_id" in df.columns
        else []
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)

    processed_files = []
    flight_records = []

    if not df.empty:

        groups = df.groupby("flight_id") if flights else [(None, df)]

        for flight_id, group_df in groups:

            file_name = (
                f"{flight_id}_{args.partition_date}.parquet"
                if flight_id
                else f"{args.partition_date}.parquet"
            )
            file_path = args.output_dir / file_name

            group_df.to_parquet(file_path, index=False)
            processed_files.append(str(file_path))

            flight_records.append(
                {
                    "flight_id": flight_id,
                    "row_count": len(group_df),
                    "column_count": len(group_df.columns),
                    "columns": list(group_df.columns),
                    "processed_file": str(file_path),
                }
            )

        print(
            f"İşlenmiş veri diske kaydedildi (partition={args.partition_date}): "
            f"{processed_files}"
        )

    metadata = {
        "partition": args.partition_date,
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "flights": flights,
        "schema": schema,
        "processed_files": processed_files,
        "flight_records": flight_records,
    }

    args.metadata_out.parent.mkdir(parents=True, exist_ok=True)
    args.metadata_out.write_text(
        json.dumps(metadata, ensure_ascii=False),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
