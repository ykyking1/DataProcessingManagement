import pandas as pd

from dagster import asset, MaterializeResult


@asset(
    group_name="processing",
    description="Raw telemetri verisini işleyerek curated katmana hazırlar.",
)
def processed_telemetry(raw_uav_telemetry):

    df = raw_uav_telemetry.copy()

    df["time"] = pd.to_datetime(
        df["time"],
        errors="coerce"
    )

    numeric_columns = [
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

    for column in numeric_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    df = df.dropna(
        subset=[
            "time",
            "latitude",
            "longitude",
        ]
    )

    df = df.drop_duplicates()

    return MaterializeResult(
        value=df,
        metadata={
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": ", ".join(df.columns),
        },
    )