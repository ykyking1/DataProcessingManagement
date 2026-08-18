import pandas as pd

from dagster import asset, MaterializeResult, MetadataValue

from partitions import daily_partitions


@asset(
    group_name="processing",
    partitions_def=daily_partitions,
    description="Raw telemetri verisini işleyerek curated katmana hazırlar.",
)
def processed_telemetry(context, raw_uav_telemetry):

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

    # -------------------------------------------------------------------
    # Şema metadata'sı (kolon adı -> tip)
    # -------------------------------------------------------------------

    schema = {
        column: str(dtype)
        for column, dtype in df.dtypes.items()
    }

    flights = (
        sorted(df["flight_id"].unique().tolist())
        if "flight_id" in df.columns
        else []
    )

    return MaterializeResult(
        value=df,
        metadata={
            "partition": context.partition_key,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": ", ".join(df.columns),
            "flights": ", ".join(flights) if flights else "-",
            "schema": MetadataValue.json(schema),
        },
    )
