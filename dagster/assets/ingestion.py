import pandas as pd
from pathlib import Path

from dagster import asset


@asset(
    compute_kind="python",
    group_name="raw_layer",
)
def raw_uav_telemetry(context):

    path = Path("data/au_air/telemetry.parquet")

    df = pd.read_parquet(path)

    context.log.info(
        f"AU-AIR verisi okundu: {len(df)} satır"
    )

    context.log.info(
        f"Kolonlar: {list(df.columns)}"
    )

    return df