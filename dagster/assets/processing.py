from pathlib import Path

import pandas as pd

from dagster import asset, MaterializeResult, MetadataValue

from partitions import daily_partitions
from metadata_store import record_asset_metadata


# ---------------------------------------------------------------------------
# İşlenmiş verinin diske kaydedileceği klasör
# ---------------------------------------------------------------------------
#
# ClickHouse dashboard'un ana veri kaynağı olmaya devam eder (filtreleme,
# CSV dışa aktarma vb. hep ClickHouse üzerinden yapılır). Bu klasör, her
# run'da üretilen curated veriyi dosya olarak da saklar; böylece işlenmiş
# veri yedeklenmiş/denetlenebilir olur ve ClickHouse dışında da incelenebilir.
#
# "raw_uav_telemetry" ile aynı çalışma dizini varsayımını kullanır (dagster
# dev, dagster/ klasöründen çalıştırılır -> data/processed = dagster/data/processed).
PROCESSED_DATA_DIR = Path("data/processed")


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

    # -------------------------------------------------------------------
    # İşlenmiş veriyi /processed klasörüne parquet olarak kaydet
    # -------------------------------------------------------------------
    #
    # ClickHouse asset'indeki (clickhouse.py) silme/yeniden-yazma mantığıyla
    # aynı granülerlikte (partition_date + flight_id) dosyalıyoruz; böylece
    # aynı güne (partition) ait farklı bir uçuşun dosyası, bu run'da
    # üzerine yazılmaz.

    partition_date = context.partition_key

    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    processed_files = []

    if not df.empty:

        if flights:
            groups = df.groupby("flight_id")
        else:
            groups = [(None, df)]

        for flight_id, group_df in groups:

            file_name = (
                f"{flight_id}_{partition_date}.parquet"
                if flight_id
                else f"{partition_date}.parquet"
            )

            file_path = PROCESSED_DATA_DIR / file_name

            group_df.to_parquet(file_path, index=False)

            processed_files.append(str(file_path))

            # Her uçuş için ayrı bir metadata geçmişi satırı -- dashboard'daki
            # Katalog sekmesinde uçuş bazlı filtreleme bunun üzerinden yapılır.
            record_asset_metadata(
                context,
                group_name="processing",
                flight_id=flight_id,
                row_count=len(group_df),
                metadata={
                    "partition": partition_date,
                    "flight_id": flight_id,
                    "row_count": len(group_df),
                    "column_count": len(group_df.columns),
                    "columns": list(group_df.columns),
                    "schema": schema,
                    "processed_file": str(file_path),
                },
            )

        context.log.info(
            f"İşlenmiş veri diske kaydedildi (partition={partition_date}): "
            f"{processed_files}"
        )

    return MaterializeResult(
        value=df,
        metadata={
            "partition": partition_date,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": ", ".join(df.columns),
            "flights": ", ".join(flights) if flights else "-",
            "schema": MetadataValue.json(schema),
            "processed_files": (
                MetadataValue.json(processed_files)
                if processed_files
                else "-"
            ),
        },
    )
