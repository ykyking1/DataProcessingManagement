import pandas as pd

from clickhouse_driver import Client
from dagster import asset, MaterializeResult


@asset(
    group_name="storage",
    compute_kind="clickhouse",
    description="Processed AU-AIR telemetri verisini ClickHouse'a yazar.",
)
def clickhouse_telemetry(context, processed_telemetry):

    df = processed_telemetry.copy()

    context.log.info(
        f"ClickHouse'a yazılacak satır sayısı: {len(df)}"
    )

    # ClickHouse bağlantısı
    client = Client(
        host="localhost",
        port=9000,
        user="default",
        password="HalukCH123!",
        database="default",
    )

    # Tabloyu oluştur
    client.execute("""
        CREATE TABLE IF NOT EXISTS default.telemetry
        (
            time DateTime64(3),
            latitude Float64,
            longitude Float64,
            altitude Float64,
            velocity_x Float64,
            velocity_y Float64,
            velocity_z Float64,
            roll Float64,
            pitch Float64,
            yaw Float64,
            image_name String,
            box_x Float64,
            box_y Float64,
            box_w Float64,
            box_h Float64,
            `class` String
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(time)
        ORDER BY time
    """)

    # ClickHouse'a yazılacak kolonlar
    columns = [
        "time",
        "latitude",
        "longitude",
        "altitude",
        "velocity_x",
        "velocity_y",
        "velocity_z",
        "roll",
        "pitch",
        "yaw",
        "image_name",
        "box_x",
        "box_y",
        "box_w",
        "box_h",
        "class",
    ]

    df = df[columns].copy()

    # Timestamp'i ClickHouse uyumlu hale getir
    df["time"] = pd.to_datetime(
        df["time"],
        errors="coerce",
    )

    # NaN değerleri None yap
    df = df.astype(object).where(pd.notna(df), None)

    # DataFrame → tuple listesi
    rows = list(
        df.itertuples(
            index=False,
            name=None,
        )
    )

    if rows:
        client.execute(
            """
            INSERT INTO default.telemetry
            (
                time,
                latitude,
                longitude,
                altitude,
                velocity_x,
                velocity_y,
                velocity_z,
                roll,
                pitch,
                yaw,
                image_name,
                box_x,
                box_y,
                box_w,
                box_h,
                `class`
            )
            VALUES
        """,
            rows,
        )

    context.log.info(
        f"ClickHouse'a {len(rows)} satır yazıldı."
    )

    return MaterializeResult(
        metadata={
            "table": "default.telemetry",
            "row_count": len(rows),
            "column_count": len(columns),
            "database": "default",
        }
    )