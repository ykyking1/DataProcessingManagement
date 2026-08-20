import os

import pandas as pd

from clickhouse_driver import Client
from dagster import asset, MaterializeResult, MetadataValue

from partitions import daily_partitions
from metadata_store import record_asset_metadata


# ---------------------------------------------------------------------------
# ClickHouse bağlantı ayarları
# ---------------------------------------------------------------------------
#
# Dashboard'daki (app.py) get_clickhouse_*() fonksiyonlarıyla aynı ortam
# değişkenlerini okur. Böylece şifre/host/port gibi bilgiler TEK bir yerden
# (ortam değişkenleri) yönetilir; Dagster tarafı ile dashboard tarafı
# birbirinden bağımsız iki farklı sabit değer olarak senkronizasyon dışı
# kalmaz. Hiçbir ortam değişkeni set edilmezse, önceki sabit kodlanmış
# değerler varsayılan olarak kullanılmaya devam eder (geriye dönük uyumluluk).

def _get_clickhouse_native_host() -> str:
    return os.environ.get("CLICKHOUSE_HOST", "localhost")


def _get_clickhouse_native_port() -> int:
    # NOT: Bu, native protokol portudur (varsayılan 9000).
    # Dashboard'un kullandığı HTTP portu (varsayılan 8123) ile KARIŞTIRMAYIN;
    # ikisi farklı ortam değişkenleriyle yönetilir.
    return int(os.environ.get("CLICKHOUSE_NATIVE_PORT", "9000"))


def _get_clickhouse_user() -> str:
    return os.environ.get("CLICKHOUSE_USER", "default")


def _get_clickhouse_password() -> str:
    return os.environ.get("CLICKHOUSE_PASSWORD", "")


def _get_clickhouse_database() -> str:
    return os.environ.get("CLICKHOUSE_DATABASE", "default")


def _get_clickhouse_table() -> str:
    return os.environ.get("CLICKHOUSE_TABLE", "telemetry")


@asset(
    group_name="storage",
    compute_kind="clickhouse",
    partitions_def=daily_partitions,
    description="Processed AU-AIR telemetri verisini ClickHouse'a yazar.",
)
def clickhouse_telemetry(context, processed_telemetry):

    df = processed_telemetry.copy()

    partition_date = context.partition_key

    context.log.info(
        f"ClickHouse'a yazılacak satır sayısı "
        f"(partition={partition_date}): {len(df)}"
    )

    # ClickHouse bağlantısı
    client = Client(
        host=_get_clickhouse_native_host(),
        port=_get_clickhouse_native_port(),
        user=_get_clickhouse_user(),
        password=_get_clickhouse_password(),
        database=_get_clickhouse_database(),
    )

    database = _get_clickhouse_database()
    table = _get_clickhouse_table()
    table_fqn = f"{database}.{table}"

    # Tabloyu oluştur
    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_fqn}
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
            `class` String,
            flight_id String DEFAULT ''
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(time)
        ORDER BY time
    """)

    # Tablo daha önce flight_id olmadan oluşturulmuş olabilir
    # (bu güncellemeden önceki run'lar). Var olan tabloları geriye
    # dönük uyumlu şekilde güncelle.
    client.execute(f"""
        ALTER TABLE {table_fqn}
        ADD COLUMN IF NOT EXISTS flight_id String DEFAULT ''
    """)

    # -----------------------------------------------------------------------
    # Backfill idempotency
    # -----------------------------------------------------------------------
    #
    # Bu asset partition'lı (günlük) olduğu için aynı gün birden fazla kez
    # materialize edilebilir (örn. backfill ile tekrar çalıştırma). Yeniden
    # INSERT edildiğinde satırların çoğalmasını (duplicate) önlemek için,
    # yazmadan önce bu partition'a (güne) VE bu run'daki uçuşlara ait
    # mevcut satırları siliyoruz.
    #
    # ÖNEMLİ: Silme yalnızca toDate(time) = partition_date şartına göre
    # yapılırsa, aynı güne denk gelen FARKLI bir uçuşun verisi de silinip
    # üzerine yazılmamış olur (yani o uçuş sessizce kaybolur). Bu yüzden
    # silme koşuluna flight_id de eklenmiştir: yalnızca bu run'da işlenen
    # uçuş(lar)ın o güne ait satırları silinir, diğer uçuşlara dokunulmaz.
    #
    # NOT: ClickHouse'da ALTER TABLE ... DELETE bir "mutation"dır ve
    # asenkron çalışır; büyük tablolarda hemen tamamlanmayabilir. Sık
    # backfill yapılan üretim ortamlarında bunun yerine
    # ReplacingMergeTree + FINAL sorgu stratejisi değerlendirilebilir.

    flight_ids_in_batch = (
        sorted(df["flight_id"].dropna().unique().tolist())
        if "flight_id" in df.columns
        else []
    )

    if flight_ids_in_batch:

        client.execute(
            f"ALTER TABLE {table_fqn} "
            "DELETE WHERE toDate(time) = %(partition_date)s "
            "AND flight_id IN %(flight_ids)s",
            {
                "partition_date": partition_date,
                "flight_ids": tuple(flight_ids_in_batch),
            },
        )

    else:

        # flight_id bilgisi yoksa (örn. eski/manuel veri) eski davranışa
        # geri dön: yalnızca tarihe göre sil.
        client.execute(
            f"ALTER TABLE {table_fqn} "
            "DELETE WHERE toDate(time) = %(partition_date)s",
            {"partition_date": partition_date},
        )

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
        "flight_id",
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
            f"""
            INSERT INTO {table_fqn}
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
                `class`,
                flight_id
            )
            VALUES
        """,
            rows,
        )

    context.log.info(
        f"ClickHouse'a {len(rows)} satır yazıldı "
        f"(partition={partition_date})."
    )

    # -----------------------------------------------------------------------
    # Şema metadata'sı (ClickHouse'daki gerçek tablo şeması)
    # -----------------------------------------------------------------------

    schema_rows = client.execute(
        f"DESCRIBE TABLE {table_fqn}"
    )

    schema = {
        row[0]: row[1]
        for row in schema_rows
    }

    # -----------------------------------------------------------------------
    # Metadata geçmişi (Postgres) -- uçuş bazlı filtreleme yapılabilmesi için
    # her uçuş kendi satır sayısıyla ayrı kaydedilir.
    # -----------------------------------------------------------------------

    if flight_ids_in_batch:

        counts_by_flight = (
            df["flight_id"].value_counts().to_dict()
        )

        for flight_id in flight_ids_in_batch:

            record_asset_metadata(
                context,
                group_name="storage",
                flight_id=flight_id,
                row_count=counts_by_flight.get(flight_id, 0),
                metadata={
                    "partition": partition_date,
                    "flight_id": flight_id,
                    "table": table_fqn,
                    "row_count": counts_by_flight.get(flight_id, 0),
                    "column_count": len(columns),
                    "database": database,
                    "schema": schema,
                },
            )

    else:

        record_asset_metadata(
            context,
            group_name="storage",
            flight_id=None,
            row_count=len(rows),
            metadata={
                "partition": partition_date,
                "flight_id": None,
                "table": table_fqn,
                "row_count": len(rows),
                "column_count": len(columns),
                "database": database,
                "schema": schema,
            },
        )

    return MaterializeResult(
        metadata={
            "partition": partition_date,
            "flights": ", ".join(flight_ids_in_batch) if flight_ids_in_batch else "-",
            "table": table_fqn,
            "row_count": len(rows),
            "column_count": len(columns),
            "database": database,
            "schema": MetadataValue.json(schema),
        }
    )
