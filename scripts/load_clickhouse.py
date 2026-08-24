import argparse
import json
import os
from pathlib import Path

import pandas as pd
from clickhouse_driver import Client


# ---------------------------------------------------------------------------
# ClickHouse bağlantı ayarları
# ---------------------------------------------------------------------------
#
# Dashboard'daki (dashboard/app.py) get_clickhouse_*() fonksiyonlarıyla aynı
# ortam değişkenlerini okur. Hiçbir ortam değişkeni set edilmezse önceki
# sabit kodlanmış değerler varsayılan olarak kullanılmaya devam eder.

def _get_clickhouse_native_host() -> str:
    return os.environ.get("CLICKHOUSE_HOST", "localhost")


def _get_clickhouse_native_port() -> int:
    # Native protokol portu (varsayılan 9000). Dashboard'un kullandığı
    # HTTP portu (varsayılan 8123) ile KARIŞTIRMAYIN.
    return int(os.environ.get("CLICKHOUSE_NATIVE_PORT", "9000"))


def _get_clickhouse_user() -> str:
    return os.environ.get("CLICKHOUSE_USER", "default")


def _get_clickhouse_password() -> str:
    return os.environ.get("CLICKHOUSE_PASSWORD", "")


def _get_clickhouse_database() -> str:
    return os.environ.get("CLICKHOUSE_DATABASE", "default")


def _get_clickhouse_table() -> str:
    return os.environ.get("CLICKHOUSE_TABLE", "telemetry")


COLUMNS = [
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Processed AU-AIR telemetri verisini ClickHouse'a yazar."
    )
    parser.add_argument(
        "--input-file",
        required=True,
        action="append",
        type=Path,
        help="İşlenmiş parquet dosyası (birden fazla kez verilebilir).",
    )
    parser.add_argument("--partition-date", required=True)
    parser.add_argument("--metadata-out", required=True, type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    partition_date = args.partition_date

    df = pd.concat(
        [pd.read_parquet(file_path) for file_path in args.input_file],
        ignore_index=True,
    )

    print(
        f"ClickHouse'a yazılacak satır sayısı "
        f"(partition={partition_date}): {len(df)}"
    )

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

    # Tablo daha önce flight_id olmadan oluşturulmuş olabilir. Var olan
    # tabloları geriye dönük uyumlu şekilde güncelle.
    client.execute(f"""
        ALTER TABLE {table_fqn}
        ADD COLUMN IF NOT EXISTS flight_id String DEFAULT ''
    """)

    # -------------------------------------------------------------------
    # Backfill idempotency
    # -------------------------------------------------------------------
    #
    # Bu partition (gün) birden fazla kez yüklenebilir (örn. backfill).
    # Yeniden INSERT edildiğinde satırların çoğalmasını (duplicate)
    # önlemek için, yazmadan önce bu partition'a VE bu run'daki uçuşlara
    # ait mevcut satırları siliyoruz. Silme koşuluna flight_id de
    # eklenmiştir; aksi halde aynı güne denk gelen FARKLI bir uçuşun
    # verisi de silinip üzerine yazılmamış olur.

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

    df = df[COLUMNS].copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.astype(object).where(pd.notna(df), None)

    rows = list(df.itertuples(index=False, name=None))

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

    print(
        f"ClickHouse'a {len(rows)} satır yazıldı (partition={partition_date})."
    )

    schema_rows = client.execute(f"DESCRIBE TABLE {table_fqn}")
    schema = {row[0]: row[1] for row in schema_rows}

    flight_records = []

    if flight_ids_in_batch:
        counts_by_flight = df["flight_id"].value_counts().to_dict()
        for flight_id in flight_ids_in_batch:
            flight_records.append(
                {
                    "flight_id": flight_id,
                    "row_count": counts_by_flight.get(flight_id, 0),
                }
            )

    metadata = {
        "partition": partition_date,
        "flights": flight_ids_in_batch,
        "table": table_fqn,
        "database": database,
        "row_count": len(rows),
        "column_count": len(COLUMNS),
        "schema": schema,
        "flight_records": flight_records,
    }

    args.metadata_out.parent.mkdir(parents=True, exist_ok=True)
    args.metadata_out.write_text(
        json.dumps(metadata, ensure_ascii=False),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
