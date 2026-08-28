"""
Asset Metadata Geçmişi (Postgres)

Dagster'ın kendi instance storage'ı (run/event log) asset materialization
geçmişini tutar, ama bunu asset/uçuş/tarih bazlı filtrelemek için uygun
değildir ve dashboard'daki Katalog sekmesi bugüne kadar yalnızca SON
materialization'ı gösteriyordu (bkz. app.py -> ASSET_CATALOG_QUERY,
"assetMaterializations(limit: 1)").

Bu modül, her asset materialize olduğunda üretilen metadata'yı (satır
sayısı, şema, uçuş kimliği, partition/tarih vb.) ayrı ve kalıcı bir
Postgres tablosuna (asset_metadata_history) yazar. Şema tanımı için bkz.
docs/postgres_asset_metadata_schema.sql. Dashboard bu tabloyu doğrudan
okuyarak asset / uçuş / tarih bazlı geçmiş sorgulama sağlar.

Postgres'e yazılamaması (bağlantı yok, tablo yok vb.) pipeline'ı
BOZMAMALI -- bu yüzden tüm hatalar burada yakalanıp sadece loglanır.
"""

import os

import psycopg2
import psycopg2.extras


# ---------------------------------------------------------------------------
# Bağlantı ayarları
# ---------------------------------------------------------------------------
#
# Dashboard'daki (dashboard/app.py) aynı isimli POSTGRES_* ortam
# değişkenleriyle senkron tutulmalı -- ClickHouse için kullanılan
# CLICKHOUSE_* deseninin aynısı.

def _get_conn_params() -> dict:
    return dict(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        dbname=os.environ.get("POSTGRES_DATABASE", "postgres"),
    )


# ---------------------------------------------------------------------------
# Şema
# ---------------------------------------------------------------------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS asset_metadata_history (
    id              BIGSERIAL PRIMARY KEY,
    asset_key       TEXT NOT NULL,
    group_name      TEXT,
    partition_date  DATE,
    flight_id       TEXT,
    run_id          TEXT,
    row_count       BIGINT,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    materialized_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_asset_metadata_asset_key
    ON asset_metadata_history (asset_key);

CREATE INDEX IF NOT EXISTS idx_asset_metadata_flight_id
    ON asset_metadata_history (flight_id);

CREATE INDEX IF NOT EXISTS idx_asset_metadata_partition_date
    ON asset_metadata_history (partition_date);
"""


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()


# ---------------------------------------------------------------------------
# Kayıt
# ---------------------------------------------------------------------------

def record_asset_metadata(
    context,
    metadata: dict,
    group_name: str = None,
    flight_id: str = None,
    row_count: int = None,
) -> None:
    """
    Bir asset (ya da bir asset'in tek bir uçuşa ait alt kümesi) materialize
    olduğunda çağrılır; verilen metadata dict'ini (JSON'a çevrilebilir
    düz Python değerleri -- dict/list/str/int/float/bool/None) Postgres'e
    yeni bir satır olarak ekler.

    Var olan bir satırı güncellemez -- kasıtlı olarak: amaç geçmişi
    (aynı asset'in zaman içindeki tüm materialization'larını) korumaktır.
    """

    asset_key = "/".join(context.asset_key.path)

    # partitions_def'i OLMAYAN asset'lerde (ör. extended_telemetry_load,
    # grid_telemetry_load) context.partition_key erişimi DagsterInvariant
    # ViolationError fırlatır -- bu fonksiyon hem partition'lı hem
    # partition'sız asset'lerden çağrılabildiği için bu durumu güvenle
    # ele alıyoruz (partition'sız asset'lerde partition_date=NULL yazılır).
    partition_date = context.partition_key if context.has_partition_key else None

    try:
        conn = psycopg2.connect(**_get_conn_params())
    except Exception as exc:
        context.log.warning(
            f"Postgres metadata kaydı atlandı (bağlantı hatası): {exc}"
        )
        return

    try:
        ensure_schema(conn)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO asset_metadata_history
                    (asset_key, group_name, partition_date, flight_id,
                     run_id, row_count, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    asset_key,
                    group_name,
                    partition_date,
                    flight_id,
                    context.run_id,
                    row_count,
                    psycopg2.extras.Json(metadata),
                ),
            )

        conn.commit()

        context.log.info(
            f"Metadata geçmişe kaydedildi (asset={asset_key}, "
            f"partition={partition_date}, flight_id={flight_id})."
        )

    except Exception as exc:
        context.log.warning(
            f"Postgres metadata kaydı yazılamadı: {exc}"
        )

    finally:
        conn.close()
