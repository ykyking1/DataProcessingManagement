-- Postgres asset metadata geçmişi şeması
--
-- Dagster'ın kendi instance storage'ı (run/event log) her materialization'ı
-- tutar, ama asset/uçuş/tarih bazlı filtrelenebilir bir sorgu arayüzü
-- sunmaz; dashboard'daki Katalog sekmesi de bugüne kadar yalnızca SON
-- materialization'ı gösteriyordu. Bu tablo, her asset materialize
-- olduğunda (raw_uav_telemetry / processed_telemetry / clickhouse_telemetry)
-- üretilen metadata'yı kalıcı ve sorgulanabilir şekilde saklar.
--
-- Yazan taraf: dagster/metadata_store.py (record_asset_metadata)
-- Okuyan taraf: dashboard/app.py (Katalog sekmesi -> Metadata Geçmişi)
--
-- NOT: docs/postgres_manifest_schema.sql (conversion_manifest) ile
-- karıştırılmamalı -- o, ayrı bir alt sistemin (.tab -> .parquet Rust
-- pipeline) taslak şemasıdır. Bu tablo, bu repodaki Dagster/dashboard
-- AU-AIR pipeline'ının asset kataloğuna aittir.

CREATE TABLE IF NOT EXISTS asset_metadata_history (
    id                      BIGSERIAL PRIMARY KEY,

    -- Hangi asset, hangi Dagster run'ı
    asset_key               TEXT NOT NULL,      -- örn. "raw_uav_telemetry"
    group_name              TEXT,                -- örn. "raw_layer" | "processing" | "storage"
    run_id                  TEXT,

    -- Filtreleme eksenleri
    partition_date          DATE,                -- Dagster günlük partition key
    flight_id               TEXT,                -- uçuş kimliği (varsa)

    -- Özet
    row_count                BIGINT,

    -- Asset'in MaterializeResult metadata'sının tamamı (schema, dosya
    -- yolu, kolon listesi vb.) -- serbest biçimli, JSONB
    metadata                 JSONB NOT NULL DEFAULT '{}'::jsonb,

    materialized_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Asset bazlı filtreleme / "bu asset'in geçmişi" sorgusu
CREATE INDEX IF NOT EXISTS idx_asset_metadata_asset_key
    ON asset_metadata_history (asset_key);

-- Uçuş bazlı filtreleme
CREATE INDEX IF NOT EXISTS idx_asset_metadata_flight_id
    ON asset_metadata_history (flight_id);

-- Tarih bazlı filtreleme
CREATE INDEX IF NOT EXISTS idx_asset_metadata_partition_date
    ON asset_metadata_history (partition_date);

-- Örnek: bir asset'in belirli bir uçuşa ait geçmişi, en yeniden eskiye
--
-- SELECT partition_date, row_count, materialized_at, metadata
-- FROM asset_metadata_history
-- WHERE asset_key = 'processed_telemetry' AND flight_id = 'flight_1'
-- ORDER BY materialized_at DESC;

-- Örnek: belirli bir tarih aralığındaki tüm materialization'lar
--
-- SELECT asset_key, flight_id, row_count, materialized_at
-- FROM asset_metadata_history
-- WHERE partition_date BETWEEN '2026-01-01' AND '2026-01-31'
-- ORDER BY partition_date, asset_key;
