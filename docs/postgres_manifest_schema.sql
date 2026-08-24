-- Postgres metadata katalog şeması
-- Konuştuğumuz "manifest" kavramının somut karşılığı: her .tab dosyasının
-- pipeline'daki durumunu, üç katmandaki (tab/parquet/clickhouse) satır
-- sayılarını ve doğrulama bilgisini tek yerden takip eder.

CREATE TABLE conversion_manifest (
    id                      BIGSERIAL PRIMARY KEY,

    -- Kaynak tanımlama / izlenebilirlik
    tab_file_name           TEXT NOT NULL,      -- temiz dosya adı (örn. "dataset_01.tab")
    ham_file_name           TEXT,               -- varsa, izlenebilirlik için
    flight_id               TEXT,               -- uçuş/görev kimliği (varsa)
    aircraft_type            TEXT,               -- uçak tipi -- her tipte sütun sayısı
                                                  -- farklı olabildiği için ÖNEMLİ,
                                                  -- yeni veri üretilirken MUTLAKA doldurulmalı

    -- Alt küme/test çalıştırması izlenebilirliği (tam dosya yerine
    -- sadece bir kısmı işlendiyse)
    is_subset                BOOLEAN NOT NULL DEFAULT FALSE,
    subset_row_count         BIGINT,             -- is_subset=true ise kaç satır alındı

    -- Pipeline durumu
    status                  TEXT NOT NULL DEFAULT 'pending',
        -- pending | processing | done | verification_failed | needs_review
    attempt_count           INT NOT NULL DEFAULT 0,
    max_attempts            INT NOT NULL DEFAULT 3,

    -- Üç katmandaki satır sayıları -- konuştuğumuz "üç yönlü mutabakat"
    row_count_tab           BIGINT,
    row_count_parquet       BIGINT,
    row_count_clickhouse    BIGINT,

    -- Şema/sütun bilgisi
    column_count              INT,
    had_trailing_tab_issue    BOOLEAN,           -- temizleme sırasında fazladan
                                                  -- tab bulunup düzeltildi mi

    -- İçerik doğrulama (byte checksum değil, sütun istatistik parmak izi)
    content_fingerprint     TEXT,               -- örn. sütun sum/min/max hash'i

    -- MinIO konumu (ESKİ/parquet akışından kalma alanlar -- Bölüm 39
    -- kararıyla parquet pipeline'dan çıkarıldı, şimdilik dokunulmadı,
    -- ileride detaylıca gözden geçirilecek)
    parquet_object_key      TEXT,               -- MinIO'daki tam yol
    parquet_size_bytes      BIGINT,

    -- MinIO konumu (GÜNCEL -- ham metin + sıkıştırma akışı, Bölüm 39)
    tab_zst_object_key      TEXT,               -- MinIO'daki .tab.zst tam yolu
    tab_zst_size_bytes      BIGINT,
    original_size_bytes      BIGINT,             -- sıkıştırma öncesi ham .tab boyutu

    -- Sıkıştırma bilgisi
    compression_algorithm    TEXT,               -- 'zstd' | 'bz2'
    compression_level        INT,

    -- Süre/performans bilgisi -- pipeline sağlığını zamanla takip etmek için
    compress_duration_seconds         DOUBLE PRECISION,
    minio_upload_duration_seconds     DOUBLE PRECISION,
    clickhouse_load_duration_seconds  DOUBLE PRECISION,

    -- ClickHouse hedef bilgisi
    clickhouse_table_name    TEXT,               -- hangi tabloya yüklendi
    clickhouse_disk_bytes    BIGINT,             -- ClickHouse'daki nihai disk boyutu
    clickhouse_loaded_at    TIMESTAMPTZ,

    -- Hata takibi
    error_detail             TEXT,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Worker'ların "bekleyen bir dosya al" sorgusu için
CREATE INDEX idx_manifest_status ON conversion_manifest (status);

-- Aynı dosyanın iki kere işlenmemesi için
CREATE UNIQUE INDEX idx_manifest_tab_file ON conversion_manifest (tab_file_name);

-- Örnek: bir worker'ın atomik olarak "bir sonraki bekleyen dosyayı" alması
-- (SKIP LOCKED, aynı anda birden fazla worker çalışırken çakışmayı önler)
--
-- UPDATE conversion_manifest
-- SET status = 'processing', attempt_count = attempt_count + 1, updated_at = now()
-- WHERE id = (
--     SELECT id FROM conversion_manifest
--     WHERE status = 'pending' OR (status = 'processing' AND updated_at < now() - interval '10 minutes')
--     ORDER BY id
--     LIMIT 1
--     FOR UPDATE SKIP LOCKED
-- )
-- RETURNING *;

-- Örnek: reconciliation job -- üç katman arasında uyuşmazlık var mı?
--
-- SELECT tab_file_name, row_count_tab, row_count_parquet, row_count_clickhouse
-- FROM conversion_manifest
-- WHERE status = 'done'
--   AND (row_count_tab != row_count_parquet OR row_count_parquet != row_count_clickhouse);
