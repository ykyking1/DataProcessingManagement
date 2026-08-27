-- MX pipeline run ve asset materialization kataloğu.
--
-- Bu veritabanı Dagster loglarını, sensor cursor'larını veya büyük veri
-- dosyalarını saklamaz. Her job'un kod kimliğini ve asset'lerin ürettiği
-- sorgulanabilir özet metadata'yı saklar.

CREATE SCHEMA IF NOT EXISTS pipeline_catalog;

CREATE TABLE IF NOT EXISTS pipeline_catalog.pipeline_job_runs (
    id                      BIGSERIAL PRIMARY KEY,

    -- Dagster run kimliği ve retry/re-execution bağlantısı
    dagster_run_id          TEXT NOT NULL UNIQUE,
    parent_run_id           TEXT,
    job_name                TEXT NOT NULL,
    run_status              TEXT NOT NULL,

    -- Mevcut MX veri kimliği
    dataset_id              TEXT,
    batch_id                TEXT,
    source_bucket           TEXT,
    source_object_key       TEXT,
    source_etag             TEXT,

    -- Bu run başladığı anda çalışan kodun sabit kimliği
    pipeline_version        TEXT NOT NULL,
    pipeline_git_tag        TEXT,
    pipeline_git_sha        TEXT NOT NULL,
    pipeline_git_dirty      BOOLEAN NOT NULL DEFAULT FALSE,
    container_image         TEXT,
    container_image_digest  TEXT,

    -- Dagster run tag'leri ve kısa hata özeti
    run_tags                JSONB NOT NULL DEFAULT '{}'::jsonb,
    failed_step             TEXT,
    error_type              TEXT,
    error_message           TEXT,

    started_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at             TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_pipeline_job_runs_status
        CHECK (
            run_status IN (
                'QUEUED',
                'NOT_STARTED',
                'MANAGED',
                'STARTING',
                'STARTED',
                'SUCCESS',
                'FAILURE',
                'CANCELING',
                'CANCELED'
            )
        ),
    CONSTRAINT chk_pipeline_job_runs_tags_object
        CHECK (jsonb_typeof(run_tags) = 'object'),
    CONSTRAINT chk_pipeline_job_runs_finished_after_started
        CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_job_runs_job_started
    ON pipeline_catalog.pipeline_job_runs (job_name, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_job_runs_status
    ON pipeline_catalog.pipeline_job_runs (run_status);

CREATE INDEX IF NOT EXISTS idx_pipeline_job_runs_dataset_batch
    ON pipeline_catalog.pipeline_job_runs (dataset_id, batch_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_job_runs_source
    ON pipeline_catalog.pipeline_job_runs (
        source_bucket,
        source_object_key,
        source_etag
    );

CREATE INDEX IF NOT EXISTS idx_pipeline_job_runs_git_sha
    ON pipeline_catalog.pipeline_job_runs (pipeline_git_sha);

CREATE INDEX IF NOT EXISTS idx_pipeline_job_runs_version
    ON pipeline_catalog.pipeline_job_runs (pipeline_version);


CREATE TABLE IF NOT EXISTS pipeline_catalog.pipeline_asset_materializations (
    id                      BIGSERIAL PRIMARY KEY,
    job_run_id              BIGINT NOT NULL REFERENCES
                                pipeline_catalog.pipeline_job_runs (id),

    asset_key               TEXT NOT NULL,
    asset_group             TEXT,
    dataset_id              TEXT,
    batch_id                TEXT,

    input_uri               TEXT,
    input_etag              TEXT,
    output_uri              TEXT,
    output_etag             TEXT,

    row_count               BIGINT,
    column_count            INTEGER,
    part_count              INTEGER,
    output_size_bytes       BIGINT,

    -- Asset'e özel alanlar: temizlik sayaçları, sıkıştırma oranı,
    -- validation istatistikleri, rapor URI'si ve DVC ayrıntıları.
    metadata                JSONB NOT NULL DEFAULT '{}'::jsonb,
    materialized_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_pipeline_asset_materialization
        UNIQUE (job_run_id, asset_key),
    CONSTRAINT chk_pipeline_asset_metadata_object
        CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT chk_pipeline_asset_row_count
        CHECK (row_count IS NULL OR row_count >= 0),
    CONSTRAINT chk_pipeline_asset_column_count
        CHECK (column_count IS NULL OR column_count >= 0),
    CONSTRAINT chk_pipeline_asset_part_count
        CHECK (part_count IS NULL OR part_count >= 0),
    CONSTRAINT chk_pipeline_asset_output_size
        CHECK (output_size_bytes IS NULL OR output_size_bytes >= 0)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_asset_key_materialized
    ON pipeline_catalog.pipeline_asset_materializations (
        asset_key,
        materialized_at DESC
    );

CREATE INDEX IF NOT EXISTS idx_pipeline_asset_dataset_batch
    ON pipeline_catalog.pipeline_asset_materializations (
        dataset_id,
        batch_id
    );

CREATE INDEX IF NOT EXISTS idx_pipeline_asset_input
    ON pipeline_catalog.pipeline_asset_materializations (
        input_uri,
        input_etag
    );
