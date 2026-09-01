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
    repository_git_sha      TEXT NOT NULL,
    repository_git_dirty    BOOLEAN NOT NULL DEFAULT FALSE,
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

-- Existing named volumes are migrated in place. For historical rows the old
-- pipeline SHA was also the only repository SHA that had been recorded.
ALTER TABLE pipeline_catalog.pipeline_job_runs
    ADD COLUMN IF NOT EXISTS repository_git_sha TEXT;

UPDATE pipeline_catalog.pipeline_job_runs
SET repository_git_sha = pipeline_git_sha
WHERE repository_git_sha IS NULL;

ALTER TABLE pipeline_catalog.pipeline_job_runs
    ALTER COLUMN repository_git_sha SET NOT NULL;

ALTER TABLE pipeline_catalog.pipeline_job_runs
    ADD COLUMN IF NOT EXISTS repository_git_dirty BOOLEAN NOT NULL DEFAULT FALSE;

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

CREATE INDEX IF NOT EXISTS idx_pipeline_job_runs_repository_git_sha
    ON pipeline_catalog.pipeline_job_runs (repository_git_sha);

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


-- Dashboard compatibility/history table.
--
-- pipeline_asset_materializations is the canonical catalog.  The dashboard's
-- Metadata Geçmişi section historically read public.asset_metadata_history,
-- so keep that query contract as a synchronized projection instead of asking
-- the dashboard to reconstruct lineage fields on every request.
CREATE TABLE IF NOT EXISTS public.asset_metadata_history (
    id                              BIGSERIAL PRIMARY KEY,
    pipeline_materialization_id     BIGINT,
    asset_key                       TEXT NOT NULL,
    group_name                      TEXT,
    partition_date                  DATE,
    flight_id                       TEXT,
    run_id                          TEXT NOT NULL,
    row_count                       BIGINT,
    metadata                        JSONB NOT NULL DEFAULT '{}'::jsonb,
    materialized_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Existing installations may still have the earlier history table shape.
-- Add the synchronization key without discarding any old rows.
ALTER TABLE public.asset_metadata_history
    ADD COLUMN IF NOT EXISTS pipeline_materialization_id BIGINT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_asset_metadata_pipeline_materialization
    ON public.asset_metadata_history (pipeline_materialization_id);

CREATE INDEX IF NOT EXISTS idx_asset_metadata_asset_materialized
    ON public.asset_metadata_history (asset_key, materialized_at DESC);

CREATE INDEX IF NOT EXISTS idx_asset_metadata_flight
    ON public.asset_metadata_history (flight_id);

CREATE INDEX IF NOT EXISTS idx_asset_metadata_partition_date
    ON public.asset_metadata_history (partition_date);


CREATE OR REPLACE FUNCTION pipeline_catalog.sync_asset_metadata_history()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    resolved_run_id          TEXT;
    resolved_flight_id       TEXT;
    resolved_partition_date  DATE;
    enriched_metadata        JSONB;
BEGIN
    SELECT dagster_run_id
    INTO resolved_run_id
    FROM pipeline_catalog.pipeline_job_runs
    WHERE id = NEW.job_run_id;

    resolved_flight_id := NULLIF(NEW.metadata ->> 'flight_id', '');
    IF resolved_flight_id IS NULL
       AND jsonb_typeof(NEW.metadata -> 'flight_ids') = 'array' THEN
        resolved_flight_id := NULLIF(NEW.metadata -> 'flight_ids' ->> 0, '');
    END IF;
    IF resolved_flight_id IS NULL THEN
        resolved_flight_id := substring(
            COALESCE(NEW.batch_id, '')
            FROM '(flight_[1-9][0-9]*_[0-9]{4}-[0-9]{2}-[0-9]{2})'
        );
    END IF;

    IF COALESCE(NEW.metadata ->> 'partition_date', '')
       ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
        resolved_partition_date := (NEW.metadata ->> 'partition_date')::DATE;
    ELSIF COALESCE(resolved_flight_id, '')
          ~ '[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
        resolved_partition_date := substring(
            resolved_flight_id
            FROM '([0-9]{4}-[0-9]{2}-[0-9]{2})$'
        )::DATE;
    ELSE
        resolved_partition_date := NEW.materialized_at::DATE;
    END IF;

    enriched_metadata := jsonb_strip_nulls(
        jsonb_build_object(
            'dataset_id', NEW.dataset_id,
            'batch_id', NEW.batch_id,
            'input_uri', NEW.input_uri,
            'input_etag', NEW.input_etag,
            'output_uri', NEW.output_uri,
            'output_etag', NEW.output_etag,
            'column_count', NEW.column_count,
            'part_count', NEW.part_count,
            'output_size_bytes', NEW.output_size_bytes
        )
    ) || COALESCE(NEW.metadata, '{}'::jsonb);

    INSERT INTO public.asset_metadata_history (
        pipeline_materialization_id,
        asset_key,
        group_name,
        partition_date,
        flight_id,
        run_id,
        row_count,
        metadata,
        materialized_at
    )
    VALUES (
        NEW.id,
        NEW.asset_key,
        NEW.asset_group,
        resolved_partition_date,
        resolved_flight_id,
        resolved_run_id,
        NEW.row_count,
        enriched_metadata,
        NEW.materialized_at
    )
    ON CONFLICT (pipeline_materialization_id) DO UPDATE SET
        asset_key = EXCLUDED.asset_key,
        group_name = EXCLUDED.group_name,
        partition_date = EXCLUDED.partition_date,
        flight_id = EXCLUDED.flight_id,
        run_id = EXCLUDED.run_id,
        row_count = EXCLUDED.row_count,
        metadata = EXCLUDED.metadata,
        materialized_at = EXCLUDED.materialized_at;

    RETURN NEW;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'trg_sync_asset_metadata_history'
          AND tgrelid =
              'pipeline_catalog.pipeline_asset_materializations'::regclass
    ) THEN
        CREATE TRIGGER trg_sync_asset_metadata_history
        AFTER INSERT OR UPDATE
        ON pipeline_catalog.pipeline_asset_materializations
        FOR EACH ROW
        EXECUTE FUNCTION pipeline_catalog.sync_asset_metadata_history();
    END IF;
END;
$$;

-- Backfill materializations that were written while the compatibility table
-- was absent. Future writes are handled by the trigger above.
INSERT INTO public.asset_metadata_history (
    pipeline_materialization_id,
    asset_key,
    group_name,
    partition_date,
    flight_id,
    run_id,
    row_count,
    metadata,
    materialized_at
)
SELECT
    materialization.id,
    materialization.asset_key,
    materialization.asset_group,
    COALESCE(
        CASE
            WHEN COALESCE(materialization.metadata ->> 'partition_date', '')
                 ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN (materialization.metadata ->> 'partition_date')::DATE
        END,
        CASE
            WHEN COALESCE(materialization.batch_id, '')
                 ~ '[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN substring(
                materialization.batch_id
                FROM '([0-9]{4}-[0-9]{2}-[0-9]{2})$'
            )::DATE
        END,
        materialization.materialized_at::DATE
    ),
    COALESCE(
        NULLIF(materialization.metadata ->> 'flight_id', ''),
        CASE
            WHEN jsonb_typeof(materialization.metadata -> 'flight_ids') = 'array'
            THEN NULLIF(materialization.metadata -> 'flight_ids' ->> 0, '')
        END,
        substring(
            COALESCE(materialization.batch_id, '')
            FROM '(flight_[1-9][0-9]*_[0-9]{4}-[0-9]{2}-[0-9]{2})'
        )
    ),
    job_run.dagster_run_id,
    materialization.row_count,
    jsonb_strip_nulls(
        jsonb_build_object(
            'dataset_id', materialization.dataset_id,
            'batch_id', materialization.batch_id,
            'input_uri', materialization.input_uri,
            'input_etag', materialization.input_etag,
            'output_uri', materialization.output_uri,
            'output_etag', materialization.output_etag,
            'column_count', materialization.column_count,
            'part_count', materialization.part_count,
            'output_size_bytes', materialization.output_size_bytes
        )
    ) || COALESCE(materialization.metadata, '{}'::jsonb),
    materialization.materialized_at
FROM pipeline_catalog.pipeline_asset_materializations AS materialization
JOIN pipeline_catalog.pipeline_job_runs AS job_run
  ON job_run.id = materialization.job_run_id
WHERE NOT EXISTS (
    SELECT 1
    FROM public.asset_metadata_history AS history
    WHERE history.pipeline_materialization_id = materialization.id
)
ON CONFLICT (pipeline_materialization_id) DO NOTHING;
