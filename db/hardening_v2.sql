-- BARCA PostgreSQL Hardening v2
-- Focus: retention policy, maintenance views, and partitioning runbook helpers.

SET lock_timeout = '5s';
SET statement_timeout = '2min';

-- ---------------------------------------------------------------------------
-- 1) ETL retention and housekeeping helpers
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.db_maintenance_log (
    event_id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION public.sp_retention_preview(p_keep_last_runs INTEGER DEFAULT 90)
RETURNS TABLE (
    run_id UUID,
    run_type TEXT,
    status TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
)
LANGUAGE sql
AS $$
    WITH ranked AS (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                ORDER BY COALESCE(r.finished_at, r.started_at) DESC
            ) AS rn
        FROM public.etl_run r
        WHERE r.status IN ('completed', 'failed')
    )
    SELECT run_id, run_type, status, started_at, finished_at
    FROM ranked
    WHERE rn > GREATEST(p_keep_last_runs, 0)
    ORDER BY COALESCE(finished_at, started_at) DESC
$$;

CREATE OR REPLACE FUNCTION public.sp_retention_apply(p_keep_last_runs INTEGER DEFAULT 90)
RETURNS TABLE (
    deleted_run_id UUID
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INTEGER := 0;
BEGIN
    IF p_keep_last_runs < 0 THEN
        RAISE EXCEPTION 'p_keep_last_runs must be >= 0';
    END IF;

    RETURN QUERY
    WITH to_delete AS (
        SELECT p.run_id
        FROM public.sp_retention_preview(p_keep_last_runs) p
    ),
    deleted AS (
        DELETE FROM public.etl_run r
        USING to_delete d
        WHERE r.run_id = d.run_id
        RETURNING r.run_id
    )
    SELECT run_id FROM deleted;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO public.db_maintenance_log(event_type, payload)
    VALUES (
        'retention_apply',
        jsonb_build_object(
            'keep_last_runs', p_keep_last_runs,
            'deleted_runs', v_count
        )
    );
END
$$;

-- ---------------------------------------------------------------------------
-- 2) Operational maintenance views
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW public.vw_table_storage_mb AS
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size_pretty,
    ROUND(pg_total_relation_size(c.oid)::numeric / 1024 / 1024, 2) AS total_size_mb
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND n.nspname = 'public'
ORDER BY total_size_mb DESC;

CREATE OR REPLACE VIEW public.vw_run_fact_counts AS
SELECT
    r.run_id,
    r.run_type,
    r.status,
    r.started_at,
    r.finished_at,
    (SELECT count(*) FROM public.fact_sales_snapshot s WHERE s.run_id = r.run_id) AS sales_rows,
    (SELECT count(*) FROM public.fact_stock_snapshot t WHERE t.run_id = r.run_id) AS stock_rows,
    (SELECT count(*) FROM public.fact_transfer_suggestion x WHERE x.run_id = r.run_id) AS transfer_rows,
    (SELECT count(*) FROM public.fact_feature_state f WHERE f.run_id = r.run_id) AS feature_rows,
    (SELECT count(*) FROM public.fact_order_forecast o WHERE o.run_id = r.run_id) AS order_forecast_rows,
    (SELECT count(*) FROM public.fact_order_source os WHERE os.run_id = r.run_id) AS order_source_rows
FROM public.etl_run r;

-- ---------------------------------------------------------------------------
-- 3) Partitioning readiness (helper metadata; no destructive migration)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW public.vw_partition_candidates AS
SELECT
    c.relname AS table_name,
    ROUND(pg_total_relation_size(c.oid)::numeric / 1024 / 1024, 2) AS size_mb,
    EXISTS (
        SELECT 1
        FROM pg_partitioned_table p
        WHERE p.partrelid = c.oid
    ) AS already_partitioned
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind IN ('r', 'p')
  AND c.relname IN (
      'fact_sales_snapshot',
      'fact_stock_snapshot',
      'fact_transfer_suggestion',
      'fact_feature_state',
      'fact_order_forecast',
      'fact_order_forecast_size',
      'fact_order_source',
      'fact_order_source_size'
  )
ORDER BY size_mb DESC;

-- Partitioning runbook:
-- 1) Esegui backup completo (ops/backup_barca.ps1).
-- 2) Pianifica finestra di manutenzione.
-- 3) Migra una tabella alla volta (hash partition by run_id) con script dedicato.
-- 4) Rebuild ANALYZE e verifica query piani.
-- 5) Solo dopo verifica, elimina tabella legacy.

SELECT count(*) AS chk_constraints
FROM pg_constraint
WHERE conname LIKE 'chk_%';

SELECT count(*) AS idx_count
FROM pg_indexes
WHERE schemaname='public' AND indexname LIKE 'idx_%';

SELECT count(*) AS funcs
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname='public'
  AND p.proname IN ('sp_retention_preview','sp_retention_apply');

SELECT *
FROM public.vw_run_fact_counts
ORDER BY started_at DESC
LIMIT 5;