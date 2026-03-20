-- BARCA PostgreSQL Hardening v1
-- Eseguire su database "barca" (DataGrip -> Query Console).

SET lock_timeout = '5s';
SET statement_timeout = '2min';

-- ---------------------------------------------------------------------------
-- 1) Data quality constraints (NOT VALID per rollout sicuro)
-- ---------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_etl_run_status') THEN
        ALTER TABLE public.etl_run
            ADD CONSTRAINT chk_etl_run_status
            CHECK (status IN ('running', 'completed', 'failed')) NOT VALID;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_dim_shop_fascia_range') THEN
        ALTER TABLE public.dim_shop
            ADD CONSTRAINT chk_dim_shop_fascia_range
            CHECK (fascia IS NULL OR fascia BETWEEN 1 AND 7) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_dim_shop_mq_nonneg') THEN
        ALTER TABLE public.dim_shop
            ADD CONSTRAINT chk_dim_shop_mq_nonneg
            CHECK (mq IS NULL OR mq >= 0) NOT VALID;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_sales_nonneg') THEN
        ALTER TABLE public.fact_sales_snapshot
            ADD CONSTRAINT chk_sales_nonneg
            CHECK (
                (consegnato_qty IS NULL OR consegnato_qty >= 0) AND
                (venduto_qty IS NULL OR venduto_qty >= 0) AND
                (periodo_qty IS NULL OR periodo_qty >= 0) AND
                (altro_venduto_qty IS NULL OR altro_venduto_qty >= 0) AND
                (valore_1 IS NULL OR valore_1 >= 0) AND
                (valore_2 IS NULL OR valore_2 >= 0) AND
                (valore_3 IS NULL OR valore_3 >= 0) AND
                (valore_4 IS NULL OR valore_4 >= 0)
            ) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_sales_sellout_range') THEN
        ALTER TABLE public.fact_sales_snapshot
            ADD CONSTRAINT chk_sales_sellout_range
            CHECK (
                (sellout_percent IS NULL OR (sellout_percent >= 0 AND sellout_percent <= 250)) AND
                (sellout_clamped IS NULL OR (sellout_clamped >= 0 AND sellout_clamped <= 100))
            ) NOT VALID;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_stock_nonneg') THEN
        ALTER TABLE public.fact_stock_snapshot
            ADD CONSTRAINT chk_stock_nonneg
            CHECK (
                (ricevuto IS NULL OR ricevuto >= 0) AND
                (giacenza IS NULL OR giacenza >= 0) AND
                (consegnato IS NULL OR consegnato >= 0) AND
                (venduto IS NULL OR venduto >= 0) AND
                (size_35 IS NULL OR size_35 >= 0) AND
                (size_36 IS NULL OR size_36 >= 0) AND
                (size_37 IS NULL OR size_37 >= 0) AND
                (size_38 IS NULL OR size_38 >= 0) AND
                (size_39 IS NULL OR size_39 >= 0) AND
                (size_40 IS NULL OR size_40 >= 0) AND
                (size_41 IS NULL OR size_41 >= 0) AND
                (size_42 IS NULL OR size_42 >= 0) AND
                (valore_giac IS NULL OR valore_giac >= 0)
            ) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_stock_sellout_range') THEN
        ALTER TABLE public.fact_stock_snapshot
            ADD CONSTRAINT chk_stock_sellout_range
            CHECK (sellout_percent IS NULL OR (sellout_percent >= 0 AND sellout_percent <= 250)) NOT VALID;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_transfer_qty_positive') THEN
        ALTER TABLE public.fact_transfer_suggestion
            ADD CONSTRAINT chk_transfer_qty_positive
            CHECK (qty > 0) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_transfer_size_range') THEN
        ALTER TABLE public.fact_transfer_suggestion
            ADD CONSTRAINT chk_transfer_size_range
            CHECK (size IS NULL OR size BETWEEN 30 AND 60) NOT VALID;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_feature_nonneg') THEN
        ALTER TABLE public.fact_feature_state
            ADD CONSTRAINT chk_feature_nonneg
            CHECK (
                (periodo_qty IS NULL OR periodo_qty >= 0) AND
                (stock_after IS NULL OR stock_after >= 0) AND
                (shop_capacity_pairs IS NULL OR shop_capacity_pairs >= 0) AND
                (shop_capacity_target IS NULL OR shop_capacity_target >= 0) AND
                (shop_free_capacity_after IS NULL OR shop_free_capacity_after >= 0) AND
                (shop_inbound_budget IS NULL OR shop_inbound_budget >= 0) AND
                (shop_outbound_budget IS NULL OR shop_outbound_budget >= 0) AND
                (shop_inbound_used IS NULL OR shop_inbound_used >= 0) AND
                (shop_outbound_used IS NULL OR shop_outbound_used >= 0) AND
                (size_35 IS NULL OR size_35 >= 0) AND
                (size_36 IS NULL OR size_36 >= 0) AND
                (size_37 IS NULL OR size_37 >= 0) AND
                (size_38 IS NULL OR size_38 >= 0) AND
                (size_39 IS NULL OR size_39 >= 0) AND
                (size_40 IS NULL OR size_40 >= 0) AND
                (size_41 IS NULL OR size_41 >= 0) AND
                (size_42 IS NULL OR size_42 >= 0)
            ) NOT VALID;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_order_forecast_nonneg') THEN
        ALTER TABLE public.fact_order_forecast
            ADD CONSTRAINT chk_order_forecast_nonneg
            CHECK (
                (totale_qty IS NULL OR totale_qty >= 0) AND
                (predizione_vendite IS NULL OR predizione_vendite >= 0) AND
                (prezzo_acquisto IS NULL OR prezzo_acquisto >= 0) AND
                (budget_acquisto IS NULL OR budget_acquisto >= 0)
            ) NOT VALID;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_order_forecast_size_nonneg') THEN
        ALTER TABLE public.fact_order_forecast_size
            ADD CONSTRAINT chk_order_forecast_size_nonneg
            CHECK (qty >= 0 AND size BETWEEN 30 AND 60) NOT VALID;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_order_source_nonneg') THEN
        ALTER TABLE public.fact_order_source
            ADD CONSTRAINT chk_order_source_nonneg
            CHECK (
                (venduto_totale IS NULL OR venduto_totale >= 0) AND
                (venduto_periodo IS NULL OR venduto_periodo >= 0) AND
                (giacenza IS NULL OR giacenza >= 0) AND
                (venduto_extra IS NULL OR venduto_extra >= 0) AND
                (prezzo_acquisto IS NULL OR prezzo_acquisto >= 0)
            ) NOT VALID;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_order_source_size_nonneg') THEN
        ALTER TABLE public.fact_order_source_size
            ADD CONSTRAINT chk_order_source_size_nonneg
            CHECK (venduto_qty >= 0 AND size BETWEEN 30 AND 60) NOT VALID;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- 2) Performance indexes (query tipiche operative/reporting)
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_etl_run_status_started ON public.etl_run (status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_etl_run_finished ON public.etl_run (finished_at DESC);

CREATE INDEX IF NOT EXISTS idx_sales_run_shop ON public.fact_sales_snapshot (run_id, shop_code);
CREATE INDEX IF NOT EXISTS idx_sales_run_article ON public.fact_sales_snapshot (run_id, article_code);
CREATE INDEX IF NOT EXISTS idx_stock_run_shop ON public.fact_stock_snapshot (run_id, shop_code);
CREATE INDEX IF NOT EXISTS idx_stock_run_article ON public.fact_stock_snapshot (run_id, article_code);
CREATE INDEX IF NOT EXISTS idx_transfer_run_to_from ON public.fact_transfer_suggestion (run_id, to_shop_code, from_shop_code);
CREATE INDEX IF NOT EXISTS idx_feature_run_shop ON public.fact_feature_state (run_id, shop_code);

CREATE INDEX IF NOT EXISTS idx_order_forecast_run_modeseason ON public.fact_order_forecast (run_id, module, season_code, mode);
CREATE INDEX IF NOT EXISTS idx_order_forecast_modeseason_article ON public.fact_order_forecast (module, season_code, mode, article_code);
CREATE INDEX IF NOT EXISTS idx_order_forecast_size_run_modeseason ON public.fact_order_forecast_size (run_id, module, season_code, mode);

CREATE INDEX IF NOT EXISTS idx_order_source_run_module ON public.fact_order_source (run_id, module, season_code);
CREATE INDEX IF NOT EXISTS idx_order_source_module_article ON public.fact_order_source (module, article_code);
CREATE INDEX IF NOT EXISTS idx_order_source_size_run_module ON public.fact_order_source_size (run_id, module, season_code);

CREATE INDEX IF NOT EXISTS idx_ingest_run_status ON public.ingest_file_log (run_id, status);

-- ---------------------------------------------------------------------------
-- 3) Views operative
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW public.vw_latest_completed_run AS
SELECT run_id, run_type, status, started_at, finished_at, metadata
FROM public.etl_run
WHERE status = 'completed'
ORDER BY COALESCE(finished_at, started_at) DESC
LIMIT 1;

CREATE OR REPLACE VIEW public.vw_latest_run_counts AS
WITH latest AS (
    SELECT run_id
    FROM public.etl_run
    WHERE status = 'completed'
    ORDER BY COALESCE(finished_at, started_at) DESC
    LIMIT 1
)
SELECT
    l.run_id,
    (SELECT count(*) FROM public.fact_sales_snapshot s WHERE s.run_id = l.run_id) AS sales_rows,
    (SELECT count(*) FROM public.fact_stock_snapshot t WHERE t.run_id = l.run_id) AS stock_rows,
    (SELECT count(*) FROM public.fact_transfer_suggestion x WHERE x.run_id = l.run_id) AS transfer_rows,
    (SELECT count(*) FROM public.fact_feature_state f WHERE f.run_id = l.run_id) AS feature_rows,
    (SELECT count(*) FROM public.fact_order_forecast o WHERE o.run_id = l.run_id) AS order_forecast_rows,
    (SELECT count(*) FROM public.fact_order_source os WHERE os.run_id = l.run_id) AS order_source_rows
FROM latest l;

-- ---------------------------------------------------------------------------
-- 4) Ruoli applicativi minimi (NOLOGIN)
-- ---------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'barca_app_ro') THEN
        CREATE ROLE barca_app_ro NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'barca_app_rw') THEN
        CREATE ROLE barca_app_rw NOLOGIN;
    END IF;
END $$;

GRANT USAGE ON SCHEMA public TO barca_app_ro, barca_app_rw;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO barca_app_ro;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO barca_app_rw;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO barca_app_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO barca_app_rw;

-- ---------------------------------------------------------------------------
-- 5) Validazione vincoli (opzionale ma raccomandata)
-- ---------------------------------------------------------------------------
-- Esegui dopo aver verificato che i dati storici rispettano i CHECK:
-- ALTER TABLE public.etl_run VALIDATE CONSTRAINT chk_etl_run_status;
-- ALTER TABLE public.dim_shop VALIDATE CONSTRAINT chk_dim_shop_fascia_range;
-- ALTER TABLE public.dim_shop VALIDATE CONSTRAINT chk_dim_shop_mq_nonneg;
-- ALTER TABLE public.fact_sales_snapshot VALIDATE CONSTRAINT chk_sales_nonneg;
-- ALTER TABLE public.fact_sales_snapshot VALIDATE CONSTRAINT chk_sales_sellout_range;
-- ALTER TABLE public.fact_stock_snapshot VALIDATE CONSTRAINT chk_stock_nonneg;
-- ALTER TABLE public.fact_stock_snapshot VALIDATE CONSTRAINT chk_stock_sellout_range;
-- ALTER TABLE public.fact_transfer_suggestion VALIDATE CONSTRAINT chk_transfer_qty_positive;
-- ALTER TABLE public.fact_transfer_suggestion VALIDATE CONSTRAINT chk_transfer_size_range;
-- ALTER TABLE public.fact_feature_state VALIDATE CONSTRAINT chk_feature_nonneg;
-- ALTER TABLE public.fact_order_forecast VALIDATE CONSTRAINT chk_order_forecast_nonneg;
-- ALTER TABLE public.fact_order_forecast_size VALIDATE CONSTRAINT chk_order_forecast_size_nonneg;
-- ALTER TABLE public.fact_order_source VALIDATE CONSTRAINT chk_order_source_nonneg;
-- ALTER TABLE public.fact_order_source_size VALIDATE CONSTRAINT chk_order_source_size_nonneg;
