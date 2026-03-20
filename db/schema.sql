CREATE TABLE IF NOT EXISTS etl_run (
    run_id UUID PRIMARY KEY,
    run_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS dim_shop (
    shop_code TEXT PRIMARY KEY,
    shop_name TEXT,
    fascia INTEGER,
    mq NUMERIC(12, 2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_article (
    article_code TEXT PRIMARY KEY,
    description TEXT,
    categoria TEXT,
    tipologia TEXT,
    marchio TEXT,
    colore TEXT,
    materiale TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS article_photo (
    article_code TEXT NOT NULL REFERENCES dim_article(article_code) ON UPDATE CASCADE,
    view_type TEXT NOT NULL DEFAULT 'lateral',
    image_path TEXT NOT NULL,
    is_primary BOOLEAN NOT NULL DEFAULT TRUE,
    checksum_sha1 TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (article_code, view_type, image_path)
);

CREATE TABLE IF NOT EXISTS ingest_file_log (
    run_id UUID NOT NULL REFERENCES etl_run(run_id) ON DELETE CASCADE,
    source_path TEXT NOT NULL,
    target_path TEXT,
    file_kind TEXT,
    status TEXT NOT NULL,
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_sales_snapshot (
    run_id UUID NOT NULL REFERENCES etl_run(run_id) ON DELETE CASCADE,
    snapshot_at TIMESTAMPTZ,
    article_code TEXT NOT NULL REFERENCES dim_article(article_code) ON UPDATE CASCADE,
    shop_code TEXT NOT NULL REFERENCES dim_shop(shop_code) ON UPDATE CASCADE,
    consegnato_qty NUMERIC(14, 2),
    venduto_qty NUMERIC(14, 2),
    periodo_qty NUMERIC(14, 2),
    altro_venduto_qty NUMERIC(14, 2),
    sellout_percent NUMERIC(8, 4),
    sellout_clamped NUMERIC(8, 4),
    valore_1 NUMERIC(14, 2),
    valore_2 NUMERIC(14, 2),
    valore_3 NUMERIC(14, 2),
    valore_4 NUMERIC(14, 2),
    PRIMARY KEY (run_id, article_code, shop_code)
);

CREATE TABLE IF NOT EXISTS fact_stock_snapshot (
    run_id UUID NOT NULL REFERENCES etl_run(run_id) ON DELETE CASCADE,
    snapshot_at TIMESTAMPTZ,
    article_code TEXT NOT NULL REFERENCES dim_article(article_code) ON UPDATE CASCADE,
    shop_code TEXT NOT NULL REFERENCES dim_shop(shop_code) ON UPDATE CASCADE,
    ricevuto NUMERIC(14, 2),
    giacenza NUMERIC(14, 2),
    consegnato NUMERIC(14, 2),
    venduto NUMERIC(14, 2),
    sellout_percent NUMERIC(8, 4),
    size_35 NUMERIC(14, 2),
    size_36 NUMERIC(14, 2),
    size_37 NUMERIC(14, 2),
    size_38 NUMERIC(14, 2),
    size_39 NUMERIC(14, 2),
    size_40 NUMERIC(14, 2),
    size_41 NUMERIC(14, 2),
    size_42 NUMERIC(14, 2),
    valore_giac NUMERIC(14, 2),
    PRIMARY KEY (run_id, article_code, shop_code)
);

CREATE TABLE IF NOT EXISTS fact_transfer_suggestion (
    run_id UUID NOT NULL REFERENCES etl_run(run_id) ON DELETE CASCADE,
    article_code TEXT NOT NULL REFERENCES dim_article(article_code) ON UPDATE CASCADE,
    size INTEGER,
    from_shop_code TEXT NOT NULL REFERENCES dim_shop(shop_code) ON UPDATE CASCADE,
    to_shop_code TEXT NOT NULL REFERENCES dim_shop(shop_code) ON UPDATE CASCADE,
    reason TEXT,
    qty NUMERIC(14, 2) NOT NULL,
    PRIMARY KEY (run_id, article_code, size, from_shop_code, to_shop_code, reason)
);

CREATE TABLE IF NOT EXISTS fact_feature_state (
    run_id UUID NOT NULL REFERENCES etl_run(run_id) ON DELETE CASCADE,
    article_code TEXT NOT NULL REFERENCES dim_article(article_code) ON UPDATE CASCADE,
    shop_code TEXT NOT NULL REFERENCES dim_shop(shop_code) ON UPDATE CASCADE,
    fascia INTEGER,
    is_outlet BOOLEAN,
    role TEXT,
    demand_raw NUMERIC(14, 4),
    demand_rule NUMERIC(14, 4),
    demand_ai NUMERIC(14, 4),
    demand_blend_weight NUMERIC(10, 6),
    demand_hybrid NUMERIC(14, 4),
    demand_model_mode TEXT,
    demand_model_quality_r2 NUMERIC(10, 6),
    periodo_qty NUMERIC(14, 2),
    stock_after NUMERIC(14, 2),
    shop_capacity_pairs NUMERIC(14, 2),
    shop_capacity_target NUMERIC(14, 2),
    shop_free_capacity_after NUMERIC(14, 2),
    shop_capacity_source TEXT,
    capacity_blocked_moves NUMERIC(14, 2),
    ops_blocked_moves NUMERIC(14, 2),
    shop_inbound_budget NUMERIC(14, 2),
    shop_outbound_budget NUMERIC(14, 2),
    shop_inbound_used NUMERIC(14, 2),
    shop_outbound_used NUMERIC(14, 2),
    size_35 NUMERIC(14, 2),
    size_36 NUMERIC(14, 2),
    size_37 NUMERIC(14, 2),
    size_38 NUMERIC(14, 2),
    size_39 NUMERIC(14, 2),
    size_40 NUMERIC(14, 2),
    size_41 NUMERIC(14, 2),
    size_42 NUMERIC(14, 2),
    PRIMARY KEY (run_id, article_code, shop_code)
);

CREATE TABLE IF NOT EXISTS fact_order_forecast (
    run_id UUID NOT NULL REFERENCES etl_run(run_id) ON DELETE CASCADE,
    module TEXT NOT NULL,
    season_code TEXT,
    mode TEXT NOT NULL,
    article_code TEXT NOT NULL REFERENCES dim_article(article_code) ON UPDATE CASCADE,
    totale_qty NUMERIC(14, 2),
    predizione_vendite NUMERIC(14, 2),
    prezzo_acquisto NUMERIC(14, 2),
    budget_acquisto NUMERIC(14, 2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, module, season_code, mode, article_code)
);

CREATE TABLE IF NOT EXISTS fact_order_forecast_size (
    run_id UUID NOT NULL REFERENCES etl_run(run_id) ON DELETE CASCADE,
    module TEXT NOT NULL,
    season_code TEXT,
    mode TEXT NOT NULL,
    article_code TEXT NOT NULL REFERENCES dim_article(article_code) ON UPDATE CASCADE,
    size INTEGER NOT NULL,
    qty NUMERIC(14, 2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, module, season_code, mode, article_code, size)
);

CREATE TABLE IF NOT EXISTS fact_order_source (
    run_id UUID NOT NULL REFERENCES etl_run(run_id) ON DELETE CASCADE,
    module TEXT NOT NULL,
    season_code TEXT,
    article_code TEXT NOT NULL REFERENCES dim_article(article_code) ON UPDATE CASCADE,
    categoria TEXT,
    tipologia TEXT,
    marchio TEXT,
    colore TEXT,
    materiale TEXT,
    descrizione TEXT,
    venduto_totale NUMERIC(14, 2),
    venduto_periodo NUMERIC(14, 2),
    giacenza NUMERIC(14, 2),
    venduto_extra NUMERIC(14, 2),
    prezzo_acquisto NUMERIC(14, 2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, module, season_code, article_code)
);

CREATE TABLE IF NOT EXISTS fact_order_source_size (
    run_id UUID NOT NULL REFERENCES etl_run(run_id) ON DELETE CASCADE,
    module TEXT NOT NULL,
    season_code TEXT,
    article_code TEXT NOT NULL REFERENCES dim_article(article_code) ON UPDATE CASCADE,
    size INTEGER NOT NULL,
    venduto_qty NUMERIC(14, 2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, module, season_code, article_code, size)
);

CREATE INDEX IF NOT EXISTS idx_sales_snapshot_shop ON fact_sales_snapshot (shop_code);
CREATE INDEX IF NOT EXISTS idx_sales_snapshot_article ON fact_sales_snapshot (article_code);
CREATE INDEX IF NOT EXISTS idx_stock_snapshot_shop ON fact_stock_snapshot (shop_code);
CREATE INDEX IF NOT EXISTS idx_stock_snapshot_article ON fact_stock_snapshot (article_code);
CREATE INDEX IF NOT EXISTS idx_transfer_from_to ON fact_transfer_suggestion (from_shop_code, to_shop_code);
CREATE INDEX IF NOT EXISTS idx_feature_shop ON fact_feature_state (shop_code);
CREATE INDEX IF NOT EXISTS idx_order_forecast_article ON fact_order_forecast (article_code);
CREATE INDEX IF NOT EXISTS idx_order_forecast_size_article ON fact_order_forecast_size (article_code);
CREATE INDEX IF NOT EXISTS idx_order_source_article ON fact_order_source (article_code);
CREATE INDEX IF NOT EXISTS idx_order_source_size_article ON fact_order_source_size (article_code);
