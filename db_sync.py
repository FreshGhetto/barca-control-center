from __future__ import annotations

import argparse
import json
import math
import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None


def _require_psycopg():
    if psycopg is None:
        raise RuntimeError("psycopg non installato. Esegui: pip install -r requirements.txt")


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def get_db_dsn() -> str:
    host = _env("BARCA_DB_HOST")
    port = _env("BARCA_DB_PORT", "5432")
    dbname = _env("BARCA_DB_NAME")
    user = _env("BARCA_DB_USER")
    password = _env("BARCA_DB_PASSWORD")
    sslmode = _env("BARCA_DB_SSLMODE", "prefer")
    miss = [k for k, v in {
        "BARCA_DB_HOST": host,
        "BARCA_DB_NAME": dbname,
        "BARCA_DB_USER": user,
        "BARCA_DB_PASSWORD": password,
    }.items() if not v]
    if miss:
        raise ValueError(
            f"Variabili DB mancanti: {miss}. "
            "Imposta BARCA_DB_HOST, BARCA_DB_NAME, BARCA_DB_USER, BARCA_DB_PASSWORD."
        )
    return f"host={host} port={port} dbname={dbname} user={user} password={password} sslmode={sslmode}"


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _txt(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    s = str(v).strip()
    return s if s else None


def _article(v: Any) -> Optional[str]:
    s = _txt(v)
    return s.upper() if s else None


def _shop(v: Any) -> Optional[str]:
    s = _txt(v)
    return s.upper() if s else None


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, str) and v.strip() == "":
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _i(v: Any) -> Optional[int]:
    x = _f(v)
    return int(round(x)) if x is not None else None


def _b(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y"}:
        return True
    if s in {"0", "false", "f", "no", "n"}:
        return False
    return None


def _dt(v: Any):
    if v is None:
        return None
    try:
        t = pd.to_datetime(v, errors="coerce")
    except Exception:
        return None
    if pd.isna(t):
        return None
    return t.to_pydatetime() if hasattr(t, "to_pydatetime") else None


def _size(col: str) -> Optional[int]:
    suf = col.split("_")[-1]
    return int(suf) if suf.isdigit() else None


def _cfg_shops(root: Path) -> Dict[str, Dict[str, Any]]:
    cfg = next(
        (
            p for p in [
                root / "config" / "lista-negozi_integrato.xlsx",
                root / "config" / "lista-negozi.xlsx",
            ] if p.exists()
        ),
        None,
    )
    if cfg is None:
        return {}
    try:
        df = pd.read_excel(cfg, sheet_name=0)
    except Exception:
        return {}
    cols = {str(c).strip().lower(): c for c in df.columns}
    c_sig = next((c for k, c in cols.items() if "sig" in k or "cod" in k or "shop" in k), None)
    c_name = next((c for k, c in cols.items() if "negozi" in k or "negozio" in k or "name" in k), None)
    c_fas = next((c for k, c in cols.items() if "fascia" in k), None)
    c_mq = next((c for k, c in cols.items() if "mq" in k or "metri" in k), None)
    out: Dict[str, Dict[str, Any]] = {}
    for _, r in df.iterrows():
        code = _shop(r.get(c_sig)) if c_sig else None
        if not code:
            continue
        out[code] = {
            "shop_name": _txt(r.get(c_name)) if c_name else None,
            "fascia": _i(r.get(c_fas)) if c_fas else None,
            "mq": _f(r.get(c_mq)) if c_mq else None,
        }
    return out


def _merge_art(dest: Dict[str, Dict[str, Optional[str]]], code: Any, row: Dict[str, Any]):
    a = _article(code)
    if not a:
        return
    rec = dest.setdefault(
        a,
        {"description": None, "categoria": None, "tipologia": None, "marchio": None, "colore": None, "materiale": None},
    )
    for k in rec.keys():
        v = _txt(row.get(k))
        if v and not rec[k]:
            rec[k] = v


def _order_jobs(out_orders: Path, summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    c_season = _txt(summary.get("current", {}).get("season")) or "unknown"
    y_season = _txt(summary.get("continuativa", {}).get("season")) or "unknown"
    jobs = [
        ("orders_current_previsione_math.csv", "current", "math", c_season),
        ("orders_continuativa_previsione_math.csv", "continuativa", "math", y_season),
        ("orders_continuativa_previsione_rf.csv", "continuativa", "rf", y_season),
        ("orders_continuativa_previsione_ibrida.csv", "continuativa", "hybrid", y_season),
    ]
    out: List[Dict[str, Any]] = []
    for fn, module, mode, season in jobs:
        p = out_orders / fn
        if p.exists():
            out.append({"path": p, "module": module, "mode": mode, "season": season})
    return out


def _order_source_jobs(out_orders: Path, summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    c_season = _txt(summary.get("current", {}).get("season")) or "unknown"
    y_season = _txt(summary.get("continuativa", {}).get("season")) or "unknown"
    jobs = [
        ("orders_current_dati_originali.csv", "current", c_season),
        ("orders_continuativa_dati_originali.csv", "continuativa", y_season),
    ]
    out: List[Dict[str, Any]] = []
    for fn, module, season in jobs:
        p = out_orders / fn
        if p.exists():
            out.append({"path": p, "module": module, "season": season})
    return out


def run_db_sync(
    root: Path,
    *,
    create_schema: bool = False,
    schema_path: Optional[Path] = None,
    run_type: str = "manual_sync",
    verbose: bool = True,
) -> Dict[str, Any]:
    _require_psycopg()
    root = root.resolve()
    out = root / "output"
    out_orders = out / "orders"
    schema = schema_path or (root / "db" / "schema.sql")
    dsn = get_db_dsn()
    run_id = uuid.uuid4()

    clean_sales = _read_csv(out / "clean_sales.csv")
    clean_stock = _read_csv(out / "clean_articles.csv")
    transfers = _read_csv(out / "suggested_transfers.csv")
    features = _read_csv(out / "features_after.csv")
    ingest = _read_json(out / "ingest" / "ingest_report_latest.json")
    ord_summary = _read_json(out_orders / "orders_summary.json")
    jobs = _order_jobs(out_orders, ord_summary)
    ord_frames = [(_read_csv(j["path"]), j) for j in jobs]
    source_jobs = _order_source_jobs(out_orders, ord_summary)
    ord_source_frames = [(_read_csv(j["path"]), j) for j in source_jobs]

    cfg = _cfg_shops(root)
    shop_codes = set(cfg.keys())
    for df, cols in [
        (clean_sales, ["Shop"]),
        (clean_stock, ["Shop"]),
        (features, ["Shop"]),
        (transfers, ["From", "To"]),
    ]:
        for c in cols:
            if c in df.columns:
                for v in df[c].dropna().tolist():
                    s = _shop(v)
                    if s:
                        shop_codes.add(s)
    dim_shops = [
        (s, _txt(cfg.get(s, {}).get("shop_name")), _i(cfg.get(s, {}).get("fascia")), _f(cfg.get(s, {}).get("mq")))
        for s in sorted(shop_codes)
    ]

    arts: Dict[str, Dict[str, Optional[str]]] = {}
    if "Article" in clean_stock.columns:
        for _, r in clean_stock.iterrows():
            _merge_art(
                arts,
                r.get("Article"),
                {"description": r.get("Description"), "categoria": None, "tipologia": None, "marchio": None, "colore": None, "materiale": None},
            )
    if "Article" in clean_sales.columns:
        for _, r in clean_sales.iterrows():
            _merge_art(
                arts,
                r.get("Article"),
                {"description": None, "categoria": None, "tipologia": None, "marchio": None, "colore": None, "materiale": None},
            )
    for df in [d for d, _ in ord_source_frames if not d.empty] + [d for d, _ in ord_frames if not d.empty]:
        for _, r in df.iterrows():
            _merge_art(
                arts,
                r.get("Codice_Articolo"),
                {
                    "description": r.get("Descrizione"),
                    "categoria": r.get("Categoria"),
                    "tipologia": r.get("Tipologia"),
                    "marchio": r.get("Marchio"),
                    "colore": r.get("Colore"),
                    "materiale": r.get("Materiale"),
                },
            )
    dim_articles = [(k, v["description"], v["categoria"], v["tipologia"], v["marchio"], v["colore"], v["materiale"]) for k, v in sorted(arts.items())]

    sales_map: Dict[Tuple[str, str], Tuple[Any, ...]] = {}
    for _, r in clean_sales.iterrows():
        a, s = _article(r.get("Article")), _shop(r.get("Shop"))
        if not a or not s:
            continue
        sales_map[(a, s)] = (
            run_id, _dt(r.get("snapshot_at")), a, s, _f(r.get("Consegnato_Qty")), _f(r.get("Venduto_Qty")),
            _f(r.get("Periodo_Qty")), _f(r.get("Altro_Venduto_Qty")), _f(r.get("Sellout_Percent")),
            _f(r.get("Sellout_Clamped")), _f(r.get("Valore_1")), _f(r.get("Valore_2")), _f(r.get("Valore_3")), _f(r.get("Valore_4")),
        )
    sales_rows = list(sales_map.values())

    stock_map: Dict[Tuple[str, str], Tuple[Any, ...]] = {}
    for _, r in clean_stock.iterrows():
        a, s = _article(r.get("Article")), _shop(r.get("Shop"))
        if not a or not s:
            continue
        stock_map[(a, s)] = (
            run_id, _dt(r.get("snapshot_at")), a, s, _f(r.get("Ricevuto")), _f(r.get("Giacenza")), _f(r.get("Consegnato")), _f(r.get("Venduto")),
            _f(r.get("Sellout_Percent")), _f(r.get("Size_35")), _f(r.get("Size_36")), _f(r.get("Size_37")), _f(r.get("Size_38")),
            _f(r.get("Size_39")), _f(r.get("Size_40")), _f(r.get("Size_41")), _f(r.get("Size_42")), _f(r.get("Valore_Giac")),
        )
    stock_rows = list(stock_map.values())

    tr_map: Dict[Tuple[str, int, str, str, str], float] = {}
    for _, r in transfers.iterrows():
        a, z, fshop, tshop = _article(r.get("Article")), _i(r.get("Size")), _shop(r.get("From")), _shop(r.get("To"))
        qty, reason = _f(r.get("Qty")), _txt(r.get("Reason")) or "UNSPECIFIED"
        if not a or z is None or not fshop or not tshop or qty is None:
            continue
        k = (a, int(z), fshop, tshop, reason)
        tr_map[k] = tr_map.get(k, 0.0) + float(qty)
    tr_rows = [(run_id, a, z, fs, ts, rs, q) for (a, z, fs, ts, rs), q in tr_map.items()]

    feat_map: Dict[Tuple[str, str], Tuple[Any, ...]] = {}
    for _, r in features.iterrows():
        a, s = _article(r.get("Article")), _shop(r.get("Shop"))
        if not a or not s:
            continue
        feat_map[(a, s)] = (
            run_id, a, s, _i(r.get("Fascia")), _b(r.get("IsOutlet")), _txt(r.get("Role")), _f(r.get("DemandRaw")), _f(r.get("DemandRule")),
            _f(r.get("DemandAI")), _f(r.get("DemandBlendWeight")), _f(r.get("DemandHybrid")), _txt(r.get("DemandModelMode")),
            _f(r.get("DemandModelQualityR2")), _f(r.get("Periodo_Qty")), _f(r.get("Stock_after")), _f(r.get("ShopCapacityPairs")),
            _f(r.get("ShopCapacityTarget")), _f(r.get("ShopFreeCapacityAfter")), _txt(r.get("ShopCapacitySource")),
            _f(r.get("CapacityBlockedMoves")), _f(r.get("OpsBlockedMoves")), _f(r.get("ShopInboundBudget")), _f(r.get("ShopOutboundBudget")),
            _f(r.get("ShopInboundUsed")), _f(r.get("ShopOutboundUsed")), _f(r.get("Size_35")), _f(r.get("Size_36")),
            _f(r.get("Size_37")), _f(r.get("Size_38")), _f(r.get("Size_39")), _f(r.get("Size_40")), _f(r.get("Size_41")), _f(r.get("Size_42")),
        )
    feat_rows = list(feat_map.values())

    ord_main: Dict[Tuple[str, str, str, str], Tuple[Any, ...]] = {}
    ord_size: Dict[Tuple[str, str, str, str, int], float] = {}
    for df, meta in ord_frames:
        module, mode, season = meta["module"], meta["mode"], meta["season"]
        tot_col = "Ibrido_Totale" if mode == "hybrid" else "Da_Acquistare_Totale"
        pred_col = "Predizione_Vendite" if mode == "math" else ("Vendita_Totale_Prevista" if mode == "rf" else None)
        pref = "Ibrido_" if mode == "hybrid" else "Acquistare_"
        size_cols = [c for c in df.columns if c.startswith(pref) and _size(c) is not None]
        for _, r in df.iterrows():
            a = _article(r.get("Codice_Articolo"))
            if not a:
                continue
            ord_main[(module, season, mode, a)] = (
                run_id, module, season, mode, a, _f(r.get(tot_col)),
                _f(r.get(pred_col)) if pred_col else None, _f(r.get("Prezzo_Acquisto")), _f(r.get("Budget_Acquisto")),
            )
            for c in size_cols:
                z, q = _size(c), _f(r.get(c))
                if z is None or q is None:
                    continue
                k = (module, season, mode, a, int(z))
                ord_size[k] = ord_size.get(k, 0.0) + float(q)
    ord_main_rows = list(ord_main.values())
    ord_size_rows = [(run_id, m, s, mo, a, z, q) for (m, s, mo, a, z), q in ord_size.items() if abs(q) > 1e-12]

    ord_src_main: Dict[Tuple[str, str, str], Tuple[Any, ...]] = {}
    ord_src_size: Dict[Tuple[str, str, str, int], float] = {}
    for df, meta in ord_source_frames:
        module, season = meta["module"], meta["season"]
        size_cols = [c for c in df.columns if c.startswith("Venduto_") and _size(c) is not None]
        for _, r in df.iterrows():
            a = _article(r.get("Codice_Articolo"))
            if not a:
                continue
            ord_src_main[(module, season, a)] = (
                run_id,
                module,
                season,
                a,
                _txt(r.get("Categoria")),
                _txt(r.get("Tipologia")),
                _txt(r.get("Marchio")),
                _txt(r.get("Colore")),
                _txt(r.get("Materiale")),
                _txt(r.get("Descrizione")),
                _f(r.get("Venduto_Totale")),
                _f(r.get("Venduto_Periodo")),
                _f(r.get("Giacenza")),
                _f(r.get("Venduto_Extra")),
                _f(r.get("Prezzo_Acquisto")),
            )
            for c in size_cols:
                z, q = _size(c), _f(r.get(c))
                if z is None or q is None:
                    continue
                k = (module, season, a, int(z))
                ord_src_size[k] = ord_src_size.get(k, 0.0) + float(q)
    ord_src_main_rows = list(ord_src_main.values())
    ord_src_size_rows = [
        (run_id, m, s, a, z, q)
        for (m, s, a, z), q in ord_src_size.items()
        if abs(q) > 1e-12
    ]

    ing_rows = []
    for r in ingest.get("rows", []) if isinstance(ingest.get("rows"), list) else []:
        if isinstance(r, dict):
            ing_rows.append((run_id, _txt(r.get("source")) or "", _txt(r.get("target")), _txt(r.get("kind")), _txt(r.get("status")) or "unknown", _txt(r.get("note"))))

    if verbose:
        print(f"[DB] start run={run_id}")
    conn = psycopg.connect(dsn)
    try:
        if create_schema:
            if verbose:
                print(f"[DB] apply schema {schema}")
            with conn.cursor() as cur:
                cur.execute(schema.read_text(encoding="utf-8"))
            conn.commit()

        base_meta = {
            "root": str(root),
            "create_schema": bool(create_schema),
            "orders_jobs": [{"module": j["module"], "mode": j["mode"], "season": j["season"], "file": str(j["path"])} for j in jobs],
            "order_source_jobs": [{"module": j["module"], "season": j["season"], "file": str(j["path"])} for j in source_jobs],
        }
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO etl_run (run_id, run_type, status, metadata) VALUES (%s, %s, 'running', %s::jsonb)",
                (run_id, run_type, json.dumps(base_meta, ensure_ascii=False)),
            )
        conn.commit()

        def _exec_many(sql: str, rows: Sequence[Tuple[Any, ...]]) -> int:
            if not rows:
                return 0
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            return len(rows)

        counts = {
            "dim_shop": _exec_many(
                """
                INSERT INTO dim_shop (shop_code, shop_name, fascia, mq) VALUES (%s, %s, %s, %s)
                ON CONFLICT (shop_code) DO UPDATE SET
                  shop_name=COALESCE(EXCLUDED.shop_name, dim_shop.shop_name),
                  fascia=COALESCE(EXCLUDED.fascia, dim_shop.fascia),
                  mq=COALESCE(EXCLUDED.mq, dim_shop.mq),
                  updated_at=NOW()
                """,
                dim_shops,
            ),
            "dim_article": _exec_many(
                """
                INSERT INTO dim_article (article_code, description, categoria, tipologia, marchio, colore, materiale)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (article_code) DO UPDATE SET
                  description=COALESCE(EXCLUDED.description, dim_article.description),
                  categoria=COALESCE(EXCLUDED.categoria, dim_article.categoria),
                  tipologia=COALESCE(EXCLUDED.tipologia, dim_article.tipologia),
                  marchio=COALESCE(EXCLUDED.marchio, dim_article.marchio),
                  colore=COALESCE(EXCLUDED.colore, dim_article.colore),
                  materiale=COALESCE(EXCLUDED.materiale, dim_article.materiale),
                  updated_at=NOW()
                """,
                dim_articles,
            ),
            "fact_sales_snapshot": _exec_many(
                """
                INSERT INTO fact_sales_snapshot (
                  run_id, snapshot_at, article_code, shop_code, consegnato_qty, venduto_qty, periodo_qty, altro_venduto_qty,
                  sellout_percent, sellout_clamped, valore_1, valore_2, valore_3, valore_4
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, article_code, shop_code) DO UPDATE SET
                  snapshot_at=EXCLUDED.snapshot_at, consegnato_qty=EXCLUDED.consegnato_qty, venduto_qty=EXCLUDED.venduto_qty,
                  periodo_qty=EXCLUDED.periodo_qty, altro_venduto_qty=EXCLUDED.altro_venduto_qty,
                  sellout_percent=EXCLUDED.sellout_percent, sellout_clamped=EXCLUDED.sellout_clamped,
                  valore_1=EXCLUDED.valore_1, valore_2=EXCLUDED.valore_2, valore_3=EXCLUDED.valore_3, valore_4=EXCLUDED.valore_4
                """,
                sales_rows,
            ),
            "fact_stock_snapshot": _exec_many(
                """
                INSERT INTO fact_stock_snapshot (
                  run_id, snapshot_at, article_code, shop_code, ricevuto, giacenza, consegnato, venduto, sellout_percent,
                  size_35, size_36, size_37, size_38, size_39, size_40, size_41, size_42, valore_giac
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, article_code, shop_code) DO UPDATE SET
                  snapshot_at=EXCLUDED.snapshot_at, ricevuto=EXCLUDED.ricevuto, giacenza=EXCLUDED.giacenza, consegnato=EXCLUDED.consegnato,
                  venduto=EXCLUDED.venduto, sellout_percent=EXCLUDED.sellout_percent,
                  size_35=EXCLUDED.size_35, size_36=EXCLUDED.size_36, size_37=EXCLUDED.size_37, size_38=EXCLUDED.size_38,
                  size_39=EXCLUDED.size_39, size_40=EXCLUDED.size_40, size_41=EXCLUDED.size_41, size_42=EXCLUDED.size_42,
                  valore_giac=EXCLUDED.valore_giac
                """,
                stock_rows,
            ),
            "fact_transfer_suggestion": _exec_many(
                """
                INSERT INTO fact_transfer_suggestion (run_id, article_code, size, from_shop_code, to_shop_code, reason, qty)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, article_code, size, from_shop_code, to_shop_code, reason) DO UPDATE SET qty=EXCLUDED.qty
                """,
                tr_rows,
            ),
            "fact_feature_state": _exec_many(
                """
                INSERT INTO fact_feature_state (
                  run_id, article_code, shop_code, fascia, is_outlet, role, demand_raw, demand_rule, demand_ai, demand_blend_weight, demand_hybrid,
                  demand_model_mode, demand_model_quality_r2, periodo_qty, stock_after, shop_capacity_pairs, shop_capacity_target, shop_free_capacity_after,
                  shop_capacity_source, capacity_blocked_moves, ops_blocked_moves, shop_inbound_budget, shop_outbound_budget, shop_inbound_used, shop_outbound_used,
                  size_35, size_36, size_37, size_38, size_39, size_40, size_41, size_42
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, article_code, shop_code) DO UPDATE SET
                  fascia=EXCLUDED.fascia, is_outlet=EXCLUDED.is_outlet, role=EXCLUDED.role, demand_raw=EXCLUDED.demand_raw, demand_rule=EXCLUDED.demand_rule,
                  demand_ai=EXCLUDED.demand_ai, demand_blend_weight=EXCLUDED.demand_blend_weight, demand_hybrid=EXCLUDED.demand_hybrid,
                  demand_model_mode=EXCLUDED.demand_model_mode, demand_model_quality_r2=EXCLUDED.demand_model_quality_r2,
                  periodo_qty=EXCLUDED.periodo_qty, stock_after=EXCLUDED.stock_after, shop_capacity_pairs=EXCLUDED.shop_capacity_pairs,
                  shop_capacity_target=EXCLUDED.shop_capacity_target, shop_free_capacity_after=EXCLUDED.shop_free_capacity_after,
                  shop_capacity_source=EXCLUDED.shop_capacity_source, capacity_blocked_moves=EXCLUDED.capacity_blocked_moves,
                  ops_blocked_moves=EXCLUDED.ops_blocked_moves, shop_inbound_budget=EXCLUDED.shop_inbound_budget, shop_outbound_budget=EXCLUDED.shop_outbound_budget,
                  shop_inbound_used=EXCLUDED.shop_inbound_used, shop_outbound_used=EXCLUDED.shop_outbound_used,
                  size_35=EXCLUDED.size_35, size_36=EXCLUDED.size_36, size_37=EXCLUDED.size_37, size_38=EXCLUDED.size_38,
                  size_39=EXCLUDED.size_39, size_40=EXCLUDED.size_40, size_41=EXCLUDED.size_41, size_42=EXCLUDED.size_42
                """,
                feat_rows,
            ),
            "fact_order_forecast": _exec_many(
                """
                INSERT INTO fact_order_forecast (
                  run_id, module, season_code, mode, article_code, totale_qty, predizione_vendite, prezzo_acquisto, budget_acquisto
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, module, season_code, mode, article_code) DO UPDATE SET
                  totale_qty=EXCLUDED.totale_qty, predizione_vendite=EXCLUDED.predizione_vendite,
                  prezzo_acquisto=EXCLUDED.prezzo_acquisto, budget_acquisto=EXCLUDED.budget_acquisto
                """,
                ord_main_rows,
            ),
            "fact_order_forecast_size": _exec_many(
                """
                INSERT INTO fact_order_forecast_size (run_id, module, season_code, mode, article_code, size, qty)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, module, season_code, mode, article_code, size) DO UPDATE SET qty=EXCLUDED.qty
                """,
                ord_size_rows,
            ),
            "fact_order_source": _exec_many(
                """
                INSERT INTO fact_order_source (
                  run_id, module, season_code, article_code, categoria, tipologia, marchio, colore, materiale,
                  descrizione, venduto_totale, venduto_periodo, giacenza, venduto_extra, prezzo_acquisto
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, module, season_code, article_code) DO UPDATE SET
                  categoria=EXCLUDED.categoria, tipologia=EXCLUDED.tipologia, marchio=EXCLUDED.marchio, colore=EXCLUDED.colore,
                  materiale=EXCLUDED.materiale, descrizione=EXCLUDED.descrizione, venduto_totale=EXCLUDED.venduto_totale,
                  venduto_periodo=EXCLUDED.venduto_periodo, giacenza=EXCLUDED.giacenza, venduto_extra=EXCLUDED.venduto_extra,
                  prezzo_acquisto=EXCLUDED.prezzo_acquisto
                """,
                ord_src_main_rows,
            ),
            "fact_order_source_size": _exec_many(
                """
                INSERT INTO fact_order_source_size (run_id, module, season_code, article_code, size, venduto_qty)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, module, season_code, article_code, size) DO UPDATE SET venduto_qty=EXCLUDED.venduto_qty
                """,
                ord_src_size_rows,
            ),
            "ingest_file_log": _exec_many(
                """
                INSERT INTO ingest_file_log (run_id, source_path, target_path, file_kind, status, note)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                ing_rows,
            ),
        }
        done_meta = {**base_meta, "status": "completed", "counts": counts}
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE etl_run SET status='completed', finished_at=NOW(), metadata=%s::jsonb WHERE run_id=%s",
                (json.dumps(done_meta, ensure_ascii=False), run_id),
            )
        conn.commit()
        out_summary = {"run_id": str(run_id), "status": "completed", "counts": counts}
        if verbose:
            print(json.dumps(out_summary, indent=2, ensure_ascii=False))
        return out_summary
    except Exception as exc:
        conn.rollback()
        fail_meta = {"root": str(root), "status": "failed", "error": str(exc)}
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE etl_run SET status='failed', finished_at=NOW(), metadata=%s::jsonb WHERE run_id=%s",
                (json.dumps(fail_meta, ensure_ascii=False), run_id),
            )
        conn.commit()
        raise
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync output CSV -> PostgreSQL")
    p.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    p.add_argument("--create-schema", action="store_true")
    p.add_argument("--schema-path", type=Path, default=None)
    p.add_argument("--run-type", type=str, default="manual_sync")
    p.add_argument("--quiet", action="store_true")
    return p.parse_args()


def main():
    a = parse_args()
    try:
        res = run_db_sync(
            root=a.root,
            create_schema=bool(a.create_schema),
            schema_path=a.schema_path,
            run_type=a.run_type,
            verbose=not a.quiet,
        )
        if a.quiet:
            print(json.dumps(res, ensure_ascii=False))
    except Exception as exc:
        print(f"[DB] ERRORE: {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
