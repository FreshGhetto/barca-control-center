from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from catalog_excel import ensure_xlsx, parse_situazione_articoli_excel
from catalog_price import build_price_snapshot_from_files
from db_sync import get_db_dsn

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None


def _require_psycopg():
    if psycopg is None:
        raise RuntimeError("psycopg non installato. Esegui: pip install -r requirements.txt")


def _txt(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    return text if text else None


def _article(value: Any) -> Optional[str]:
    text = _txt(value)
    return text.upper().replace(" ", "") if text else None


def _season(value: Any) -> str:
    text = (_txt(value) or "UNKNOWN").upper().replace(" ", "")
    return text or "UNKNOWN"


def _store(value: Any) -> Optional[str]:
    text = _txt(value)
    return text.upper().replace(" ", "") if text else None


def _f(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _b(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    return text in {"1", "true", "t", "yes", "y"}


def _emit_progress(progress_cb: Optional[Callable[[Dict[str, Any]], None]], **payload: Any) -> None:
    if progress_cb is None:
        return
    try:
        progress_cb(dict(payload))
    except Exception:
        return


def _parse_catalog_excels(
    excel_files: Sequence[Path],
    sheet: int | str = 0,
    progress_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    total_files = len(excel_files)
    for idx, file_path in enumerate(excel_files, start=1):
        _emit_progress(
            progress_cb,
            stage="parsing_excel",
            file_name=file_path.name,
            current=idx,
            total=total_files,
            message=f"Parsing Excel {idx}/{total_files}: {file_path.name}",
        )
        parsed = parse_situazione_articoli_excel(ensure_xlsx(file_path), sheet=sheet)
        frames.append(parsed)
        _emit_progress(
            progress_cb,
            stage="parsing_excel",
            file_name=file_path.name,
            current=idx,
            total=total_files,
            rows=int(len(parsed)),
            message=f"Excel elaborato {idx}/{total_files}: {file_path.name}",
        )
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df["season_code"] = df.get("stagione_da", "").map(_season)
    df["season_label"] = df.get("stagione_descr", "").astype(str).str.strip()
    df["article_code"] = df.get("articolo", "").map(_article)
    df["store_code"] = df.get("neg", "").map(_store)
    df["description"] = df.get("descrizione", "").astype(str)
    df["color"] = df.get("colore", "").astype(str)
    df["supplier"] = df.get("fornitore", "").astype(str)
    df["reparto"] = df.get("reparto", "").astype(str)
    df["categoria"] = df.get("categoria", "").astype(str)
    df["tipologia"] = df.get("tipologia", "").astype(str)
    for col in ("giac", "con", "ven", "perc_ven"):
        df[col] = pd.to_numeric(df.get(col, 0.0), errors="coerce").fillna(0.0)
    df["is_total"] = df.get("is_total", 0).map(_b)
    df["synthetic_total"] = df.get("synthetic_total", 0).map(_b)
    df = df[df["article_code"].notna() & df["store_code"].notna()].copy()
    df = df.drop_duplicates(subset=["season_code", "article_code", "store_code"], keep="last")
    return df.reset_index(drop=True)


def _empty_catalog_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "source_file",
            "source_sheet",
            "season_code",
            "season_label",
            "supplier",
            "reparto",
            "categoria",
            "tipologia",
            "article_code",
            "description",
            "color",
            "store_code",
            "is_total",
            "synthetic_total",
            "giac",
            "con",
            "ven",
            "perc_ven",
            "sizes_json",
        ]
    )


def _build_size_rows(catalog_df: pd.DataFrame) -> list[Tuple[str, str, str, int, float]]:
    rows: list[Tuple[str, str, str, int, float]] = []
    for rec in catalog_df.itertuples(index=False):
        raw = getattr(rec, "sizes_json", "") or ""
        if not raw or raw in {"{}", "[]"}:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        for key, value in (data or {}).items():
            try:
                size = int(key)
                qty = float(value)
            except Exception:
                continue
            if abs(qty) <= 1e-12:
                continue
            rows.append((str(rec.season_code), str(rec.article_code), str(rec.store_code), size, qty))
    return rows


def _catalog_price_pair_scope_sql(run_id: Optional[str]) -> Tuple[str, Tuple[Any, ...]]:
    if run_id:
        return (
            """
            FROM fact_catalog_article_store_snapshot s
            WHERE s.run_id = %s::uuid
              AND COALESCE(TRIM(s.season_code), '') <> ''
            """,
            (str(run_id),),
        )
    return (
        """
        FROM fact_catalog_article_store_snapshot s
        JOIN etl_run r
          ON r.run_id = s.run_id
        WHERE r.run_type = 'catalog_import'
          AND r.status = 'completed'
          AND COALESCE(TRIM(s.season_code), '') <> ''
        """,
        (),
    )


def _normalize_catalog_price_pairs_on_conn(conn, *, run_id: Optional[str] = None) -> Dict[str, int]:
    scope_sql, params = _catalog_price_pair_scope_sql(run_id)
    cte_sql = f"""
        WITH candidate_targets AS (
          SELECT DISTINCT
            s.run_id,
            UPPER(TRIM(s.season_code)) AS season_code,
            UPPER(TRIM(s.article_code)) AS article_code
          {scope_sql}
        ),
        mapped AS (
          SELECT
            c.run_id,
            c.season_code AS target_season,
            c.article_code,
            CASE RIGHT(c.season_code, 1)
              WHEN 'G' THEN LEFT(c.season_code, 2) || 'E'
              WHEN 'E' THEN LEFT(c.season_code, 2) || 'G'
              WHEN 'Y' THEN LEFT(c.season_code, 2) || 'I'
              WHEN 'I' THEN LEFT(c.season_code, 2) || 'Y'
              ELSE NULL
            END AS source_season
          FROM candidate_targets c
        ),
        source_prices AS (
          SELECT
            m.run_id,
            m.target_season,
            m.article_code,
            src.price_listino,
            src.price_saldo
          FROM mapped m
          JOIN fact_catalog_price_snapshot src
            ON src.run_id = m.run_id
           AND UPPER(TRIM(src.season_code)) = m.source_season
           AND UPPER(TRIM(src.article_code)) = m.article_code
          WHERE m.source_season IS NOT NULL
            AND (src.price_listino IS NOT NULL OR src.price_saldo IS NOT NULL)
        )
    """

    with conn.cursor() as cur:
        cur.execute(
            cte_sql
            + """
            UPDATE fact_catalog_price_snapshot tgt
            SET
              price_listino = COALESCE(tgt.price_listino, src.price_listino),
              price_saldo = COALESCE(tgt.price_saldo, src.price_saldo)
            FROM source_prices src
            WHERE tgt.run_id = src.run_id
              AND UPPER(TRIM(tgt.season_code)) = src.target_season
              AND UPPER(TRIM(tgt.article_code)) = src.article_code
              AND (
                (tgt.price_listino IS NULL AND src.price_listino IS NOT NULL) OR
                (tgt.price_saldo IS NULL AND src.price_saldo IS NOT NULL)
              )
            """,
            params,
        )
        updated = int(cur.rowcount or 0)

        cur.execute(
            cte_sql
            + """
            INSERT INTO fact_catalog_price_snapshot (
              run_id, season_code, article_code, price_listino, price_saldo
            )
            SELECT
              src.run_id,
              src.target_season,
              src.article_code,
              src.price_listino,
              src.price_saldo
            FROM source_prices src
            LEFT JOIN fact_catalog_price_snapshot tgt
              ON tgt.run_id = src.run_id
             AND UPPER(TRIM(tgt.season_code)) = src.target_season
             AND UPPER(TRIM(tgt.article_code)) = src.article_code
            WHERE tgt.run_id IS NULL
            ON CONFLICT (run_id, season_code, article_code) DO NOTHING
            """,
            params,
        )
        inserted = int(cur.rowcount or 0)

    return {"updated": updated, "inserted": inserted}


def normalize_catalog_price_pairs(*, run_id: Optional[str] = None) -> Dict[str, int]:
    _require_psycopg()
    dsn = get_db_dsn()
    with psycopg.connect(dsn) as conn:
        stats = _normalize_catalog_price_pairs_on_conn(conn, run_id=run_id)
        conn.commit()
    return stats


def _load_current_price_snapshot(conn) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (p.season_code, p.article_code)
              p.season_code,
              p.article_code,
              p.price_listino,
              p.price_saldo
            FROM fact_catalog_price_snapshot p
            JOIN etl_run r
              ON r.run_id = p.run_id
            WHERE r.run_type = 'catalog_import'
              AND r.status = 'completed'
            ORDER BY
              p.season_code,
              p.article_code,
              COALESCE(r.finished_at, r.started_at) DESC,
              p.created_at DESC,
              p.run_id DESC
            """
        )
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=["season_code", "article_code", "price_listino_prev", "price_saldo_prev"])
    return pd.DataFrame(rows, columns=["season_code", "article_code", "price_listino_prev", "price_saldo_prev"])


def import_catalog_to_db(
    *,
    root: Path,
    excel_files: Sequence[Path],
    price_files: Sequence[Path],
    sheet: int | str = 0,
    create_schema: bool = True,
    verbose: bool = True,
    progress_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    _require_psycopg()
    if not excel_files and not price_files:
        raise ValueError("Carica almeno un file Excel catalogo (.xls/.xlsx) o un CSV prezzi.")

    root = root.resolve()
    run_id = uuid.uuid4()
    dsn = get_db_dsn()
    schema_path = root / "db" / "schema.sql"
    metadata_seed = {
        "sheet": str(sheet),
        "excel_files": [str(Path(p).name) for p in excel_files],
        "price_files": [str(Path(p).name) for p in price_files],
    }

    if verbose:
        print(f"[CATALOG] start run_id={run_id}")
    conn = psycopg.connect(dsn)
    run_inserted = False
    try:
        _emit_progress(progress_cb, stage="starting", progress=1.0, message="Preparazione import catalogo")
        if create_schema:
            _emit_progress(progress_cb, stage="schema", progress=3.0, message="Aggiornamento struttura database catalogo")
            with conn.cursor() as cur:
                cur.execute(schema_path.read_text(encoding="utf-8"))
            conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO etl_run (run_id, run_type, status, metadata) VALUES (%s, %s, 'running', %s::jsonb)",
                (run_id, "catalog_import", json.dumps(metadata_seed, ensure_ascii=False)),
            )
        conn.commit()
        run_inserted = True

        excel_total = max(len(excel_files), 1)

        def _excel_progress(event: Dict[str, Any]) -> None:
            current = float(event.get("current") or 0)
            total = float(event.get("total") or excel_total)
            progress = 5.0 + (current / max(total, 1.0)) * 45.0
            _emit_progress(progress_cb, progress=progress, **event)

        def _price_progress(event: Dict[str, Any]) -> None:
            current = float(event.get("current") or 0)
            total = float(event.get("total") or max(len(price_files), 1))
            progress = 50.0 + (current / max(total, 1.0)) * 16.0
            _emit_progress(progress_cb, progress=progress, **event)

        catalog_df = _parse_catalog_excels(excel_files, sheet=sheet, progress_cb=_excel_progress) if excel_files else _empty_catalog_df()
        if not excel_files:
            _emit_progress(progress_cb, stage="parsing_excel", progress=50.0, message="Nessun file Excel catalogo da elaborare")

        price_df, price_stats = build_price_snapshot_from_files(price_files, progress_cb=_price_progress)
        if not price_files:
            _emit_progress(progress_cb, stage="parsing_price", progress=66.0, message="Nessun CSV prezzi da elaborare")
        if catalog_df.empty and price_df.empty:
            raise RuntimeError("Import catalogo vuoto: nessuna riga utile trovata nei file caricati.")

        _emit_progress(progress_cb, stage="preparing_rows", progress=70.0, message="Preparazione righe per il database")
        _emit_progress(progress_cb, stage="normalizing_existing_prices", progress=71.0, message="Allineamento prezzi stagioni gemelle già presenti")
        existing_price_normalization = _normalize_catalog_price_pairs_on_conn(conn)

        catalog_seasons = {str(x) for x in catalog_df["season_code"].dropna().astype(str).tolist() if str(x).strip()}
        if len(catalog_seasons) == 1 and not price_df.empty:
            price_df.loc[price_df["season_code"] == "UNKNOWN", "season_code"] = next(iter(catalog_seasons))
        if not price_df.empty:
            current_prices = _load_current_price_snapshot(conn)
            if not current_prices.empty:
                price_df = price_df.merge(current_prices, on=["season_code", "article_code"], how="left")
                price_df["price_listino"] = price_df["price_listino"].where(price_df["price_listino"].notna(), price_df["price_listino_prev"])
                price_df["price_saldo"] = price_df["price_saldo"].where(price_df["price_saldo"].notna(), price_df["price_saldo_prev"])
                price_df = price_df.drop(columns=["price_listino_prev", "price_saldo_prev"])
        season_values = sorted(
            {
                str(x)
                for x in pd.concat(
                    [
                        catalog_df.get("season_code", pd.Series(dtype="object")),
                        price_df.get("season_code", pd.Series(dtype="object")),
                    ],
                    ignore_index=True,
                ).dropna()
                if str(x).strip()
            }
        )

        size_rows_base = _build_size_rows(catalog_df)

        article_meta = (
            catalog_df[
                [
                    "article_code",
                    "description",
                    "categoria",
                    "tipologia",
                    "color",
                ]
            ]
            .drop_duplicates(subset=["article_code"], keep="last")
            .reset_index(drop=True)
        )
        if not price_df.empty:
            price_only = price_df.loc[~price_df["article_code"].isin(article_meta["article_code"]), ["article_code"]].copy()
            if not price_only.empty:
                price_only["description"] = None
                price_only["categoria"] = None
                price_only["tipologia"] = None
                price_only["color"] = None
                article_meta = pd.concat([article_meta, price_only], ignore_index=True)

        dim_articles = []
        for rec in article_meta.itertuples(index=False):
            dim_articles.append(
                (
                    rec.article_code,
                    _txt(rec.description),
                    _txt(rec.categoria),
                    _txt(rec.tipologia),
                    None,
                    _txt(rec.color),
                    None,
                )
            )

        dim_shops = []
        for shop_code in sorted({code for code in catalog_df["store_code"].astype(str).tolist() if code and code != "XX"}):
            dim_shops.append((shop_code, None, None, None))

        snapshot_rows = []
        for rec in catalog_df.itertuples(index=False):
            snapshot_rows.append(
                (
                    run_id,
                    _txt(rec.source_file),
                    _txt(rec.source_sheet),
                    str(rec.season_code),
                    _txt(rec.season_label),
                    _txt(rec.supplier),
                    _txt(rec.reparto),
                    _txt(rec.categoria),
                    _txt(rec.tipologia),
                    str(rec.article_code),
                    _txt(rec.description),
                    _txt(rec.color),
                    str(rec.store_code),
                    bool(rec.is_total),
                    bool(rec.synthetic_total),
                    _f(rec.giac),
                    _f(rec.con),
                    _f(rec.ven),
                    _f(rec.perc_ven),
                )
            )

        size_rows = [(run_id, season_code, article_code, store_code, size, qty) for season_code, article_code, store_code, size, qty in size_rows_base]

        price_rows = []
        for rec in price_df.itertuples(index=False):
            price_rows.append(
                (
                    run_id,
                    str(rec.season_code),
                    str(rec.article_code),
                    _f(rec.price_listino),
                    _f(rec.price_saldo),
                )
            )

        file_log_rows = []
        for path in excel_files:
            file_log_rows.append((run_id, path.name, "catalog_excel", None, None))
        for path in price_files:
            name = path.name.lower()
            role = "price_csv"
            if "listino" in name:
                role = "price_listino_csv"
            elif "saldo" in name:
                role = "price_saldo_csv"
            file_log_rows.append((run_id, path.name, role, None, None))

        def _exec_many(
            sql: str,
            rows: Sequence[Tuple[Any, ...]],
            *,
            stage: str,
            message: str,
            progress_from: float,
            progress_to: float,
            batch_size: int = 5000,
        ) -> int:
            if not rows:
                _emit_progress(progress_cb, stage=stage, progress=progress_to, message=f"{message}: nessuna riga")
                return 0
            total_rows = len(rows)
            done_rows = 0
            with conn.cursor() as cur:
                for start in range(0, total_rows, batch_size):
                    batch = rows[start : start + batch_size]
                    cur.executemany(sql, batch)
                    done_rows += len(batch)
                    frac = done_rows / max(total_rows, 1)
                    progress = progress_from + (progress_to - progress_from) * frac
                    _emit_progress(
                        progress_cb,
                        stage=stage,
                        progress=progress,
                        message=f"{message}: {done_rows}/{total_rows}",
                        rows_done=done_rows,
                        rows_total=total_rows,
                    )
            return len(rows)

        counts = {
            "dim_shop": _exec_many(
                """
                INSERT INTO dim_shop (shop_code, shop_name, fascia, mq)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (shop_code) DO UPDATE SET
                  shop_name = COALESCE(EXCLUDED.shop_name, dim_shop.shop_name),
                  fascia = COALESCE(EXCLUDED.fascia, dim_shop.fascia),
                  mq = COALESCE(EXCLUDED.mq, dim_shop.mq),
                  updated_at = NOW()
                """,
                dim_shops,
                stage="writing_dim_shop",
                message="Aggiornamento negozi",
                progress_from=72.0,
                progress_to=74.0,
            ),
            "dim_article": _exec_many(
                """
                INSERT INTO dim_article (article_code, description, categoria, tipologia, marchio, colore, materiale)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (article_code) DO UPDATE SET
                  description = COALESCE(EXCLUDED.description, dim_article.description),
                  categoria = COALESCE(EXCLUDED.categoria, dim_article.categoria),
                  tipologia = COALESCE(EXCLUDED.tipologia, dim_article.tipologia),
                  marchio = COALESCE(EXCLUDED.marchio, dim_article.marchio),
                  colore = COALESCE(EXCLUDED.colore, dim_article.colore),
                  materiale = COALESCE(EXCLUDED.materiale, dim_article.materiale),
                  updated_at = NOW()
                """,
                dim_articles,
                stage="writing_dim_article",
                message="Aggiornamento anagrafica articoli",
                progress_from=74.0,
                progress_to=76.0,
            ),
            "fact_catalog_article_store_snapshot": _exec_many(
                """
                INSERT INTO fact_catalog_article_store_snapshot (
                  run_id, source_file, source_sheet, season_code, season_label, supplier, reparto, categoria, tipologia,
                  article_code, description, color, store_code, is_total, synthetic_total, giac, con, ven, perc_ven
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, season_code, article_code, store_code) DO UPDATE SET
                  source_file = EXCLUDED.source_file,
                  source_sheet = EXCLUDED.source_sheet,
                  season_label = EXCLUDED.season_label,
                  supplier = EXCLUDED.supplier,
                  reparto = EXCLUDED.reparto,
                  categoria = EXCLUDED.categoria,
                  tipologia = EXCLUDED.tipologia,
                  description = EXCLUDED.description,
                  color = EXCLUDED.color,
                  is_total = EXCLUDED.is_total,
                  synthetic_total = EXCLUDED.synthetic_total,
                  giac = EXCLUDED.giac,
                  con = EXCLUDED.con,
                  ven = EXCLUDED.ven,
                  perc_ven = EXCLUDED.perc_ven
                """,
                snapshot_rows,
                stage="writing_store_snapshot",
                message="Scrittura snapshot articoli/negozi",
                progress_from=76.0,
                progress_to=88.0,
            ),
            "fact_catalog_article_store_size_snapshot": _exec_many(
                """
                INSERT INTO fact_catalog_article_store_size_snapshot (
                  run_id, season_code, article_code, store_code, size, qty
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, season_code, article_code, store_code, size) DO UPDATE SET
                  qty = EXCLUDED.qty
                """,
                size_rows,
                stage="writing_size_snapshot",
                message="Scrittura dettaglio taglie",
                progress_from=88.0,
                progress_to=94.0,
            ),
            "fact_catalog_price_snapshot": _exec_many(
                """
                INSERT INTO fact_catalog_price_snapshot (
                  run_id, season_code, article_code, price_listino, price_saldo
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (run_id, season_code, article_code) DO UPDATE SET
                  price_listino = EXCLUDED.price_listino,
                  price_saldo = EXCLUDED.price_saldo
                """,
                price_rows,
                stage="writing_price_snapshot",
                message="Scrittura prezzi listino/saldo",
                progress_from=94.0,
                progress_to=97.0,
            ),
            "catalog_import_file_log": _exec_many(
                """
                INSERT INTO catalog_import_file_log (run_id, file_name, file_role, season_code, note)
                VALUES (%s, %s, %s, %s, %s)
                """,
                file_log_rows,
                stage="writing_file_log",
                message="Registrazione file importati",
                progress_from=97.0,
                progress_to=98.0,
            ),
        }
        _emit_progress(progress_cb, stage="normalizing_current_prices", progress=97.5, message="Allineamento prezzi stagioni gemelle della run corrente")
        current_run_price_normalization = _normalize_catalog_price_pairs_on_conn(conn, run_id=str(run_id))
        final_metadata = {
            **metadata_seed,
            "catalog_seasons": season_values,
            "price_stats": price_stats,
            "status": "completed",
            "counts": counts,
            "price_normalization": {
                "existing_runs": existing_price_normalization,
                "current_run": current_run_price_normalization,
            },
        }
        counts["fact_catalog_price_snapshot_normalized_existing"] = (
            int(existing_price_normalization.get("updated", 0)) + int(existing_price_normalization.get("inserted", 0))
        )
        counts["fact_catalog_price_snapshot_normalized_current_run"] = (
            int(current_run_price_normalization.get("updated", 0)) + int(current_run_price_normalization.get("inserted", 0))
        )
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE etl_run SET status='completed', finished_at=NOW(), metadata=%s::jsonb WHERE run_id=%s",
                (json.dumps(final_metadata, ensure_ascii=False), run_id),
            )
        conn.commit()
        _emit_progress(progress_cb, stage="completed", progress=100.0, message="Import catalogo completato")
        return {
            "run_id": str(run_id),
            "status": "completed",
            "counts": counts,
            "catalog_seasons": season_values,
            "price_stats": price_stats,
        }
    except Exception as exc:
        conn.rollback()
        if run_inserted:
            failure_metadata = {**metadata_seed, "status": "failed", "error": str(exc)}
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE etl_run SET status='failed', finished_at=NOW(), metadata=%s::jsonb WHERE run_id=%s",
                    (json.dumps(failure_metadata, ensure_ascii=False), run_id),
                )
            conn.commit()
        raise
    finally:
        conn.close()


def _pick_catalog_run_id(conn, explicit_run_id: Optional[str] = None) -> str:
    if explicit_run_id:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_id
                FROM etl_run
                WHERE run_id = %s::uuid
                  AND run_type = 'catalog_import'
                LIMIT 1
                """,
                (explicit_run_id,),
            )
            row = cur.fetchone()
        if not row:
            raise ValueError(f"run_id catalogo {explicit_run_id} non trovato.")
        return str(row[0])

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id
            FROM etl_run
            WHERE run_type = 'catalog_import'
              AND status = 'completed'
            ORDER BY COALESCE(finished_at, started_at) DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError("Nessuna importazione catalogo disponibile.")
    return str(row[0])


def _catalog_facets(conn, source_run_id: Optional[str] = None) -> Dict[str, List[str]]:
    source_name = "fact_catalog_article_store_snapshot" if source_run_id else "vw_catalog_article_store_current"
    where_parts = ["store_code = 'XX'"]
    params: List[Any] = []
    if source_run_id:
        where_parts.insert(0, "run_id = %s::uuid")
        params.append(source_run_id)
    where_sql = " AND ".join(where_parts)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT season_code
            FROM {source_name}
            WHERE {where_sql}
            ORDER BY season_code
            """,
            tuple(params),
        )
        seasons = [str(row[0]) for row in cur.fetchall() if row and row[0]]
        cur.execute(
            f"""
            SELECT DISTINCT reparto
            FROM {source_name}
            WHERE {where_sql}
              AND reparto IS NOT NULL
              AND reparto <> ''
            ORDER BY reparto
            """,
            tuple(params),
        )
        reparti = [str(row[0]) for row in cur.fetchall() if row and row[0]]
        cur.execute(
            f"""
            SELECT DISTINCT supplier
            FROM {source_name}
            WHERE {where_sql}
              AND supplier IS NOT NULL
              AND supplier <> ''
            ORDER BY supplier
            """,
            tuple(params),
        )
        suppliers = [str(row[0]) for row in cur.fetchall() if row and row[0]]
        cur.execute(
            f"""
            SELECT DISTINCT categoria
            FROM {source_name}
            WHERE {where_sql}
              AND categoria IS NOT NULL
              AND categoria <> ''
            ORDER BY categoria
            """,
            tuple(params),
        )
        categorie = [str(row[0]) for row in cur.fetchall() if row and row[0]]
    return {"seasons": seasons, "reparti": reparti, "suppliers": suppliers, "categorie": categorie}


def get_catalog_status(source_run_id: Optional[str] = None) -> Dict[str, Any]:
    _require_psycopg()
    dsn = get_db_dsn()
    with psycopg.connect(dsn) as conn:
        run_id = _pick_catalog_run_id(conn, source_run_id)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_id, status, started_at, finished_at, metadata
                FROM etl_run
                WHERE run_id = %s::uuid
                LIMIT 1
                """,
                (run_id,),
            )
            run_row = cur.fetchone()
            if source_run_id:
                cur.execute(
                    """
                    SELECT
                      (SELECT count(*) FROM fact_catalog_article_store_snapshot WHERE run_id = %s::uuid AND store_code = 'XX') AS article_rows,
                      (SELECT count(*) FROM fact_catalog_article_store_snapshot WHERE run_id = %s::uuid AND store_code <> 'XX') AS store_rows,
                      (SELECT count(*) FROM fact_catalog_article_store_size_snapshot WHERE run_id = %s::uuid) AS size_rows,
                      (SELECT count(*) FROM fact_catalog_price_snapshot WHERE run_id = %s::uuid) AS price_rows
                    """,
                    (run_id, run_id, run_id, run_id),
                )
            else:
                cur.execute(
                    """
                    SELECT
                      (SELECT count(*) FROM vw_catalog_article_store_current WHERE store_code = 'XX') AS article_rows,
                      (SELECT count(*) FROM vw_catalog_article_store_current WHERE store_code <> 'XX') AS store_rows,
                      (SELECT count(*) FROM vw_catalog_article_store_size_current) AS size_rows,
                      (SELECT count(*) FROM vw_catalog_price_current) AS price_rows
                    """
                )
            counts = cur.fetchone() or [0, 0, 0, 0]

        metadata = run_row[4] if run_row and isinstance(run_row[4], dict) else {}
        return {
            "available": True,
            "run": {
                "run_id": str(run_row[0]),
                "status": str(run_row[1]),
                "started_at": run_row[2].isoformat() if run_row[2] else None,
                "finished_at": run_row[3].isoformat() if run_row[3] else None,
                "metadata": metadata,
            },
            "counts": {
                "articles": int(counts[0] or 0),
                "stores": int(counts[1] or 0),
                "sizes": int(counts[2] or 0),
                "prices": int(counts[3] or 0),
            },
            "facets": _catalog_facets(conn, source_run_id=run_id if source_run_id else None),
        }


def list_catalog_articles(
    *,
    search: str = "",
    season_code: str = "",
    reparto: str = "",
    categoria: str = "",
    limit: int = 100,
    offset: int = 0,
    source_run_id: Optional[str] = None,
) -> Dict[str, Any]:
    _require_psycopg()
    dsn = get_db_dsn()
    with psycopg.connect(dsn) as conn:
        run_id = _pick_catalog_run_id(conn, source_run_id)
        if source_run_id:
            source_name = "fact_catalog_article_store_snapshot"
            where_parts = ["s.run_id = %s::uuid", "s.store_code = 'XX'"]
            params: List[Any] = [run_id]
            price_join = """
                LEFT JOIN fact_catalog_price_snapshot p
                  ON p.run_id = s.run_id
                 AND p.season_code = s.season_code
                 AND p.article_code = s.article_code
            """
        else:
            source_name = "vw_catalog_article_store_current"
            where_parts = ["s.store_code = 'XX'"]
            params = []
            price_join = """
                LEFT JOIN vw_catalog_price_current p
                  ON p.season_code = s.season_code
                 AND p.article_code = s.article_code
            """

        if search.strip():
            where_parts.append(
                """
                (
                  s.article_code ILIKE %s OR
                  COALESCE(s.description, '') ILIKE %s OR
                  COALESCE(s.color, '') ILIKE %s OR
                  COALESCE(s.supplier, '') ILIKE %s OR
                  COALESCE(s.reparto, '') ILIKE %s OR
                  COALESCE(s.categoria, '') ILIKE %s OR
                  COALESCE(s.tipologia, '') ILIKE %s
                )
                """
            )
            needle = f"%{search.strip()}%"
            params.extend([needle] * 7)
        if season_code.strip():
            where_parts.append("s.season_code = %s")
            params.append(season_code.strip().upper())
        if reparto.strip():
            where_parts.append("COALESCE(s.reparto, '') = %s")
            params.append(reparto.strip())
        if categoria.strip():
            where_parts.append("COALESCE(s.categoria, '') = %s")
            params.append(categoria.strip())

        where_sql = " AND ".join(where_parts)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT count(*)
                FROM {source_name} s
                WHERE {where_sql}
                """,
                tuple(params),
            )
            total = int((cur.fetchone() or [0])[0] or 0)

            cur.execute(
                f"""
                SELECT
                  s.season_code,
                  s.article_code,
                  COALESCE(s.description, da.description, '') AS description,
                  COALESCE(s.color, da.colore, '') AS color,
                  COALESCE(s.supplier, '') AS supplier,
                  COALESCE(s.reparto, '') AS reparto,
                  COALESCE(s.categoria, '') AS categoria,
                  COALESCE(s.tipologia, '') AS tipologia,
                  COALESCE(s.giac, 0) AS giac,
                  COALESCE(s.con, 0) AS con,
                  COALESCE(s.ven, 0) AS ven,
                  COALESCE(s.perc_ven, 0) AS perc_ven,
                  p.price_listino AS price_listino,
                  p.price_saldo AS price_saldo
                FROM {source_name} s
                LEFT JOIN dim_article da
                  ON da.article_code = s.article_code
                {price_join}
                WHERE {where_sql}
                ORDER BY s.season_code DESC, s.article_code ASC
                LIMIT %s OFFSET %s
                """,
                tuple(params + [int(limit), int(offset)]),
            )
            rows = []
            for row in cur.fetchall():
                rows.append(
                    {
                        "season_code": str(row[0] or ""),
                        "article_code": str(row[1] or ""),
                        "description": str(row[2] or ""),
                        "color": str(row[3] or ""),
                        "supplier": str(row[4] or ""),
                        "reparto": str(row[5] or ""),
                        "categoria": str(row[6] or ""),
                        "tipologia": str(row[7] or ""),
                        "giac": float(row[8] or 0),
                        "con": float(row[9] or 0),
                        "ven": float(row[10] or 0),
                        "perc_ven": float(row[11] or 0),
                        "price_listino": None if row[12] is None else float(row[12]),
                        "price_saldo": None if row[13] is None else float(row[13]),
                    }
                )
    return {"run_id": run_id, "rows": rows, "total": total, "offset": int(offset), "limit": int(limit)}


def get_catalog_article_detail(
    *,
    article_code: str,
    season_code: str,
    source_run_id: Optional[str] = None,
) -> Dict[str, Any]:
    _require_psycopg()
    dsn = get_db_dsn()
    with psycopg.connect(dsn) as conn:
        run_id = _pick_catalog_run_id(conn, source_run_id)
        if source_run_id:
            source_name = "fact_catalog_article_store_snapshot"
            price_join = """
                LEFT JOIN fact_catalog_price_snapshot p
                  ON p.run_id = s.run_id
                 AND p.season_code = s.season_code
                 AND p.article_code = s.article_code
            """
            summary_where = """
                WHERE s.run_id = %s::uuid
                  AND s.season_code = %s
                  AND s.article_code = %s
                  AND s.store_code = 'XX'
            """
            stores_sql = """
                SELECT store_code, giac, con, ven, perc_ven
                FROM fact_catalog_article_store_snapshot
                WHERE run_id = %s::uuid
                  AND season_code = %s
                  AND article_code = %s
                  AND store_code <> 'XX'
                ORDER BY store_code
            """
            sizes_sql = """
                SELECT store_code, size, qty
                FROM fact_catalog_article_store_size_snapshot
                WHERE run_id = %s::uuid
                  AND season_code = %s
                  AND article_code = %s
                ORDER BY store_code, size
            """
            params: Tuple[Any, ...] = (run_id, season_code, article_code.upper().replace(" ", ""))
        else:
            source_name = "vw_catalog_article_store_current"
            price_join = """
                LEFT JOIN vw_catalog_price_current p
                  ON p.season_code = s.season_code
                 AND p.article_code = s.article_code
            """
            summary_where = """
                WHERE s.season_code = %s
                  AND s.article_code = %s
                  AND s.store_code = 'XX'
            """
            stores_sql = """
                SELECT store_code, giac, con, ven, perc_ven
                FROM vw_catalog_article_store_current
                WHERE season_code = %s
                  AND article_code = %s
                  AND store_code <> 'XX'
                ORDER BY store_code
            """
            sizes_sql = """
                SELECT store_code, size, qty
                FROM vw_catalog_article_store_size_current
                WHERE season_code = %s
                  AND article_code = %s
                ORDER BY store_code, size
            """
            params = (season_code, article_code.upper().replace(" ", ""))

        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  s.season_code,
                  s.article_code,
                  COALESCE(s.description, da.description, '') AS description,
                  COALESCE(s.color, da.colore, '') AS color,
                  COALESCE(s.supplier, '') AS supplier,
                  COALESCE(s.reparto, '') AS reparto,
                  COALESCE(s.categoria, '') AS categoria,
                  COALESCE(s.tipologia, '') AS tipologia,
                  COALESCE(s.giac, 0) AS giac,
                  COALESCE(s.con, 0) AS con,
                  COALESCE(s.ven, 0) AS ven,
                  COALESCE(s.perc_ven, 0) AS perc_ven,
                  p.price_listino AS price_listino,
                  p.price_saldo AS price_saldo
                FROM {source_name} s
                LEFT JOIN dim_article da
                  ON da.article_code = s.article_code
                {price_join}
                {summary_where}
                LIMIT 1
                """,
                params,
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Articolo catalogo non trovato: {season_code} / {article_code}")

            summary = {
                "season_code": str(row[0] or ""),
                "article_code": str(row[1] or ""),
                "description": str(row[2] or ""),
                "color": str(row[3] or ""),
                "supplier": str(row[4] or ""),
                "reparto": str(row[5] or ""),
                "categoria": str(row[6] or ""),
                "tipologia": str(row[7] or ""),
                "giac": float(row[8] or 0),
                "con": float(row[9] or 0),
                "ven": float(row[10] or 0),
                "perc_ven": float(row[11] or 0),
                "price_listino": None if row[12] is None else float(row[12]),
                "price_saldo": None if row[13] is None else float(row[13]),
            }

            cur.execute(
                stores_sql,
                params,
            )
            stores = [
                {
                    "store_code": str(store_row[0] or ""),
                    "giac": float(store_row[1] or 0),
                    "con": float(store_row[2] or 0),
                    "ven": float(store_row[3] or 0),
                    "perc_ven": float(store_row[4] or 0),
                    "sizes": {},
                }
                for store_row in cur.fetchall()
            ]
            store_map = {store["store_code"]: store for store in stores}

            cur.execute(
                sizes_sql,
                params,
            )
            for size_row in cur.fetchall():
                store_code = str(size_row[0] or "")
                if store_code not in store_map:
                    continue
                store_map[store_code]["sizes"][str(int(size_row[1]))] = float(size_row[2] or 0)

    return {"run_id": run_id, "summary": summary, "stores": stores}
