from __future__ import annotations

import hashlib
import json
import re
import shutil
import unicodedata
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from catalog_image_provider import fetch_image_bytes
from catalog_local_images import flatten_index, scan_local_images
from catalog_models import Article, CatalogStoreRow
from catalog_showcase import export_showcase_catalog
from db_sync import get_db_dsn

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None


_SEASON_PREFIX_RE = re.compile(r"^\s*(?:20)?(\d{2})\s*[_\-/ ]*\s*([A-Z])(?=$|[_\-/\s])", re.IGNORECASE)
_SEASON_ANY_RE = re.compile(r"(?<!\d)(?:20)?(\d{2})\s*[_\-/ ]*\s*([A-Z])(?=$|[^A-Z0-9])", re.IGNORECASE)


def _require_psycopg() -> None:
    if psycopg is None:
        raise RuntimeError("psycopg non installato. Esegui: pip install -r requirements.txt")


def _normalize_season_label(value: str) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    match = _SEASON_PREFIX_RE.search(text)
    if not match:
        match = _SEASON_ANY_RE.search(text)
    if match:
        return f"{match.group(1)}{match.group(2).upper()}"
    return text


def _season_aliases(value: str) -> List[str]:
    code = _normalize_season_label(value)
    if not code:
        return []
    if len(code) == 3 and code[2] in {"E", "G"}:
        year = code[:2]
        return [f"{year}E", f"{year}G"]
    if len(code) == 3 and code[2] in {"I", "Y"}:
        year = code[:2]
        return [f"{year}I", f"{year}Y"]
    return [code]


def _catalog_key(season_code: str, article_code: str) -> str:
    return f"{str(season_code or '').strip().upper()}||{str(article_code or '').strip().upper()}"


def _parse_manual_codes(raw_value: str) -> List[str]:
    tokens = re.findall(r"\b\d{1,3}/[A-Z0-9]{2,}\b", str(raw_value or ""), flags=re.IGNORECASE)
    out: List[str] = []
    seen = set()
    for item in tokens:
        code = str(item).strip().upper().replace(" ", "")
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _compute_source_mode(primary_source: str, allow_fallback: bool) -> str:
    source = str(primary_source or "local").strip().lower()
    if source == "web":
        return "web_then_local" if allow_fallback else "web_only"
    return "local_then_web" if allow_fallback else "local_only"


def _catalog_source_tables(source_run_id: Optional[str]) -> Dict[str, Any]:
    if source_run_id:
        return {
            "store_table": "fact_catalog_article_store_snapshot",
            "size_table": "fact_catalog_article_store_size_snapshot",
            "price_table": "fact_catalog_price_snapshot",
            "where_prefix": "run_id = %s::uuid AND ",
            "params_prefix": [source_run_id],
            "run_id": source_run_id,
        }
    return {
        "store_table": "vw_catalog_article_store_current",
        "size_table": "vw_catalog_article_store_size_current",
        "price_table": "vw_catalog_price_current",
        "where_prefix": "",
        "params_prefix": [],
        "run_id": None,
    }


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


def _load_catalog_articles_from_db(*, source_run_id: Optional[str] = None) -> Tuple[str, Dict[str, Article], Dict[str, Dict[str, Optional[float]]]]:
    _require_psycopg()
    dsn = get_db_dsn()
    with psycopg.connect(dsn) as conn:
        effective_run_id = _pick_catalog_run_id(conn, source_run_id)
        source = _catalog_source_tables(source_run_id)
        articles: Dict[str, Article] = {}

        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  season_code,
                  article_code,
                  COALESCE(description, '') AS description,
                  COALESCE(color, '') AS color,
                  COALESCE(supplier, '') AS supplier,
                  COALESCE(reparto, '') AS reparto,
                  COALESCE(categoria, '') AS categoria,
                  COALESCE(tipologia, '') AS tipologia,
                  store_code,
                  COALESCE(giac, 0) AS giac,
                  COALESCE(con, 0) AS con,
                  COALESCE(ven, 0) AS ven,
                  COALESCE(perc_ven, 0) AS perc_ven,
                  COALESCE(source_file, '') AS source_file
                FROM {source['store_table']}
                WHERE {source['where_prefix']}season_code <> ''
                ORDER BY season_code, article_code, store_code
                """,
                tuple(source["params_prefix"]),
            )
            for row in cur.fetchall():
                season_code = str(row[0] or "").strip().upper()
                article_code = str(row[1] or "").strip().upper()
                article_key = _catalog_key(season_code, article_code)
                article = articles.get(article_key)
                if article is None:
                    article = Article(
                        code=article_code,
                        season=season_code,
                        season_code=season_code,
                        season_label=season_code,
                        description=str(row[2] or ""),
                        color=str(row[3] or ""),
                        supplier=str(row[4] or ""),
                        reparto=str(row[5] or ""),
                        categoria=str(row[6] or ""),
                        tipologia=str(row[7] or ""),
                        giac=float(row[9] or 0.0),
                        con=float(row[10] or 0.0),
                        ven=float(row[11] or 0.0),
                        perc_ven=float(row[12] or 0.0),
                    )
                    articles[article_key] = article
                source_file = str(row[13] or "").strip()
                if source_file:
                    article.source_files.add(source_file)

                store_code = str(row[8] or "").strip().upper()
                if not store_code or store_code == "XX":
                    continue
                article.stores[store_code] = CatalogStoreRow(
                    store=store_code,
                    giac=float(row[9] or 0.0),
                    con=float(row[10] or 0.0),
                    ven=float(row[11] or 0.0),
                    perc_ven=float(row[12] or 0.0),
                )

            cur.execute(
                f"""
                SELECT season_code, article_code, store_code, size, qty
                FROM {source['size_table']}
                WHERE {source['where_prefix']}season_code <> ''
                ORDER BY season_code, article_code, store_code, size
                """,
                tuple(source["params_prefix"]),
            )
            for row in cur.fetchall():
                season_code = str(row[0] or "").strip().upper()
                article_code = str(row[1] or "").strip().upper()
                article_key = _catalog_key(season_code, article_code)
                article = articles.get(article_key)
                if article is None:
                    continue
                store_code = str(row[2] or "").strip().upper()
                if not store_code or store_code == "XX":
                    continue
                store_row = article.stores.get(store_code)
                if store_row is None:
                    store_row = CatalogStoreRow(store=store_code)
                    article.stores[store_code] = store_row
                try:
                    size = int(row[3])
                    qty = float(row[4] or 0.0)
                except Exception:
                    continue
                store_row.sizes[size] = qty

            price_lookup: Dict[str, Dict[str, Optional[float]]] = {}
            cur.execute(
                f"""
                SELECT season_code, article_code, price_listino, price_saldo
                FROM {source['price_table']}
                WHERE {source['where_prefix']}season_code <> ''
                """,
                tuple(source["params_prefix"]),
            )
            for row in cur.fetchall():
                season_code = str(row[0] or "").strip().upper()
                article_code = str(row[1] or "").strip().upper()
                article_key = _catalog_key(season_code, article_code)
                price_lookup[article_key] = {
                    "prezzo_listino": None if row[2] is None else float(row[2]),
                    "prezzo_saldo": None if row[3] is None else float(row[3]),
                }

    for article in articles.values():
        if article.stores:
            article.recompute_totals()

    return effective_run_id, articles, price_lookup


def _filter_article_keys(
    articles: Mapping[str, Article],
    *,
    seasons: Sequence[str],
    reparti: Sequence[str],
    suppliers: Sequence[str],
    categories: Sequence[str],
    manual_codes: Sequence[str],
) -> List[str]:
    season_filter = {str(x or "").strip().upper() for x in seasons if str(x or "").strip()}
    reparto_filter = {str(x or "").strip().lower() for x in reparti if str(x or "").strip()}
    supplier_filter = {str(x or "").strip().lower() for x in suppliers if str(x or "").strip()}
    category_filter = {str(x or "").strip().lower() for x in categories if str(x or "").strip()}
    manual_filter = {str(x or "").strip().upper().replace(" ", "") for x in manual_codes if str(x or "").strip()}

    keys: List[str] = []
    for key, article in articles.items():
        if season_filter and str(article.season or article.season_code or "").strip().upper() not in season_filter:
            continue
        if reparto_filter and str(article.reparto or "").strip().lower() not in reparto_filter:
            continue
        if supplier_filter and str(article.supplier or "").strip().lower() not in supplier_filter:
            continue
        if category_filter and str(article.categoria or "").strip().lower() not in category_filter:
            continue
        if manual_filter and str(article.code or "").strip().upper() not in manual_filter:
            continue
        keys.append(key)

    keys.sort(
        key=lambda item: (
            str(articles[item].season or articles[item].season_code or "").upper(),
            str(articles[item].reparto or "").upper(),
            str(articles[item].categoria or "").upper(),
            str(articles[item].code or "").upper(),
        )
    )
    return keys


def _compute_local_index_signature(
    *,
    root_raw: str,
    selected_seasons: Sequence[str],
    position_name: str,
    allow_position_variants: bool,
) -> str:
    root_norm = str(Path(root_raw).expanduser()).strip().lower()
    seasons_norm = "|".join(sorted([str(x).strip().upper() for x in selected_seasons if str(x).strip()]))
    pos_norm = (position_name or "xl").strip().lower() or "xl"
    var_norm = "1" if allow_position_variants else "0"
    return f"{root_norm}::{seasons_norm}::{pos_norm}::{var_norm}"


def _local_index_cache_path(cache_root: Path, signature: str) -> Path:
    key = hashlib.sha1(str(signature or "").encode("utf-8")).hexdigest()
    return cache_root / f"{key}.json"


def _save_local_index_cache(
    *,
    cache_root: Path,
    signature: str,
    index_map: Mapping[str, str],
    summary: Mapping[str, int],
) -> None:
    if not signature or not index_map:
        return
    cache_root.mkdir(parents=True, exist_ok=True)
    payload = {
        "signature": str(signature),
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "summary": {str(k): int(v) for k, v in (summary or {}).items() if str(k)},
        "index": {str(k): str(v) for k, v in (index_map or {}).items() if str(k)},
    }
    _local_index_cache_path(cache_root, signature).write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )


def _load_local_index_cache(*, cache_root: Path, signature: str) -> Optional[Tuple[Dict[str, str], Dict[str, int]]]:
    if not signature:
        return None
    cache_path = _local_index_cache_path(cache_root, signature)
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if str(payload.get("signature", "") or "") not in {"", signature}:
        return None

    raw_index = payload.get("index", {})
    if not isinstance(raw_index, dict):
        return None

    index_map: Dict[str, str] = {}
    missing_files = 0
    for key, value in raw_index.items():
        kk = str(key or "").strip()
        vv = str(value or "").strip()
        if not kk or not vv:
            continue
        p = Path(vv)
        if p.exists() and p.is_file():
            index_map[kk] = vv
        else:
            missing_files += 1
    if not index_map:
        return None

    summary_raw = payload.get("summary", {})
    summary: Dict[str, int] = {}
    if isinstance(summary_raw, dict):
        for key, value in summary_raw.items():
            sk = str(key or "").strip()
            if not sk:
                continue
            try:
                summary[sk] = int(value)
            except Exception:
                continue
    summary["cache_entries"] = len(index_map)
    if missing_files:
        summary["cache_missing_files"] = missing_files
    return index_map, summary


def _resolve_local_season_dirs(root_dir: Path, requested_seasons: Sequence[str]) -> List[str]:
    if not root_dir.exists() or not root_dir.is_dir():
        return []
    season_map: Dict[str, List[str]] = {}
    for child in root_dir.iterdir():
        if not child.is_dir():
            continue
        season_map.setdefault(_normalize_season_label(child.name), []).append(child.name)

    selected: List[str] = []
    wanted = [str(x).strip().upper() for x in requested_seasons if str(x).strip()]
    for code in wanted:
        for alias in _season_aliases(code):
            selected.extend(season_map.get(alias, []))
    if not wanted:
        for entries in season_map.values():
            selected.extend(entries)
    return sorted(list(dict.fromkeys(selected)))


def _build_local_image_index(
    *,
    root_dir: Path,
    requested_seasons: Sequence[str],
    position_name: str,
    allow_position_variants: bool,
    cache_root: Path,
) -> Tuple[Dict[str, str], Dict[str, int], str]:
    selected_dirs = _resolve_local_season_dirs(root_dir, requested_seasons)
    if not selected_dirs:
        return {}, {"seasons": 0, "codes_total_unique": 0}, ""

    signature = _compute_local_index_signature(
        root_raw=str(root_dir),
        selected_seasons=selected_dirs,
        position_name=position_name,
        allow_position_variants=allow_position_variants,
    )
    cached = _load_local_index_cache(cache_root=cache_root, signature=signature)
    if cached:
        index_map, summary = cached
        summary = dict(summary)
        summary["cache_hit"] = 1
        return index_map, summary, signature

    season_index, summary = scan_local_images(
        root_dir=root_dir,
        season_names=selected_dirs,
        position=(position_name or "xl").strip() or "xl",
        allow_position_variants=allow_position_variants,
    )
    flat_index = flatten_index(season_index, season_priority=selected_dirs)
    index_map = {key: str(value) for key, value in flat_index.items()}
    _save_local_index_cache(cache_root=cache_root, signature=signature, index_map=index_map, summary=summary)
    return index_map, summary, signature


def _zip_dir(source_dir: Path, zip_path: Path) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in source_dir.rglob("*"):
            if not path.is_file():
                continue
            zf.write(path, path.relative_to(source_dir))


def _emit_progress(
    progress_cb: Optional[Callable[[Dict[str, Any]], None]],
    *,
    stage: str,
    message: str,
    progress: float,
    **extra: Any,
) -> None:
    if progress_cb is None:
        return
    payload: Dict[str, Any] = {
        "stage": str(stage or "").strip() or "running",
        "message": str(message or "").strip(),
        "progress": max(0.0, min(100.0, float(progress or 0.0))),
    }
    payload.update(extra)
    progress_cb(payload)


def _safe_job_id(job_id: str) -> str:
    value = str(job_id or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", value):
        raise ValueError("job_id non valido.")
    return value


def _output_paths(root: Path, job_id: str) -> Dict[str, Path]:
    base_dir = root / "output" / "catalog_showcase" / job_id
    return {
        "base_dir": base_dir,
        "export_dir": base_dir / "export",
        "zip_path": base_dir / "catalog_showcase.zip",
        "summary_path": base_dir / "summary.json",
    }


def _slugify_filename_piece(value: str, *, fallback: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text[:48] if text else fallback


def _selection_filename_piece(
    values: Sequence[str],
    *,
    empty_label: str,
    plural_label: str,
) -> str:
    cleaned = [str(x).strip() for x in values if str(x).strip()]
    if not cleaned:
        return empty_label
    if len(cleaned) == 1:
        return _slugify_filename_piece(cleaned[0], fallback=plural_label)
    if len(cleaned) <= 3:
        joined = "-".join([_slugify_filename_piece(item, fallback=plural_label) for item in cleaned])
        return joined[:72] if joined else plural_label
    return f"{len(cleaned)}-{plural_label}"


def _category_code_filename_piece(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "cat"
    match = re.match(r"^\s*([A-Za-z0-9]{1,4})\b", text)
    if match:
        return _slugify_filename_piece(match.group(1), fallback="cat")
    return _slugify_filename_piece(text, fallback="cat")


def _categories_filename_piece(values: Sequence[str]) -> str:
    cleaned = [str(x).strip() for x in values if str(x).strip()]
    if not cleaned:
        return "tutte-categorie"

    codes: List[str] = []
    for item in cleaned:
        code = _category_code_filename_piece(item)
        if code and code not in codes:
            codes.append(code)

    if not codes:
        return "categorie"
    return "-".join(codes)


def _reparto_code_filename_piece(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "rep"
    normalized = re.sub(r"\s+", " ", text).strip().upper()
    reparto_map = {
        "SCARPE DONNA": "sd",
        "SCARPE UOMO": "su",
    }
    if normalized in reparto_map:
        return reparto_map[normalized]

    words = re.findall(r"[A-Z0-9]+", normalized)
    if words:
        acronym = "".join(word[:1] for word in words[:4]).lower()
        if acronym:
            return acronym
    return _slugify_filename_piece(text, fallback="rep")


def _reparti_filename_piece(values: Sequence[str]) -> str:
    cleaned = [str(x).strip() for x in values if str(x).strip()]
    if not cleaned:
        return "tutti-reparti"

    codes: List[str] = []
    for item in cleaned:
        code = _reparto_code_filename_piece(item)
        if code and code not in codes:
            codes.append(code)

    if not codes:
        return "reparti"
    return "-".join(codes)


def _suppliers_filename_piece(values: Sequence[str]) -> str:
    return _selection_filename_piece(
        values,
        empty_label="tutti-fornitori",
        plural_label="fornitori",
    )


def _build_catalog_download_filename(
    *,
    selected_seasons: Sequence[str],
    selected_reparti: Sequence[str],
    selected_suppliers: Sequence[str],
    selected_categories: Sequence[str],
    manual_codes_count: int,
    export_mode: str,
    job_id: str,
) -> str:
    season_piece = _selection_filename_piece(
        selected_seasons,
        empty_label="tutte-stagioni",
        plural_label="stagioni",
    )
    reparto_piece = _reparti_filename_piece(selected_reparti)
    supplier_piece = _suppliers_filename_piece(selected_suppliers) if any(str(x).strip() for x in selected_suppliers) else ""
    category_piece = _categories_filename_piece(selected_categories)

    parts = [
        "catalogo-barca",
        season_piece,
        reparto_piece,
        supplier_piece,
        category_piece,
    ]
    if int(manual_codes_count or 0) > 0:
        parts.append(f"{int(manual_codes_count)}-codici")

    filename = "_".join([part for part in parts if part])
    filename = re.sub(r"_+", "_", filename).strip("_")
    if len(filename) > 180:
        filename = filename[:180].rstrip("_-")
    return f"{filename}.zip"


def export_catalog_showcase(
    *,
    root: Path,
    job_id: Optional[str] = None,
    export_mode: str,
    primary_source: str,
    allow_fallback: bool,
    selected_seasons: Sequence[str],
    selected_reparti: Sequence[str],
    selected_suppliers: Sequence[str],
    selected_categories: Sequence[str],
    manual_codes_text: str = "",
    photo_root: str = "",
    photo_position: str = "xl",
    allow_position_variants: bool = True,
    source_run_id: Optional[str] = None,
    progress_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    root = root.resolve()
    _emit_progress(
        progress_cb,
        stage="loading_catalog",
        message="Lettura catalogo dal database",
        progress=2.0,
    )
    requested_run_id = str(source_run_id or "").strip() or None
    effective_run_id, articles, price_lookup = _load_catalog_articles_from_db(source_run_id=requested_run_id)
    article_source_scope = "explicit_run" if requested_run_id else "current_view"
    article_source_note = ""
    if requested_run_id and not articles:
        effective_run_id, articles, price_lookup = _load_catalog_articles_from_db(source_run_id=None)
        article_source_scope = "current_view_fallback"
        article_source_note = (
            "La run richiesta non contiene snapshot articoli/negozi; uso il catalogo corrente aggregato."
        )

    manual_codes = _parse_manual_codes(manual_codes_text)
    requested_keys = _filter_article_keys(
        articles,
        seasons=selected_seasons,
        reparti=selected_reparti,
        suppliers=selected_suppliers,
        categories=selected_categories,
        manual_codes=manual_codes,
    )
    if not requested_keys:
        raise ValueError("Nessun articolo selezionato per il catalogo vetrina.")
    _emit_progress(
        progress_cb,
        stage="filtering_articles",
        message=f"Selezionati {len(requested_keys)} articoli per il catalogo",
        progress=8.0,
        requested=len(requested_keys),
    )

    source_mode = _compute_source_mode(primary_source, allow_fallback)
    use_local = source_mode in {"local_only", "local_then_web", "web_then_local"}
    use_web = source_mode in {"web_only", "web_then_local", "local_then_web"}

    local_image_index: Dict[str, str] = {}
    local_index_summary: Dict[str, int] = {}
    local_index_signature = ""
    photo_root_path = Path(str(photo_root or "").strip()).expanduser() if str(photo_root or "").strip() else None

    if use_local:
        if photo_root_path and photo_root_path.exists() and photo_root_path.is_dir():
            _emit_progress(
                progress_cb,
                stage="indexing_local_images",
                message="Indicizzazione archivio foto locale",
                progress=14.0,
                requested=len(requested_keys),
                photo_root=str(photo_root_path),
            )
            requested_article_seasons = sorted(
                {
                    str(articles[key].season or articles[key].season_code or "").strip().upper()
                    for key in requested_keys
                    if str(articles[key].season or articles[key].season_code or "").strip()
                }
            )
            local_image_index, local_index_summary, local_index_signature = _build_local_image_index(
                root_dir=photo_root_path,
                requested_seasons=requested_article_seasons,
                position_name=photo_position,
                allow_position_variants=allow_position_variants,
                cache_root=root / "output" / "ui" / "local_index_cache",
            )
        elif source_mode == "local_only":
            raise ValueError("Sorgente immagini impostata su archivio locale, ma la cartella foto non è valida.")
    _emit_progress(
        progress_cb,
        stage="preparing_export",
        message="Preparazione output catalogo vetrina",
        progress=18.0,
        requested=len(requested_keys),
    )

    title_parts: List[str] = []
    if selected_seasons:
        title_parts.append("Stagioni: " + ", ".join(sorted({str(x).strip().upper() for x in selected_seasons if str(x).strip()})))
    if selected_reparti:
        title_parts.append("Reparti: " + ", ".join(sorted({str(x).strip().upper() for x in selected_reparti if str(x).strip()})))
    if selected_suppliers:
        title_parts.append("Fornitori: " + ", ".join(sorted({str(x).strip() for x in selected_suppliers if str(x).strip()})))
    if selected_categories:
        title_parts.append("Categorie: " + ", ".join(sorted({str(x).strip().upper() for x in selected_categories if str(x).strip()})))
    if manual_codes:
        title_parts.append(f"Codici manuali: {len(manual_codes)}")

    title = "Catalogo BARCA"
    if title_parts:
        title = f"{title} ({' | '.join(title_parts)})"

    job_id = _safe_job_id(job_id) if str(job_id or "").strip() else datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
    paths = _output_paths(root, job_id)
    if paths["base_dir"].exists():
        shutil.rmtree(paths["base_dir"], ignore_errors=True)
    paths["export_dir"].mkdir(parents=True, exist_ok=True)

    def _forward_export_progress(payload: Dict[str, Any]) -> None:
        try:
            ratio = float(payload.get("ratio") or 0.0)
        except Exception:
            ratio = 0.0
        ratio = max(0.0, min(1.0, ratio))
        payload_stage = str(payload.get("stage") or "rendering_articles").strip().lower() or "rendering_articles"
        if payload_stage == "building_html":
            stage_name = "building_html"
            overall_progress = 95.0
            message = str(payload.get("message") or "Composizione catalogo HTML")
        else:
            stage_name = "rendering_articles"
            overall_progress = 18.0 + (ratio * 76.0)
            message = str(payload.get("message") or "Generazione articoli catalogo")
        _emit_progress(
            progress_cb,
            stage=stage_name,
            message=message,
            progress=overall_progress,
            requested=len(requested_keys),
            current=payload.get("current"),
            total=payload.get("total"),
            current_article=payload.get("current_article"),
            current_season=payload.get("current_season"),
            exported_jpg=int(payload.get("exported_jpg") or 0),
            exported_html_images=int(payload.get("exported_html_images") or 0),
            used_local=int(payload.get("used_local") or 0),
            used_web=int(payload.get("used_web") or 0),
        )

    result = export_showcase_catalog(
        output_dir=paths["export_dir"],
        articles=articles,
        codes=requested_keys,
        export_mode=str(export_mode or "both").strip().lower() or "both",
        source_mode=source_mode,
        code_to_local_image=local_image_index,
        fetch_remote_bytes=fetch_image_bytes if use_web else None,
        title=title,
        price_lookup=price_lookup,
        progress_cb=_forward_export_progress,
    )

    _emit_progress(
        progress_cb,
        stage="writing_reports",
        message="Scrittura report di esportazione",
        progress=97.0,
        requested=len(requested_keys),
        exported_jpg=int(result.get("exported_jpg", 0) or 0),
        exported_html_images=int(result.get("exported_html_images", 0) or 0),
        used_local=int(result.get("used_local", 0) or 0),
        used_web=int(result.get("used_web", 0) or 0),
    )
    (paths["export_dir"] / "_missing_images.txt").write_text(
        "\n".join([str(x) for x in (result.get("missing_images", []) or [])]),
        encoding="utf-8",
    )
    (paths["export_dir"] / "_missing_articles.txt").write_text(
        "\n".join([str(x) for x in (result.get("missing_articles", []) or [])]),
        encoding="utf-8",
    )
    (paths["export_dir"] / "_errors.txt").write_text(
        "\n".join([str(x) for x in (result.get("errors", []) or [])]),
        encoding="utf-8",
    )
    (paths["export_dir"] / "_image_source_report.txt").write_text(
        "season\tcode\tsource\tdetail\n" + "\n".join([str(x) for x in (result.get("image_source_report_lines", []) or [])]),
        encoding="utf-8",
    )
    _emit_progress(
        progress_cb,
        stage="creating_zip",
        message="Creazione archivio ZIP",
        progress=99.0,
        requested=len(requested_keys),
        exported_jpg=int(result.get("exported_jpg", 0) or 0),
        exported_html_images=int(result.get("exported_html_images", 0) or 0),
        used_local=int(result.get("used_local", 0) or 0),
        used_web=int(result.get("used_web", 0) or 0),
    )
    _zip_dir(paths["export_dir"], paths["zip_path"])

    summary = {
        "job_id": job_id,
        "run_id": effective_run_id,
        "requested_run_id": requested_run_id or "",
        "article_source_scope": article_source_scope,
        "article_source_note": article_source_note,
        "export_mode": str(export_mode or "both").strip().lower() or "both",
        "source_mode": source_mode,
        "requested": int(result.get("requested", 0) or 0),
        "exported_jpg": int(result.get("exported_jpg", 0) or 0),
        "exported_html_images": int(result.get("exported_html_images", 0) or 0),
        "used_local": int(result.get("used_local", 0) or 0),
        "used_web": int(result.get("used_web", 0) or 0),
        "prices_attached": int(result.get("prices_attached", 0) or 0),
        "missing_articles": len(result.get("missing_articles", []) or []),
        "missing_images": len(result.get("missing_images", []) or []),
        "errors": len(result.get("errors", []) or []),
        "filters": {
            "selected_seasons": [str(x) for x in selected_seasons],
            "selected_reparti": [str(x) for x in selected_reparti],
            "selected_suppliers": [str(x) for x in selected_suppliers],
            "selected_categories": [str(x) for x in selected_categories],
            "manual_codes_count": len(manual_codes),
        },
        "download_filename": _build_catalog_download_filename(
            selected_seasons=selected_seasons,
            selected_reparti=selected_reparti,
            selected_suppliers=selected_suppliers,
            selected_categories=selected_categories,
            manual_codes_count=len(manual_codes),
            export_mode=str(export_mode or "both").strip().lower() or "both",
            job_id=job_id,
        ),
        "photo_root": str(photo_root_path) if photo_root_path else "",
        "photo_position": str(photo_position or "xl"),
        "allow_position_variants": bool(allow_position_variants),
        "local_index_summary": local_index_summary,
        "local_index_signature": local_index_signature,
        "zip_path": str(paths["zip_path"]),
        "html_path": str(paths["export_dir"] / "html" / "catalogo.html") if (paths["export_dir"] / "html" / "catalogo.html").exists() else "",
        "output_dir": str(paths["export_dir"]),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    paths["summary_path"].write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    _emit_progress(
        progress_cb,
        stage="completed",
        message="Catalogo vetrina completato",
        progress=100.0,
        requested=len(requested_keys),
        exported_jpg=int(summary.get("exported_jpg", 0) or 0),
        exported_html_images=int(summary.get("exported_html_images", 0) or 0),
        used_local=int(summary.get("used_local", 0) or 0),
        used_web=int(summary.get("used_web", 0) or 0),
        missing_images=int(summary.get("missing_images", 0) or 0),
    )
    return summary


def get_catalog_showcase_result(*, root: Path, job_id: str) -> Dict[str, Any]:
    job = _safe_job_id(job_id)
    paths = _output_paths(root.resolve(), job)
    if not paths["summary_path"].exists():
        raise FileNotFoundError(f"Risultato catalogo vetrina non trovato: {job}")
    return json.loads(paths["summary_path"].read_text(encoding="utf-8"))


def list_catalog_showcase_results(*, root: Path, limit: int = 20) -> List[Dict[str, Any]]:
    base = root.resolve() / "output" / "catalog_showcase"
    if not base.exists() or not base.is_dir():
        return []

    items: List[Tuple[float, Dict[str, Any]]] = []
    for child in base.iterdir():
        if not child.is_dir():
            continue
        summary_path = child / "summary.json"
        if not summary_path.exists() or not summary_path.is_file():
            continue
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        job_id = str(payload.get("job_id") or child.name).strip()
        if not job_id:
            continue
        payload["job_id"] = job_id
        items.append((float(summary_path.stat().st_mtime), payload))

    items.sort(key=lambda item: item[0], reverse=True)
    return [payload for _, payload in items[: max(int(limit or 0), 0)]]


def get_latest_catalog_showcase_result(*, root: Path) -> Dict[str, Any]:
    rows = list_catalog_showcase_results(root=root, limit=1)
    if not rows:
        raise FileNotFoundError("Nessun risultato catalogo vetrina disponibile.")
    return rows[0]


def get_catalog_showcase_zip_path(*, root: Path, job_id: str) -> Path:
    job = _safe_job_id(job_id)
    path = _output_paths(root.resolve(), job)["zip_path"]
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"ZIP catalogo vetrina non trovato: {job}")
    return path


def get_catalog_showcase_html_path(*, root: Path, job_id: str) -> Path:
    job = _safe_job_id(job_id)
    path = _output_paths(root.resolve(), job)["export_dir"] / "html" / "catalogo.html"
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"HTML catalogo vetrina non trovato: {job}")
    return path


def get_catalog_showcase_html_asset_path(*, root: Path, job_id: str, asset_path: str) -> Path:
    job = _safe_job_id(job_id)
    rel = Path(str(asset_path or "").strip())
    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError("asset_path non valido.")
    path = (_output_paths(root.resolve(), job)["export_dir"] / "html" / rel).resolve()
    html_root = (_output_paths(root.resolve(), job)["export_dir"] / "html").resolve()
    if html_root not in path.parents and path != html_root:
        raise ValueError("asset_path fuori dalla cartella HTML.")
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Asset HTML non trovato: {job} / {asset_path}")
    return path
