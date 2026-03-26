from __future__ import annotations

import io
import html
import re
from pathlib import Path
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from PIL import Image, ImageDraw, ImageFont

from catalog_local_images import lookup_local_image_path, normalize_code
from catalog_models import Article

_INVALID_FS_CHARS_RE = re.compile(r'[<>:"/\\|?*]')


def _load_font(size: int) -> ImageFont.ImageFont:
    candidates = [
        "DejaVuSans.ttf",
        "Arial.ttf",
        "arial.ttf",
        "LiberationSans-Regular.ttf",
        r"C:\Windows\Fonts\arial.ttf",
        r"C:\Windows\Fonts\calibri.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/Library/Fonts/Arial.ttf",
    ]
    for name in candidates:
        try:
            return ImageFont.truetype(name, size=size)
        except Exception:
            continue
    return ImageFont.load_default()


def _sanitize_folder_name(value: str, fallback: str) -> str:
    s = str(value or "").strip()
    if not s:
        return fallback
    s = _INVALID_FS_CHARS_RE.sub("_", s)
    s = re.sub(r"\s+", " ", s).strip().rstrip(". ")
    return s[:80] if s else fallback


def _save_bytes_as_jpeg(data: bytes, out_path: Path) -> None:
    with Image.open(io.BytesIO(data)) as im:
        rgb = im.convert("RGB")
        rgb.save(out_path, "JPEG", quality=95, subsampling=0, optimize=True)


def _as_optional_float(value: object) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _format_price(value: Optional[float]) -> str:
    return f"{value:.2f}" if value is not None else "-"


def _catalog_price_key(season_code: str, article_code: str) -> str:
    return f"{str(season_code or '').strip().upper()}||{str(article_code or '').strip().upper()}"


def _season_price_aliases(season_code: str) -> List[str]:
    code = str(season_code or "").strip().upper()
    if len(code) != 3:
        return []
    year = code[:2]
    suffix = code[2]
    if suffix == "G":
        return [f"{year}E"]
    if suffix == "E":
        return [f"{year}G"]
    if suffix == "Y":
        return [f"{year}I"]
    if suffix == "I":
        return [f"{year}Y"]
    return []


def _resolve_article_prices(
    price_lookup: Optional[Mapping[str, Mapping[str, Optional[float]]]],
    article: Article,
) -> Tuple[Optional[float], Optional[float]]:
    lookup = price_lookup or {}
    season_code = str(article.season or article.season_code or "").strip().upper()
    article_code = str(article.code or "").strip().upper()

    direct_info = lookup.get(_catalog_price_key(season_code, article_code), {})
    listino_price = _as_optional_float(direct_info.get("prezzo_listino"))
    saldo_price = _as_optional_float(direct_info.get("prezzo_saldo"))
    if listino_price is not None and saldo_price is not None:
        return listino_price, saldo_price

    for alias_code in _season_price_aliases(season_code):
        alias_info = lookup.get(_catalog_price_key(alias_code, article_code), {})
        if listino_price is None:
            listino_price = _as_optional_float(alias_info.get("prezzo_listino"))
        if saldo_price is None:
            saldo_price = _as_optional_float(alias_info.get("prezzo_saldo"))
        if listino_price is not None and saldo_price is not None:
            break

    return listino_price, saldo_price


def _format_qty(value: object, *, blank_zero: bool = False) -> str:
    try:
        qty = float(value or 0.0)
    except Exception:
        qty = 0.0
    if blank_zero and abs(qty) < 1e-9:
        return ""
    return f"{qty:.0f}"


def _format_pct(value: object) -> str:
    try:
        pct = float(value or 0.0)
    except Exception:
        pct = 0.0
    return f"{pct:.1f}%"


def _format_sizes_inline(sizes: Mapping[int, float]) -> str:
    parts: List[str] = []
    for raw_size, raw_qty in sorted((sizes or {}).items(), key=lambda x: int(x[0])):
        try:
            qty = float(raw_qty or 0.0)
        except Exception:
            continue
        if abs(qty) < 1e-9:
            continue
        parts.append(f"{int(raw_size)}:{qty:.0f}")
    return " | ".join(parts) if parts else "-"


def _build_article_detail_html(
    article: Article,
    *,
    listino_price: Optional[float],
    saldo_price: Optional[float],
) -> str:
    def _esc(value: object) -> str:
        return html.escape(str(value or ""), quote=True)

    stores = [
        sr
        for key, sr in (article.stores or {}).items()
        if str(key or "").strip() and str(key or "").strip().upper() != "XX"
    ]
    stores.sort(key=lambda sr: (str(sr.store or "").strip().upper(), str(sr.store or "").strip()))

    size_keys: set[int] = set()
    for sr in stores:
        for size, qty in (sr.sizes or {}).items():
            try:
                qty_f = float(qty or 0.0)
                size_i = int(size)
            except Exception:
                continue
            if abs(qty_f) > 1e-9:
                size_keys.add(size_i)
    if not size_keys:
        for size, qty in (article.size_totals or {}).items():
            try:
                qty_f = float(qty or 0.0)
                size_i = int(size)
            except Exception:
                continue
            if abs(qty_f) > 1e-9:
                size_keys.add(size_i)
    size_cols = sorted(size_keys)

    description = " • ".join([x for x in [article.description, article.color] if x]) or "-"
    source_files = ", ".join(sorted([str(x) for x in (article.source_files or set()) if str(x).strip()])) or "-"

    detail_rows: List[str] = []
    detail_rows.append("<section class='detail-content'>")
    detail_rows.append("<div class='detail-head'>")
    detail_rows.append("<div>")
    detail_rows.append(f"<div class='detail-code'>{_esc(article.code or '-')}</div>")
    detail_rows.append(f"<div class='detail-desc'>{_esc(description)}</div>")
    detail_rows.append("</div>")
    detail_rows.append("<div class='detail-badges'>")
    detail_rows.append(f"<span class='detail-badge'>Stagione { _esc(article.season or '-') }</span>")
    detail_rows.append(f"<span class='detail-badge'>Reparto { _esc(article.reparto or '-') }</span>")
    detail_rows.append(f"<span class='detail-badge'>Categoria { _esc(article.categoria or '-') }</span>")
    detail_rows.append("</div>")
    detail_rows.append("</div>")

    detail_rows.append("<div class='detail-kpis'>")
    for label, value in [
        ("GIAC", _format_qty(article.giac)),
        ("CON", _format_qty(article.con)),
        ("VEN", _format_qty(article.ven)),
        ("%VEN", _format_pct(article.perc_ven)),
        ("NEGOZI", str(len(stores))),
    ]:
        detail_rows.append(
            "<div class='detail-kpi'>"
            f"<span>{_esc(label)}</span>"
            f"<strong>{_esc(value)}</strong>"
            "</div>"
        )
    detail_rows.append("</div>")

    detail_rows.append("<div class='detail-section-title'>Scheda articolo</div>")
    detail_rows.append("<div class='detail-meta-grid'>")
    for label, value in [
        ("Fornitore", article.supplier or "-"),
        ("Tipologia", article.tipologia or "-"),
        ("Prezzo listino", _format_price(listino_price)),
        ("Prezzo saldo", _format_price(saldo_price)),
        ("Taglie totali", _format_sizes_inline(article.size_totals or {})),
        ("File sorgente", source_files),
    ]:
        detail_rows.append(
            "<div class='detail-meta'>"
            f"<span>{_esc(label)}</span>"
            f"<strong>{_esc(value)}</strong>"
            "</div>"
        )
    detail_rows.append("</div>")

    detail_rows.append("<div class='detail-section-title'>Situazione per negozio</div>")
    if not stores:
        detail_rows.append(
            "<div class='detail-empty'>Nessun dettaglio negozi disponibile per questo articolo.</div>"
        )
        detail_rows.append("</section>")
        return "".join(detail_rows)

    detail_rows.append("<div class='detail-table-wrap'>")
    detail_rows.append("<table class='detail-table'>")
    detail_rows.append("<thead><tr>")
    for header in ["NEG", "GIAC", "CON", "VEN", "%VEN"]:
        detail_rows.append(f"<th>{_esc(header)}</th>")
    for size in size_cols:
        detail_rows.append(f"<th>{_esc(size)}</th>")
    detail_rows.append("</tr></thead><tbody>")

    for sr in stores:
        sr_pct = float(sr.perc_ven or 0.0)
        if abs(sr_pct) < 1e-9 and float(sr.con or 0.0) > 0:
            sr_pct = (float(sr.ven or 0.0) / float(sr.con or 0.0)) * 100.0

        detail_rows.append("<tr>")
        detail_rows.append(f"<td>{_esc(sr.store)}</td>")
        detail_rows.append(f"<td>{_esc(_format_qty(sr.giac))}</td>")
        detail_rows.append(f"<td>{_esc(_format_qty(sr.con))}</td>")
        detail_rows.append(f"<td>{_esc(_format_qty(sr.ven))}</td>")
        detail_rows.append(f"<td>{_esc(_format_pct(sr_pct))}</td>")
        for size in size_cols:
            detail_rows.append(f"<td>{_esc(_format_qty((sr.sizes or {}).get(size, 0.0), blank_zero=True))}</td>")
        detail_rows.append("</tr>")

    detail_rows.append("<tr class='detail-total'>")
    detail_rows.append("<td>TOT</td>")
    detail_rows.append(f"<td>{_esc(_format_qty(article.giac))}</td>")
    detail_rows.append(f"<td>{_esc(_format_qty(article.con))}</td>")
    detail_rows.append(f"<td>{_esc(_format_qty(article.ven))}</td>")
    detail_rows.append(f"<td>{_esc(_format_pct(article.perc_ven))}</td>")
    for size in size_cols:
        detail_rows.append(
            f"<td>{_esc(_format_qty((article.size_totals or {}).get(size, 0.0), blank_zero=True))}</td>"
        )
    detail_rows.append("</tr>")

    detail_rows.append("</tbody></table></div>")
    detail_rows.append("</section>")
    return "".join(detail_rows)


def _resolve_image_bytes(
    *,
    code: str,
    source_mode: str,
    code_to_local_image: Mapping[str, str | Path],
    fetch_remote_bytes: Optional[Callable[[str], Tuple[Optional[bytes], Optional[str]]]],
) -> Tuple[Optional[bytes], Optional[str], str]:
    """
    Returns: (image_bytes, detail, source_kind)
      source_kind: "local" | "web" | ""
      detail can be populated even on success (e.g. local fallback reason).
    """
    c = normalize_code(code)

    def _try_local() -> Tuple[Optional[bytes], Optional[str], str]:
        p = lookup_local_image_path(c, code_to_local_image)
        if not p:
            return None, "local_not_found", ""
        if not p.exists() or not p.is_file():
            return None, "local_file_missing", ""
        try:
            return p.read_bytes(), None, "local"
        except Exception as e:
            return None, f"local_read_error:{type(e).__name__}", ""

    def _try_web() -> Tuple[Optional[bytes], Optional[str], str]:
        if fetch_remote_bytes is None:
            return None, "web_fetch_unavailable", ""
        try:
            b, err = fetch_remote_bytes(c)
            if b:
                return b, None, "web"
            return None, err or "web_not_found", ""
        except Exception as e:
            return None, f"web_fetch_error:{type(e).__name__}", ""

    if source_mode in ("local_only", "local_then_web"):
        b, err, kind = _try_local()
        if b or source_mode == "local_only":
            return b, err, kind
        local_detail = err or "local_not_found"
        b2, err2, kind2 = _try_web()
        if b2:
            return b2, f"{local_detail};web_ok", kind2
        return None, f"{err};{err2}" if (err and err2) else (err or err2), ""

    if source_mode in ("web_only", "web_then_local"):
        b, err, kind = _try_web()
        if b or source_mode == "web_only":
            return b, err, kind
        web_detail = err or "web_not_found"
        b2, err2, kind2 = _try_local()
        if b2:
            return b2, f"{web_detail};local_ok", kind2
        return None, f"{err};{err2}" if (err and err2) else (err or err2), ""

    return None, "not_found", ""


def render_showcase_jpg(
    article: Article,
    img_bytes: Optional[bytes],
    *,
    listino_price: Optional[float] = None,
    saldo_price: Optional[float] = None,
    canvas_w: int = 1300,
    canvas_h: int = 1500,
) -> Image.Image:
    """
    Simple showcase card:
      - big product image
      - bottom row with CODE + VEN / ORD / GIAC
    """
    img = Image.new("RGB", (canvas_w, canvas_h), (255, 255, 255))
    draw = ImageDraw.Draw(img)

    m = 36
    footer_h = 320
    photo_box = (m, m, canvas_w - m, canvas_h - footer_h - m)
    draw.rectangle((6, 6, canvas_w - 6, canvas_h - 6), outline=(0, 0, 0), width=6)
    draw.rectangle(photo_box, outline=(0, 0, 0), width=3)

    f_code = _load_font(74)
    f_val = _load_font(56)
    f_price = _load_font(38)
    f_desc = _load_font(31)

    if img_bytes:
        try:
            with Image.open(io.BytesIO(img_bytes)) as prod:
                p = prod.convert("RGB")
                bw = photo_box[2] - photo_box[0]
                bh = photo_box[3] - photo_box[1]
                scale = min(bw / p.width, bh / p.height, 1.0)
                nw = max(1, int(p.width * scale))
                nh = max(1, int(p.height * scale))
                p2 = p.resize((nw, nh), Image.Resampling.LANCZOS)
                px = photo_box[0] + (bw - nw) // 2
                py = photo_box[1] + (bh - nh) // 2
                img.paste(p2, (px, py))
        except Exception:
            draw.text((photo_box[0] + 24, photo_box[1] + 24), "IMMAGINE NON DISPONIBILE", font=f_desc, fill=(70, 70, 70))
    else:
        draw.text((photo_box[0] + 24, photo_box[1] + 24), "IMMAGINE NON DISPONIBILE", font=f_desc, fill=(70, 70, 70))

    code = article.code or ""
    ven = float(article.ven or 0.0)
    ord_v = float(article.con or 0.0)  # ORD mapped to CON from Barca DB
    giac = float(article.giac or 0.0)

    y0 = canvas_h - footer_h + 24
    code_text = code or "-"
    price_text = f"LISTINO {_format_price(listino_price)}    SALDO {_format_price(saldo_price)}"

    draw.text((m, y0), code_text, font=f_code, fill=(0, 0, 0))
    code_box = draw.textbbox((m, y0), code_text, font=f_code)
    price_box = draw.textbbox((0, 0), price_text, font=f_price)
    price_w = max(0, price_box[2] - price_box[0])
    price_right_x = canvas_w - m - price_w
    min_price_x = code_box[2] + 24

    if price_right_x >= min_price_x:
        price_x = price_right_x
        price_y = y0 + 18
        kpi_y = y0 + 110
    else:
        # Fallback per codici molto lunghi: prezzi sotto il codice.
        price_x = m
        price_y = y0 + 92
        kpi_y = y0 + 150

    draw.text((price_x, price_y), price_text, font=f_price, fill=(0, 0, 0))
    draw.text((m, kpi_y), f"VEN {ven:.0f}    ORD {ord_v:.0f}    GIAC {giac:.0f}", font=f_val, fill=(0, 0, 0))

    desc = " • ".join([x for x in [article.description, article.color] if x])
    if desc:
        draw.text((m, kpi_y + 74), desc[:80], font=f_desc, fill=(70, 70, 70))

    return img


def _build_catalog_html(
    *,
    title: str,
    items: Sequence[Dict[str, str]],
) -> str:
    rows = sorted(
        items,
        key=lambda x: (
            x.get("season_norm", ""),
            x.get("categoria", ""),
            x.get("reparto", ""),
            x.get("code", ""),
        ),
    )
    seasons = sorted({(it.get("season_norm", "") or "STAGIONE_SCONOSCIUTA") for it in rows})
    categories = sorted({(it.get("categoria", "") or "CATEGORIA_SCONOSCIUTA") for it in rows})
    reparti = sorted({(it.get("reparto", "") or "REPARTO_SCONOSCIUTO") for it in rows})
    suppliers = sorted({(it.get("supplier", "") or "FORNITORE_SCONOSCIUTO") for it in rows})
    sources = sorted({(it.get("source", "") or "none") for it in rows})

    def _esc(v: str) -> str:
        return html.escape(str(v or ""), quote=True)

    out: List[str] = []
    out.append("<!doctype html>")
    out.append("<html lang='it'>")
    out.append("<head>")
    out.append("<meta charset='utf-8'>")
    out.append("<meta name='viewport' content='width=device-width, initial-scale=1'>")
    out.append(f"<title>{_esc(title)}</title>")
    out.append(
        "<style>"
        ":root{--bg:#f3f4f6;--card:#ffffff;--line:#d9dee6;--text:#111827;--muted:#5b6573;--accent:#0f4c81;}"
        "*{box-sizing:border-box;}"
        "body{font-family:Segoe UI,Arial,sans-serif;margin:0;background:var(--bg);color:var(--text);}"
        ".wrap{max-width:1700px;margin:0 auto;padding:18px 18px 28px 18px;}"
        "h1{margin:0 0 10px 0;font-size:30px;line-height:1.2;}"
        ".toolbar{display:grid;grid-template-columns:2fr repeat(5,minmax(130px,1fr)) minmax(220px,1.3fr);gap:10px;padding:12px;border:1px solid var(--line);border-radius:12px;background:#fff;position:sticky;top:0;z-index:20;}"
        ".ctrl label{display:block;font-size:12px;font-weight:700;color:var(--muted);margin-bottom:4px;}"
        ".ctrl input,.ctrl select{width:100%;padding:8px 10px;border:1px solid #cfd6df;border-radius:8px;background:#fff;color:#0f172a;}"
        ".toolbar-bottom{display:flex;align-items:center;gap:10px;justify-content:space-between;margin-top:10px;font-size:13px;color:var(--muted);}"
        ".toolbar-bottom .left{display:flex;align-items:center;gap:10px;}"
        ".toolbar-bottom .right{display:flex;align-items:center;gap:8px;}"
        ".btn{border:1px solid #c4ccd7;background:#fff;border-radius:8px;padding:8px 10px;cursor:pointer;font-weight:600;color:#1f2937;}"
        ".btn:hover{background:#f4f7fb;}"
        ".btn[disabled]{opacity:0.5;cursor:not-allowed;}"
        ".page{display:none;}"
        ".page.active{display:block;}"
        ".page-head{display:flex;align-items:flex-start;justify-content:space-between;gap:14px;flex-wrap:wrap;padding:14px 16px;border:1px solid var(--line);border-radius:12px;background:#fff;margin-top:14px;}"
        ".page-title{font-size:22px;font-weight:800;line-height:1.15;color:#0f172a;}"
        ".page-hint{font-size:13px;color:var(--muted);margin-top:4px;}"
        ".page-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap;}"
        ".flag-grid{margin-top:14px;}"
        ".flag-empty{font-size:13px;color:var(--muted);padding:4px 2px;}"
        ".grid{margin-top:14px;display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:14px;}"
        ".card{background:var(--card);border:1px solid var(--line);border-radius:12px;overflow:hidden;display:flex;flex-direction:column;transition:border-color .16s ease,box-shadow .16s ease,background-color .16s ease;}"
        ".card.hidden{display:none;}"
        ".card.is-flagged{border-color:#6c9a7f;box-shadow:0 10px 24px rgba(63,104,78,0.16);background:linear-gradient(180deg,#f7fcf8 0%,#eef7f1 100%);}"
        ".ph{height:250px;background:#fff;border-bottom:1px solid #eef2f6;display:flex;align-items:center;justify-content:center;padding:8px;position:relative;overflow:hidden;isolation:isolate;}"
        ".ph img{max-width:100%;max-height:100%;object-fit:contain;display:block;border-radius:6px;cursor:zoom-in;position:relative;z-index:1;}"
        ".meta{padding:10px 12px 14px 12px;display:flex;flex-direction:column;gap:7px;position:relative;z-index:2;background:#fff;border-top:1px solid #eef2f6;}"
        ".meta-head{display:flex;align-items:flex-start;justify-content:space-between;gap:8px;}"
        ".code{font-size:18px;font-weight:800;letter-spacing:0.2px;}"
        ".flag-check{display:inline-flex;align-items:center;gap:8px;border:1px solid #c4ccd7;background:#fff;border-radius:999px;padding:6px 10px;font-size:11px;font-weight:700;color:#334155;cursor:pointer;white-space:nowrap;user-select:none;}"
        ".flag-check input{margin:0;width:16px;height:16px;accent-color:var(--accent);cursor:pointer;}"
        ".flag-check.active{background:#e2f1e7;border-color:#6c9a7f;color:#163323;}"
        ".flag-check.active input{accent-color:#1f6a43;}"
        ".flag-check-text{line-height:1;}"
        ".meta-actions{display:flex;align-items:center;justify-content:flex-end;gap:8px;}"
        ".detail-btn{border:1px solid #c4ccd7;background:#f8fafc;border-radius:999px;padding:6px 10px;font-size:11px;font-weight:800;color:#0f4c81;cursor:pointer;white-space:nowrap;}"
        ".detail-btn:hover{background:#eef4fb;}"
        ".kpi{font-size:15px;font-weight:700;}"
        ".price{font-size:14px;font-weight:700;color:#1f2937;}"
        ".tags{display:flex;flex-wrap:wrap;gap:6px;}"
        ".tag{font-size:11px;color:#123; background:#eef4fb;border:1px solid #d6e5f7;padding:3px 7px;border-radius:999px;}"
        ".desc{font-size:13px;color:var(--muted);min-height:34px;line-height:1.25;}"
        ".srcd{font-size:12px;color:#4b5563;background:#f6f8fb;border:1px solid #e3e8f0;border-radius:8px;padding:6px 8px;}"
        ".miss{font-size:12px;color:#8b1f1f;background:#fff4f4;border:1px solid #ffd5d5;border-radius:8px;padding:6px 8px;}"
        ".card.is-flagged .ph{background:linear-gradient(180deg,#ffffff 0%,#f2faf5 100%);border-bottom-color:#dce9e0;}"
        ".card.is-flagged .meta{background:transparent;border-top-color:#dce9e0;}"
        ".card.is-flagged .code,.card.is-flagged .kpi,.card.is-flagged .price{color:#0f172a;}"
        ".card.is-flagged .desc{color:#4b5563;}"
        ".card.is-flagged .tag{background:#fff;border-color:#cfe1d5;color:#234131;}"
        ".detail-template{display:none;}"
        ".modal{position:fixed;inset:0;background:rgba(0,0,0,0.72);display:none;align-items:center;justify-content:center;padding:24px;z-index:50;}"
        ".modal.show{display:flex;}"
        ".modal img{max-width:min(92vw,1500px);max-height:88vh;object-fit:contain;background:#fff;border-radius:10px;padding:10px;}"
        ".modal-close{position:absolute;top:16px;right:18px;border:0;background:#fff;color:#111;padding:8px 10px;border-radius:8px;font-weight:700;cursor:pointer;}"
        ".modal-detail{align-items:flex-start;overflow:auto;padding:28px;}"
        ".detail-shell{position:relative;width:min(1180px,96vw);margin:auto;background:#fff;border-radius:14px;padding:22px;border:1px solid #dbe2ea;box-shadow:0 18px 48px rgba(15,23,42,0.18);}"
        ".detail-body{display:flex;flex-direction:column;gap:16px;color:var(--text);}"
        ".detail-head{display:flex;align-items:flex-start;justify-content:space-between;gap:14px;flex-wrap:wrap;padding-right:84px;}"
        ".detail-code{font-size:30px;font-weight:800;line-height:1.05;}"
        ".detail-desc{font-size:15px;color:var(--muted);margin-top:6px;max-width:780px;line-height:1.4;}"
        ".detail-badges{display:flex;flex-wrap:wrap;gap:8px;}"
        ".detail-badge{font-size:12px;font-weight:700;padding:6px 10px;border-radius:999px;background:#eef4fb;border:1px solid #d6e5f7;color:#123;}"
        ".detail-kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;}"
        ".detail-kpi{border:1px solid var(--line);border-radius:12px;background:#f8fafc;padding:12px 14px;}"
        ".detail-kpi span{display:block;font-size:12px;color:var(--muted);font-weight:700;}"
        ".detail-kpi strong{display:block;margin-top:6px;font-size:24px;line-height:1.1;}"
        ".detail-section-title{font-size:15px;font-weight:800;margin:2px 0 0 0;}"
        ".detail-meta-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:10px;}"
        ".detail-meta{border:1px solid var(--line);border-radius:12px;background:#fff;padding:10px 12px;min-height:70px;}"
        ".detail-meta span{display:block;font-size:12px;color:var(--muted);font-weight:700;}"
        ".detail-meta strong{display:block;margin-top:6px;font-size:14px;line-height:1.35;word-break:break-word;}"
        ".detail-table-wrap{overflow:auto;border:1px solid var(--line);border-radius:12px;background:#fff;}"
        ".detail-table{width:100%;min-width:760px;border-collapse:collapse;font-size:13px;}"
        ".detail-table thead th{position:sticky;top:0;background:#eef4fb;color:#0f172a;font-size:12px;text-transform:uppercase;letter-spacing:0.03em;}"
        ".detail-table th,.detail-table td{padding:9px 10px;border-bottom:1px solid #e6ebf2;text-align:left;white-space:nowrap;}"
        ".detail-table tbody tr:nth-child(even){background:#fbfcfe;}"
        ".detail-table tbody tr.detail-total{background:#eef4fb;font-weight:800;}"
        ".detail-empty{padding:12px;border:1px dashed #c9d3df;border-radius:12px;background:#fbfcfe;color:var(--muted);}"
        "@media (max-width:1180px){.toolbar{grid-template-columns:repeat(2,minmax(180px,1fr));}}"
        "@media (max-width:700px){.toolbar{grid-template-columns:1fr;position:static;}.toolbar-bottom{flex-direction:column;align-items:flex-start;}.toolbar-bottom .right{width:100%;display:grid;grid-template-columns:1fr;}.page-actions{width:100%;display:grid;grid-template-columns:1fr;}.ph{height:220px;}.detail-shell{padding:16px;}.detail-head{padding-right:0;}.detail-code{font-size:24px;}.detail-table{font-size:12px;}}"
        "</style>"
    )
    out.append("</head><body><div class='wrap'>")
    out.append(f"<h1>{_esc(title)}</h1>")
    out.append("<section id='catalog-page' class='page active'>")
    out.append("<section class='toolbar'>")
    out.append(
        "<div class='ctrl'><label for='f-search'>Ricerca</label>"
        "<input id='f-search' type='text' placeholder='Codice / descrizione / colore / fornitore / prezzo'></div>"
    )
    out.append("<div class='ctrl'><label for='f-season'>Stagione</label><select id='f-season'><option value=''>Tutte</option>")
    out.extend([f"<option value='{_esc(x)}'>{_esc(x)}</option>" for x in seasons])
    out.append("</select></div>")
    out.append("<div class='ctrl'><label for='f-cat'>Categoria</label><select id='f-cat'><option value=''>Tutte</option>")
    out.extend([f"<option value='{_esc(x)}'>{_esc(x)}</option>" for x in categories])
    out.append("</select></div>")
    out.append("<div class='ctrl'><label for='f-rep'>Reparto</label><select id='f-rep'><option value=''>Tutti</option>")
    out.extend([f"<option value='{_esc(x)}'>{_esc(x)}</option>" for x in reparti])
    out.append("</select></div>")
    out.append("<div class='ctrl'><label for='f-supplier'>Fornitore</label><select id='f-supplier'><option value=''>Tutti</option>")
    out.extend([f"<option value='{_esc(x)}'>{_esc(x)}</option>" for x in suppliers])
    out.append("</select></div>")
    out.append("<div class='ctrl'><label for='f-src'>Fonte immagine</label><select id='f-src'><option value=''>Tutte</option>")
    out.extend([f"<option value='{_esc(x)}'>{_esc(x)}</option>" for x in sources])
    out.append("</select></div>")
    out.append(
        "<div class='ctrl'><label for='f-sort'>Ordina per</label>"
        "<select id='f-sort'>"
        "<option value='season_cat_code'>Stagione / categoria / codice</option>"
        "<option value='code_asc'>Codice A→Z</option>"
        "<option value='code_desc'>Codice Z→A</option>"
        "<option value='ven_desc'>VEN alto→basso</option>"
        "<option value='ven_asc'>VEN basso→alto</option>"
        "<option value='ord_desc'>ORD alto→basso</option>"
        "<option value='ord_asc'>ORD basso→alto</option>"
        "<option value='giac_desc'>GIAC alto→basso</option>"
        "<option value='giac_asc'>GIAC basso→alto</option>"
        "</select></div>"
    )
    out.append("</section>")
    out.append(
        "<div class='toolbar-bottom'>"
        "<div class='left'>"
        "<label><input type='checkbox' id='f-with-img' checked> Solo articoli con immagine</label>"
        "<button id='f-reset' class='btn' type='button'>Reset filtri</button>"
        "</div>"
        "<div class='right'>"
        "<button id='f-select-visible' class='btn' type='button' disabled>Seleziona filtrati</button>"
        "<button id='f-clear-visible' class='btn' type='button' disabled>Deseleziona filtrati</button>"
        "<button id='f-open-flag' class='btn' type='button' disabled>Vai ai selezionati</button>"
        "<div id='flag-count'>0 selezionati</div>"
        "</div>"
        "<div id='result-count'>0 risultati</div>"
        "</div>"
    )
    out.append("<section id='grid' class='grid'>")
    for it in rows:
        code = it.get("code", "")
        ven = it.get("ven", "0")
        ord_v = it.get("ord", "0")
        giac = it.get("giac", "0")
        prezzo_listino = it.get("prezzo_listino", "")
        prezzo_saldo = it.get("prezzo_saldo", "")
        desc = it.get("description", "")
        miss = it.get("missing_reason", "")
        season = it.get("season_norm", "") or "STAGIONE_SCONOSCIUTA"
        categoria = it.get("categoria", "") or "CATEGORIA_SCONOSCIUTA"
        reparto = it.get("reparto", "") or "REPARTO_SCONOSCIUTO"
        supplier = it.get("supplier", "") or "FORNITORE_SCONOSCIUTO"
        source = it.get("source", "") or "none"
        source_detail = it.get("source_detail", "")
        img_rel = it.get("img_rel", "")
        detail_html = str(it.get("detail_html", "") or "")

        ven_n = float(it.get("ven", "0") or 0.0)
        ord_n = float(it.get("ord", "0") or 0.0)
        giac_n = float(it.get("giac", "0") or 0.0)
        searchable = " ".join(
            [str(code), str(desc), str(season), str(categoria), str(reparto), str(supplier), str(source), str(prezzo_listino), str(prezzo_saldo)]
        ).lower()

        if img_rel:
            img_block = f"<img src='{_esc(img_rel)}' alt='{_esc(code)}' class='zoomable' data-full='{_esc(img_rel)}' loading='lazy' decoding='async'>"
            has_img = "1"
        else:
            img_block = "<div class='miss'>Immagine non disponibile</div>"
            has_img = "0"

        out.append(
            "<article class='card' "
            f"data-code='{_esc(code)}' "
            f"data-season='{_esc(season)}' "
            f"data-category='{_esc(categoria)}' "
            f"data-reparto='{_esc(reparto)}' "
            f"data-supplier='{_esc(supplier)}' "
            f"data-source='{_esc(source)}' "
            f"data-source-detail='{_esc(source_detail)}' "
            f"data-ven='{ven_n:.6f}' "
            f"data-ord='{ord_n:.6f}' "
            f"data-giac='{giac_n:.6f}' "
            f"data-ven-label='{_esc(ven)}' "
            f"data-ord-label='{_esc(ord_v)}' "
            f"data-giac-label='{_esc(giac)}' "
            f"data-listino='{_esc(prezzo_listino)}' "
            f"data-saldo='{_esc(prezzo_saldo)}' "
            f"data-description='{_esc(desc)}' "
            f"data-missing-reason='{_esc(miss)}' "
            f"data-img-rel='{_esc(img_rel)}' "
            f"data-has-image='{has_img}' "
            f"data-search='{_esc(searchable)}'>"
        )
        out.append(f"<div class='ph'>{img_block}</div>")
        out.append("<div class='meta'>")
        out.append("<div class='meta-head'>")
        out.append(f"<div class='code'>{_esc(code)}</div>")
        out.append(
            "<label class='flag-check' data-role='flag-toggle'>"
            "<input class='flag-checkbox' type='checkbox' aria-label='Seleziona articolo'>"
            "<span class='flag-check-text'>Seleziona</span>"
            "</label>"
        )
        out.append("</div>")
        out.append(f"<div class='kpi'>VEN {_esc(ven)} | ORD {_esc(ord_v)} | GIAC {_esc(giac)}</div>")
        out.append(
            f"<div class='price'>LISTINO {_esc(prezzo_listino or '-')} | SALDO {_esc(prezzo_saldo or '-')}</div>"
        )
        out.append(
            "<div class='tags'>"
            f"<span class='tag'>{_esc(season)}</span>"
            f"<span class='tag'>{_esc(reparto)}</span>"
            f"<span class='tag'>{_esc(categoria)}</span>"
            f"<span class='tag'>SRC: {_esc(source)}</span>"
            "</div>"
        )
        out.append(f"<div class='desc'>Fornitore: {_esc(supplier)}</div>")
        out.append(f"<div class='desc'>{_esc(desc)}</div>")
        if detail_html:
            out.append("<div class='meta-actions'><button class='detail-btn' type='button'>PIU INFO</button></div>")
        if source_detail:
            out.append(f"<div class='srcd'>Dettaglio sorgente: {_esc(source_detail)}</div>")
        if miss:
            out.append(f"<div class='miss'>{_esc(miss)}</div>")
        out.append("</div>")
        if detail_html:
            out.append("<template class='detail-template'>")
            out.append(detail_html)
            out.append("</template>")
        out.append("</article>")
    out.append("</section>")
    out.append("</section>")
    out.append(
        "<section id='flag-page' class='page'>"
        "<div class='page-head'>"
        "<div>"
        "<div class='page-title'>Articoli selezionati</div>"
        "<div class='page-hint'>Vista dedicata con solo i prodotti che hai flaggato.</div>"
        "</div>"
        "<div class='page-actions'>"
        "<button id='flag-back' class='btn' type='button'>Torna al catalogo</button>"
        "<button id='f-export-flag' class='btn' type='button' disabled>Esporta selezionati (Excel)</button>"
        "<button id='f-clear-flag' class='btn' type='button' disabled>Svuota selezione</button>"
        "</div>"
        "</div>"
        "<div id='flag-grid' class='grid flag-grid'><div class='flag-empty'>Nessun articolo selezionato.</div></div>"
        "</section>"
    )

    out.append(
        "<div id='img-modal' class='modal' aria-hidden='true'>"
        "<button class='modal-close' id='img-modal-close' type='button'>Chiudi</button>"
        "<img id='img-modal-view' src='' alt='Preview'>"
        "</div>"
    )
    out.append(
        "<div id='detail-modal' class='modal modal-detail' aria-hidden='true'>"
        "<div class='detail-shell'>"
        "<button class='modal-close' id='detail-modal-close' type='button'>Chiudi</button>"
        "<div id='detail-modal-body' class='detail-body'></div>"
        "</div>"
        "</div>"
    )

    out.append(
        """
<script>
(function(){
  function q(id){return document.getElementById(id);}
  function norm(v){return String(v||"").toLowerCase().replace(/^\\s+|\\s+$/g,"");}
  function cmpText(a,b){
    a=String(a||""); b=String(b||"");
    if(a===b){return 0;}
    if(a.localeCompare){return a.localeCompare(b,"it",{sensitivity:"base",numeric:true});}
    return a>b?1:-1;
  }
  function num(v){var n=parseFloat(v);return isNaN(n)?0:n;}
  function hasClass(el,name){
    var cls=el&&el.className?(" "+el.className+" "):" ";
    return cls.indexOf(" "+name+" ")!==-1;
  }
  function closestCard(el){
    while(el&&el!==document){
      if(hasClass(el,"card")){return el;}
      el=el.parentNode;
    }
    return null;
  }
  function escapeXml(v){
    return String(v||"")
      .replace(/&/g,"&amp;")
      .replace(/</g,"&lt;")
      .replace(/>/g,"&gt;")
      .replace(/"/g,"&quot;")
      .replace(/'/g,"&apos;");
  }
  function dateStamp(){
    var d=new Date();
    function pad(n){n=parseInt(n,10)||0;return n<10?("0"+n):String(n);}
    return String(d.getFullYear())+pad(d.getMonth()+1)+pad(d.getDate())+"_"+pad(d.getHours())+pad(d.getMinutes())+pad(d.getSeconds());
  }
  function slugPart(v,maxLen){
    var s=String(v||"").toLowerCase().replace(/^\\s+|\\s+$/g,"");
    if(!s){return "";}
    if(s.normalize){s=s.normalize("NFD").replace(/[\\u0300-\\u036f]/g,"");}
    s=s.replace(/[^a-z0-9]+/g,"-").replace(/^-+|-+$/g,"").replace(/-+/g,"-");
    if(!s){return "";}
    maxLen=parseInt(maxLen,10)||28;
    if(s.length>maxLen){s=s.slice(0,maxLen).replace(/-+$/g,"");}
    return s;
  }
  function downloadBlob(blob,filename){
    var url=(window.URL||window.webkitURL).createObjectURL(blob);
    var a=document.createElement("a");
    a.href=url; a.download=filename;
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    setTimeout(function(){(window.URL||window.webkitURL).revokeObjectURL(url);},1000);
  }

  var grid=q("grid");
  if(!grid){return;}
  var cards=Array.prototype.slice.call(grid.querySelectorAll(".card"));
  var cardsByCode={};
  for(var cidx=0;cidx<cards.length;cidx++){
    var ccode=cards[cidx].getAttribute("data-code")||"";
    if(ccode&&!cardsByCode[ccode]){cardsByCode[ccode]=cards[cidx];}
  }

  var controls={
    search:q("f-search"),
    season:q("f-season"),
    cat:q("f-cat"),
    rep:q("f-rep"),
    supplier:q("f-supplier"),
    src:q("f-src"),
    sort:q("f-sort"),
    withImg:q("f-with-img"),
    reset:q("f-reset"),
    count:q("result-count"),
    flagCount:q("flag-count"),
    selectVisible:q("f-select-visible"),
    clearVisible:q("f-clear-visible"),
    openFlag:q("f-open-flag"),
    flagGrid:q("flag-grid"),
    exportFlag:q("f-export-flag"),
    clearFlag:q("f-clear-flag"),
    flagBack:q("flag-back"),
    mainPage:q("catalog-page"),
    flagPage:q("flag-page")
  };
  if(!controls.search||!controls.season||!controls.cat||!controls.rep||!controls.supplier||!controls.src||!controls.sort||!controls.withImg||!controls.reset||!controls.count){
    return;
  }

  var flagged={};

  function setPage(page){
    var showFlag=(page==="flag");
    if(controls.mainPage){controls.mainPage.className=showFlag?"page":"page active";}
    if(controls.flagPage){controls.flagPage.className=showFlag?"page active":"page";}
  }

  function getFlaggedCodes(){
    var out=[];
    for(var code in flagged){
      if(Object.prototype.hasOwnProperty.call(flagged,code)&&flagged[code]){out.push(code);}
    }
    out.sort(cmpText);
    return out;
  }

  function setCardFlagVisual(card,on){
    if(!card){return;}
    if(on){card.classList.add("is-flagged");}
    else{card.classList.remove("is-flagged");}
    var checks=card.querySelectorAll(".flag-check");
    for(var i=0;i<checks.length;i++){
      var wrap=checks[i];
      wrap.className=on?"flag-check active":"flag-check";
      wrap.setAttribute("aria-checked",on?"true":"false");
    }
    var inputs=card.querySelectorAll(".flag-checkbox");
    for(var j=0;j<inputs.length;j++){
      inputs[j].checked=!!on;
    }
    var labels=card.querySelectorAll(".flag-check-text");
    for(var k=0;k<labels.length;k++){
      labels[k].textContent=on?"Selezionato":"Seleziona";
    }
  }

  function syncFlagVisualByCode(code,on){
    for(var i=0;i<cards.length;i++){
      if((cards[i].getAttribute("data-code")||"")===code){setCardFlagVisual(cards[i],on);}
    }
    if(!controls.flagGrid){return;}
    var clones=controls.flagGrid.querySelectorAll(".card");
    for(var j=0;j<clones.length;j++){
      if((clones[j].getAttribute("data-code")||"")===code){setCardFlagVisual(clones[j],on);}
    }
  }

  function getVisibleCards(){
    var out=[];
    for(var i=0;i<cards.length;i++){
      if(cards[i].style.display!=="none"){out.push(cards[i]);}
    }
    return out;
  }

  function syncVisibleActionButtons(){
    if(!controls.selectVisible&&!controls.clearVisible){return;}
    var visible=getVisibleCards();
    var total=visible.length;
    var selected=0;
    for(var i=0;i<visible.length;i++){
      var code=visible[i].getAttribute("data-code")||"";
      if(code&&flagged[code]){selected+=1;}
    }
    if(controls.selectVisible){controls.selectVisible.disabled=(total===0||selected===total);}
    if(controls.clearVisible){controls.clearVisible.disabled=(selected===0);}
  }

  function setVisibleFlags(shouldFlag){
    var visible=getVisibleCards();
    for(var i=0;i<visible.length;i++){
      var code=visible[i].getAttribute("data-code")||"";
      if(!code){continue;}
      if(shouldFlag){flagged[code]=true;}
      else{delete flagged[code];}
      syncFlagVisualByCode(code,shouldFlag);
    }
    renderFlaggedGrid();
    syncVisibleActionButtons();
  }

  function renderFlaggedGrid(){
    var codes=getFlaggedCodes();
    if(controls.flagCount){controls.flagCount.textContent=String(codes.length)+" selezionati";}
    if(controls.openFlag){controls.openFlag.disabled=(codes.length===0);}
    if(controls.exportFlag){controls.exportFlag.disabled=(codes.length===0);}
    if(controls.clearFlag){controls.clearFlag.disabled=(codes.length===0);}
    if(!controls.flagGrid){return;}

    while(controls.flagGrid.firstChild){controls.flagGrid.removeChild(controls.flagGrid.firstChild);}
    if(!codes.length){
      var empty=document.createElement("div");
      empty.className="flag-empty";
      empty.textContent="Nessun articolo selezionato.";
      controls.flagGrid.appendChild(empty);
      return;
    }

    for(var i=0;i<codes.length;i++){
      var code=codes[i];
      var original=cardsByCode[code];
      if(!original){continue;}
      var clone=original.cloneNode(true);
      clone.style.display="";
      clone.classList.add("flag-copy");
      setCardFlagVisual(clone,true);
      controls.flagGrid.appendChild(clone);
    }
  }

  function collectFlaggedRows(){
    var codes=getFlaggedCodes();
    var rows=[];
    for(var i=0;i<codes.length;i++){
      var code=codes[i];
      var card=cardsByCode[code];
      if(!card){continue;}
      rows.push({
        code:card.getAttribute("data-code")||"",
        ven:card.getAttribute("data-ven-label")||"",
        ord:card.getAttribute("data-ord-label")||"",
        giac:card.getAttribute("data-giac-label")||"",
        listino:card.getAttribute("data-listino")||"",
        saldo:card.getAttribute("data-saldo")||"",
        season:card.getAttribute("data-season")||"",
        reparto:card.getAttribute("data-reparto")||"",
        supplier:card.getAttribute("data-supplier")||"",
        categoria:card.getAttribute("data-category")||"",
        source:card.getAttribute("data-source")||"",
        description:card.getAttribute("data-description")||"",
        sourceDetail:card.getAttribute("data-source-detail")||"",
        missingReason:card.getAttribute("data-missing-reason")||"",
        imagePath:card.getAttribute("data-img-rel")||""
      });
    }
    return rows;
  }

  function buildFlagExportFileName(rowCount){
    var parts=["barca","catalogo","vetrina","selezionati"];
    var filters=[];
    var season=slugPart(controls.season&&controls.season.value,24);
    var reparto=slugPart(controls.rep&&controls.rep.value,24);
    var categoria=slugPart(controls.cat&&controls.cat.value,24);
    var supplier=slugPart(controls.supplier&&controls.supplier.value,24);
    var source=slugPart(controls.src&&controls.src.value,20);
    var search=slugPart(controls.search&&controls.search.value,20);
    if(season){filters.push(season);}
    if(reparto){filters.push(reparto);}
    if(categoria){filters.push(categoria);}
    if(supplier){filters.push(supplier);}
    if(source){filters.push("src-"+source);}
    if(search){filters.push("ricerca-"+search);}
    if(controls.withImg&&controls.withImg.checked){filters.push("con-immagine");}
    if(!filters.length){filters.push("filtri-liberi");}
    parts=parts.concat(filters.slice(0,4));
    parts.push(String(parseInt(rowCount,10)||0)+"-articoli");
    parts.push(dateStamp());
    return parts.join("_")+".xls";
  }

  function exportFlaggedExcel(){
    var rows=collectFlaggedRows();
    if(!rows.length){return;}

    var headers=[
      "Codice","VEN","ORD","GIAC","Listino","Saldo","Stagione","Reparto","Fornitore","Categoria",
      "Fonte","Descrizione","Dettaglio sorgente","Motivo immagine mancante","Percorso immagine"
    ];
    var keys=[
      "code","ven","ord","giac","listino","saldo","season","reparto","supplier","categoria",
      "source","description","sourceDetail","missingReason","imagePath"
    ];

    var xml=[];
    xml.push('<?xml version="1.0" encoding="utf-8"?>');
    xml.push('<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"');
    xml.push(' xmlns:o="urn:schemas-microsoft-com:office:office"');
    xml.push(' xmlns:x="urn:schemas-microsoft-com:office:excel"');
    xml.push(' xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">');
    xml.push('<Worksheet ss:Name="ArticoliFlaggati"><Table>');
    xml.push('<Row>');
    for(var h=0;h<headers.length;h++){
      xml.push('<Cell><Data ss:Type="String">'+escapeXml(headers[h])+'</Data></Cell>');
    }
    xml.push('</Row>');
    for(var r=0;r<rows.length;r++){
      var row=rows[r];
      xml.push('<Row>');
      for(var k=0;k<keys.length;k++){
        var value=row[keys[k]]||"";
        xml.push('<Cell><Data ss:Type="String">'+escapeXml(value)+'</Data></Cell>');
      }
      xml.push('</Row>');
    }
    xml.push('</Table></Worksheet></Workbook>');

    var blob=new Blob([xml.join("")],{type:"application/vnd.ms-excel;charset=utf-8;"});
    downloadBlob(blob,buildFlagExportFileName(rows.length));
  }

  function toggleFlagFromCheckbox(target){
    if(!target||!hasClass(target,"flag-checkbox")){return false;}
    var card=closestCard(target);
    if(!card){return true;}
    var code=card.getAttribute("data-code")||"";
    if(!code){return true;}
    var on=!!target.checked;
    if(on){flagged[code]=true;}
    else{delete flagged[code];}
    syncFlagVisualByCode(code,on);
    renderFlaggedGrid();
    syncVisibleActionButtons();
    return true;
  }

  function sortCards(arr,mode){
    if(mode==="code_asc"){arr.sort(function(a,b){return cmpText(a.getAttribute("data-code"),b.getAttribute("data-code"));});return;}
    if(mode==="code_desc"){arr.sort(function(a,b){return cmpText(b.getAttribute("data-code"),a.getAttribute("data-code"));});return;}
    if(mode==="ven_desc"){arr.sort(function(a,b){return (num(b.getAttribute("data-ven"))-num(a.getAttribute("data-ven")))||cmpText(a.getAttribute("data-code"),b.getAttribute("data-code"));});return;}
    if(mode==="ven_asc"){arr.sort(function(a,b){return (num(a.getAttribute("data-ven"))-num(b.getAttribute("data-ven")))||cmpText(a.getAttribute("data-code"),b.getAttribute("data-code"));});return;}
    if(mode==="ord_desc"){arr.sort(function(a,b){return (num(b.getAttribute("data-ord"))-num(a.getAttribute("data-ord")))||cmpText(a.getAttribute("data-code"),b.getAttribute("data-code"));});return;}
    if(mode==="ord_asc"){arr.sort(function(a,b){return (num(a.getAttribute("data-ord"))-num(b.getAttribute("data-ord")))||cmpText(a.getAttribute("data-code"),b.getAttribute("data-code"));});return;}
    if(mode==="giac_desc"){arr.sort(function(a,b){return (num(b.getAttribute("data-giac"))-num(a.getAttribute("data-giac")))||cmpText(a.getAttribute("data-code"),b.getAttribute("data-code"));});return;}
    if(mode==="giac_asc"){arr.sort(function(a,b){return (num(a.getAttribute("data-giac"))-num(b.getAttribute("data-giac")))||cmpText(a.getAttribute("data-code"),b.getAttribute("data-code"));});return;}
    arr.sort(function(a,b){
      return cmpText(a.getAttribute("data-season"),b.getAttribute("data-season"))||
             cmpText(a.getAttribute("data-category"),b.getAttribute("data-category"))||
             cmpText(a.getAttribute("data-reparto"),b.getAttribute("data-reparto"))||
             cmpText(a.getAttribute("data-code"),b.getAttribute("data-code"));
    });
  }

  function applyAll(){
    var fSearch=norm(controls.search.value);
    var fSeason=norm(controls.season.value);
    var fCat=norm(controls.cat.value);
    var fRep=norm(controls.rep.value);
    var fSupplier=norm(controls.supplier.value);
    var fSrc=norm(controls.src.value);
    var onlyImg=!!controls.withImg.checked;
    var visible=[];

    for(var i=0;i<cards.length;i++){
      var c=cards[i];
      var okSearch=!fSearch||norm(c.getAttribute("data-search")).indexOf(fSearch)!==-1;
      var okSeason=!fSeason||norm(c.getAttribute("data-season"))===fSeason;
      var okCat=!fCat||norm(c.getAttribute("data-category"))===fCat;
      var okRep=!fRep||norm(c.getAttribute("data-reparto"))===fRep;
      var okSupplier=!fSupplier||norm(c.getAttribute("data-supplier"))===fSupplier;
      var okSrc=!fSrc||norm(c.getAttribute("data-source"))===fSrc;
      var okImg=!onlyImg||c.getAttribute("data-has-image")==="1";
      var show=okSearch&&okSeason&&okCat&&okRep&&okSupplier&&okSrc&&okImg;
      c.style.display=show?"":"none";
      if(show){visible.push(c);}
    }
    sortCards(visible,controls.sort.value);
    for(var j=0;j<visible.length;j++){grid.appendChild(visible[j]);}
    controls.count.textContent=String(visible.length)+" risultati su "+String(cards.length);
    syncVisibleActionButtons();
  }

  function bindInput(el){
    if(!el){return;}
    el.addEventListener("input",applyAll);
    el.addEventListener("change",applyAll);
  }
  bindInput(controls.search);
  bindInput(controls.season);
  bindInput(controls.cat);
  bindInput(controls.rep);
  bindInput(controls.supplier);
  bindInput(controls.src);
  bindInput(controls.sort);
  bindInput(controls.withImg);

  controls.reset.addEventListener("click",function(){
    controls.search.value="";
    controls.season.value="";
    controls.cat.value="";
    controls.rep.value="";
    controls.supplier.value="";
    controls.src.value="";
    controls.sort.value="season_cat_code";
    controls.withImg.checked=true;
    applyAll();
  });
  if(controls.selectVisible){
    controls.selectVisible.addEventListener("click",function(){setVisibleFlags(true);});
  }
  if(controls.clearVisible){
    controls.clearVisible.addEventListener("click",function(){setVisibleFlags(false);});
  }
  if(controls.openFlag){
    controls.openFlag.addEventListener("click",function(){setPage("flag");});
  }
  if(controls.flagBack){
    controls.flagBack.addEventListener("click",function(){setPage("catalog");});
  }

  if(controls.clearFlag){
    controls.clearFlag.addEventListener("click",function(){
      flagged={};
      for(var i=0;i<cards.length;i++){setCardFlagVisual(cards[i],false);}
      renderFlaggedGrid();
      syncVisibleActionButtons();
    });
  }
  if(controls.exportFlag){
    controls.exportFlag.addEventListener("click",exportFlaggedExcel);
  }

  var modal=q("img-modal");
  var modalImg=q("img-modal-view");
  var modalClose=q("img-modal-close");
  var detailModal=q("detail-modal");
  var detailBody=q("detail-modal-body");
  var detailClose=q("detail-modal-close");
  function closeModal(){
    if(!modal||!modalImg){return;}
    modal.className="modal";
    modalImg.src="";
    modal.setAttribute("aria-hidden","true");
  }
  function closeDetailModal(){
    if(!detailModal||!detailBody){return;}
    detailModal.className="modal modal-detail";
    detailBody.innerHTML="";
    detailModal.setAttribute("aria-hidden","true");
  }
  function openDetailModal(card){
    if(!detailModal||!detailBody||!card){return;}
    var tpl=card.querySelector(".detail-template");
    if(!tpl){return;}
    detailBody.innerHTML=tpl.innerHTML||"";
    detailModal.className="modal modal-detail show";
    detailModal.setAttribute("aria-hidden","false");
  }

  function handleGridClick(ev){
    ev=ev||window.event;
    var t=ev.target||ev.srcElement;
    if(!t){return;}

    if(hasClass(t,"detail-btn")){
      if(ev.preventDefault){ev.preventDefault();}
      ev.returnValue=false;
      openDetailModal(closestCard(t));
      return;
    }

    if(hasClass(t,"zoomable")){
      if(ev.preventDefault){ev.preventDefault();}
      ev.returnValue=false;
      if(modal&&modalImg){
        modalImg.src=t.getAttribute("data-full")||t.getAttribute("src")||"";
        modal.className="modal show";
        modal.setAttribute("aria-hidden","false");
      }
    }
  }

  function handleGridChange(ev){
    ev=ev||window.event;
    var t=ev.target||ev.srcElement;
    if(!t){return;}
    if(toggleFlagFromCheckbox(t)){
      return;
    }
  }

  grid.addEventListener("click",handleGridClick);
  grid.addEventListener("change",handleGridChange);
  if(controls.flagGrid){controls.flagGrid.addEventListener("click",handleGridClick);}
  if(controls.flagGrid){controls.flagGrid.addEventListener("change",handleGridChange);}

  if(modal){
    modal.addEventListener("click",function(ev){
      ev=ev||window.event;
      var target=ev.target||ev.srcElement;
      if(target===modal){closeModal();}
    });
  }
  if(detailModal){
    detailModal.addEventListener("click",function(ev){
      ev=ev||window.event;
      var target=ev.target||ev.srcElement;
      if(target===detailModal){closeDetailModal();}
    });
  }
  if(modalClose){modalClose.addEventListener("click",closeModal);}
  if(detailClose){detailClose.addEventListener("click",closeDetailModal);}
  document.addEventListener("keydown",function(ev){
    ev=ev||window.event;
    var key=ev.key||ev.keyCode;
    if(key==="Escape"||key===27){closeModal();closeDetailModal();}
  });

  renderFlaggedGrid();
  setPage("catalog");
  applyAll();
})();
</script>
"""
    )
    out.append("</div></body></html>")
    return "\n".join(out)


def export_showcase_catalog(
    *,
    output_dir: str | Path,
    articles: Mapping[str, Article],
    codes: Sequence[str],
    export_mode: str,  # html | jpg | both
    source_mode: str,  # local_only | local_then_web | web_only | web_then_local
    code_to_local_image: Mapping[str, str | Path],
    fetch_remote_bytes: Optional[Callable[[str], Tuple[Optional[bytes], Optional[str]]]],
    title: str,
    price_lookup: Optional[Mapping[str, Mapping[str, Optional[float]]]] = None,
    progress_cb: Optional[Callable[[Dict[str, object]], None]] = None,
    status_cb: Optional[Callable[[str], None]] = None,
) -> Dict[str, object]:
    out_base = Path(output_dir)
    out_base.mkdir(parents=True, exist_ok=True)

    do_html = export_mode in ("html", "both")
    do_jpg = export_mode in ("jpg", "both")

    html_img_dir = out_base / "html" / "images"
    jpg_dir = out_base / "jpg"
    if do_html:
        html_img_dir.mkdir(parents=True, exist_ok=True)
    if do_jpg:
        jpg_dir.mkdir(parents=True, exist_ok=True)

    requested_keys: List[str] = []
    seen = set()
    for c in codes:
        key = str(c or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        requested_keys.append(key)

    exported_jpg = 0
    exported_html_images = 0
    used_local = 0
    used_web = 0
    missing_articles: List[str] = []
    missing_images: List[str] = []
    errors: List[str] = []
    source_report_lines: List[str] = []
    html_rows: List[Dict[str, str]] = []
    prices_attached = 0

    total = max(len(requested_keys), 1)
    for idx, article_key in enumerate(requested_keys, start=1):
        if status_cb:
            status_cb(f"{idx}/{len(requested_keys)} • {article_key}")

        art = articles.get(article_key)
        if art is None:
            missing_articles.append(article_key)
            source_report_lines.append(f"{article_key}\tARTICLE_MISSING\t-")
            if progress_cb:
                progress_cb(
                    {
                        "stage": "rendering_articles",
                        "ratio": idx / total,
                        "current": idx,
                        "total": len(requested_keys),
                        "current_article": article_key,
                        "message": f"Articolo {idx}/{len(requested_keys)} • {article_key}",
                        "exported_jpg": exported_jpg,
                        "exported_html_images": exported_html_images,
                        "used_local": used_local,
                        "used_web": used_web,
                    }
                )
            continue

        listino_price, saldo_price = _resolve_article_prices(price_lookup, art)
        if listino_price is not None or saldo_price is not None:
            prices_attached += 1

        img_bytes, err, src_kind = _resolve_image_bytes(
            code=art.code,
            source_mode=source_mode,
            code_to_local_image=code_to_local_image,
            fetch_remote_bytes=fetch_remote_bytes,
        )
        if src_kind == "local":
            used_local += 1
        elif src_kind == "web":
            used_web += 1
        source_report_lines.append(f"{art.season or '-'}\t{art.code}\t{src_kind or 'none'}\t{err or ''}")
        if not img_bytes:
            missing_images.append(f"{art.season or '-'}\t{art.code}\t{err or 'no_image'}")

        season_slug = _sanitize_folder_name(art.season or art.season_code or "", "SEASON")
        code_file = f"{idx:04d}_{season_slug}_{art.code.replace('/', '_')}.jpg"

        if do_jpg:
            try:
                cat_folder = _sanitize_folder_name(art.categoria, "CATEGORIA_SCONOSCIUTA")
                jpg_cat_dir = jpg_dir / cat_folder
                jpg_cat_dir.mkdir(parents=True, exist_ok=True)
                out_path = jpg_cat_dir / code_file
                card = render_showcase_jpg(
                    art,
                    img_bytes,
                    listino_price=listino_price,
                    saldo_price=saldo_price,
                )
                card.save(out_path, "JPEG", quality=95, subsampling=0, optimize=True)
                exported_jpg += 1
            except Exception as e:
                errors.append(f"{art.season or '-'}\t{art.code}\trender_jpg\t{type(e).__name__}:{e}")

        img_rel = ""
        if do_html and img_bytes:
            try:
                out_img = html_img_dir / code_file
                _save_bytes_as_jpeg(img_bytes, out_img)
                img_rel = f"images/{code_file}"
                exported_html_images += 1
            except Exception as e:
                errors.append(f"{art.season or '-'}\t{art.code}\thtml_image\t{type(e).__name__}:{e}")

        if do_html:
            html_rows.append(
                {
                    "code": art.code,
                    "season_norm": str(art.season or "").strip(),
                    "reparto": str(art.reparto or "").strip(),
                    "supplier": str(art.supplier or "").strip(),
                    "categoria": str(art.categoria or "").strip(),
                    "ven": f"{float(art.ven or 0.0):.0f}",
                    "ord": f"{float(art.con or 0.0):.0f}",  # ORD mapped to CON
                    "giac": f"{float(art.giac or 0.0):.0f}",
                    "prezzo_listino": f"{listino_price:.2f}" if listino_price is not None else "",
                    "prezzo_saldo": f"{saldo_price:.2f}" if saldo_price is not None else "",
                    "description": " • ".join([x for x in [art.description, art.color] if x]),
                    "img_rel": img_rel,
                    "source": src_kind or "none",
                    "source_detail": err or "",
                    "missing_reason": "" if img_rel else (err or "no_image"),
                    "detail_html": _build_article_detail_html(
                        art,
                        listino_price=listino_price,
                        saldo_price=saldo_price,
                    ),
                }
            )

        if progress_cb:
            progress_cb(
                {
                    "stage": "rendering_articles",
                    "ratio": idx / total,
                    "current": idx,
                    "total": len(requested_keys),
                    "current_article": art.code,
                    "current_season": art.season or art.season_code or "",
                    "message": f"Articolo {idx}/{len(requested_keys)} • {art.code}",
                    "exported_jpg": exported_jpg,
                    "exported_html_images": exported_html_images,
                    "used_local": used_local,
                    "used_web": used_web,
                }
            )

    html_path = None
    if do_html:
        try:
            if progress_cb:
                progress_cb(
                    {
                        "stage": "building_html",
                        "ratio": 1.0,
                        "current": len(requested_keys),
                        "total": len(requested_keys),
                        "message": "Composizione catalogo HTML",
                        "exported_jpg": exported_jpg,
                        "exported_html_images": exported_html_images,
                        "used_local": used_local,
                        "used_web": used_web,
                    }
                )
            html_content = _build_catalog_html(title=title, items=html_rows)
            html_path = out_base / "html" / "catalogo.html"
            html_path.write_text(html_content, encoding="utf-8")
        except Exception as e:
            errors.append(f"html_build\t{type(e).__name__}:{e}")

    return {
        "requested": len(requested_keys),
        "exported_jpg": exported_jpg,
        "exported_html_images": exported_html_images,
        "used_local": used_local,
        "used_web": used_web,
        "prices_attached": prices_attached,
        "missing_articles": missing_articles,
        "missing_images": missing_images,
        "errors": errors,
        "image_source_report_lines": source_report_lines,
        "html_path": str(html_path) if html_path else "",
        "output_dir": str(out_base),
    }
