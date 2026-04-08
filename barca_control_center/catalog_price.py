from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd


_CODE_RE = re.compile(r"^[A-Z0-9]{1,4}/[A-Z0-9]{2,}$", re.IGNORECASE)
_DELIM_CANDIDATES = (";", ",", "\t", "|")
_LISTINO_MARKERS = ("ANALISI ARTICOLI",)
_SALDO_MARKERS = ("ANALISI LISTINI E RICARICHI",)
_SEASON_RE = re.compile(r"(?<!\d)(?:20)?(\d{2})[\s_\-]*([A-Z])(?![A-Z0-9])", re.IGNORECASE)
_COLOR_CONTEXT_RE = re.compile(r"^\d{1,3}\s+[A-Z]", re.IGNORECASE)
_NUMERICISH_RE = re.compile(r"^[\d\s.,%-]+$")


def _decode_best_effort(data: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin1"):
        try:
            return data.decode(encoding)
        except Exception:
            continue
    return data.decode("latin1", errors="replace")


def _sniff_delimiter(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,\t|")
        return dialect.delimiter
    except Exception:
        counts = {delim: sample.count(delim) for delim in _DELIM_CANDIDATES}
        return max(counts, key=counts.get) if counts else ","


def _normalize_code(token: str) -> str:
    return str(token or "").strip().upper().replace(" ", "")


def _first_token(value: str) -> str:
    parts = str(value or "").strip().split()
    return parts[0] if parts else ""


def _is_article_code(token: str) -> bool:
    return bool(_CODE_RE.match(_normalize_code(token)))


def _to_float(value: str) -> Optional[float]:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except Exception:
        return None


def _read_rows(data: bytes) -> list[list[str]]:
    text = _decode_best_effort(data)
    sample = "\n".join(text.splitlines()[:25])
    delimiter = _sniff_delimiter(sample)
    return list(csv.reader(io.StringIO(text), delimiter=delimiter))


def _detect_price_kind(data: bytes) -> str:
    probe = _decode_best_effort(data)[:20000].upper()
    if all(marker in probe for marker in ("MATERIALE", "COLORE", "MARCHIO", "ARTICOLO")):
        return "metadata_color"
    if any(marker in probe for marker in _SALDO_MARKERS):
        return "saldo"
    if any(marker in probe for marker in _LISTINO_MARKERS):
        return "listino"
    return "unknown"


def _extract_season_code(name: str, data: bytes | None = None) -> str:
    match = _SEASON_RE.search(str(name or "").upper())
    if match:
        return f"{match.group(1)}{match.group(2).upper()}"
    if data is not None:
        probe = _decode_best_effort(data)[:5000].upper()
        match = _SEASON_RE.search(probe)
        if match:
            return f"{match.group(1)}{match.group(2).upper()}"
    return "UNKNOWN"


def _emit_progress(progress_cb: Optional[Callable[[Dict[str, Any]], None]], **payload: Any) -> None:
    if progress_cb is None:
        return
    try:
        progress_cb(dict(payload))
    except Exception:
        return


def _normalize_label(value: Any) -> Optional[str]:
    text = str(value or "").replace("\xa0", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text or None


def _normalize_reparto(value: Any) -> Optional[str]:
    text = _normalize_label(value)
    if not text:
        return None
    upper = text.upper()
    if "SCARPE UOMO" in upper:
        return "SCARPE UOMO"
    if "SCARPE DONNA" in upper:
        return "SCARPE DONNA"
    if "ABBIGLIAMENTO UOMO" in upper:
        return "ABBIGLIAMENTO UOMO"
    if "ABBIGLIAMENTO DONNA" in upper:
        return "ABBIGLIAMENTO DONNA"
    if "TENNIS UNISEX" in upper:
        return "TENNIS UNISEX"
    if "PELLETTERIA" in upper:
        return "PELLETTERIA"
    return None


def _is_numericish(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return bool(_NUMERICISH_RE.fullmatch(text))


def _clean_context_tokens(chunk: Sequence[str]) -> List[str]:
    out: List[str] = []
    for item in chunk:
        text = _normalize_label(item)
        if not text:
            continue
        upper = text.upper()
        if upper in {"%", "ARTICOLO"}:
            continue
        if upper.startswith(("SUBTOTALE", "TOTALI", "VALORE ", "COSTO ")):
            continue
        if _is_numericish(text):
            continue
        out.append(text)
    return out


def _split_article_cell(value: Any) -> Tuple[Optional[str], Optional[str]]:
    text = _normalize_label(value)
    if not text:
        return None, None
    parts = text.split(maxsplit=1)
    code = _normalize_code(parts[0]) if parts else ""
    if not _is_article_code(code):
        return None, None
    description = parts[1].strip() if len(parts) > 1 else None
    return code, description or None


def _first_non_empty(series: pd.Series) -> Optional[str]:
    for value in series:
        text = _normalize_label(value)
        if text:
            return text
    return None


def _aggregate_article_metadata(frame: pd.DataFrame) -> pd.DataFrame:
    columns = ["season_code", "article_code", "description", "reparto", "categoria", "marchio", "tipologia", "color", "materiale"]
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)
    out = frame.copy()
    out["season_code"] = out["season_code"].astype(str).str.strip().str.upper().replace("", "UNKNOWN")
    out["article_code"] = out["article_code"].astype(str).map(_normalize_code)
    for col in ("description", "reparto", "categoria", "marchio", "tipologia", "color", "materiale"):
        if col not in out.columns:
            out[col] = None
        out[col] = out[col].map(_normalize_label)
    out = out[out["article_code"].map(_is_article_code)].copy()
    if out.empty:
        return pd.DataFrame(columns=columns)
    grouped = (
        out.groupby(["season_code", "article_code"], as_index=False)
        .agg(
            {
                "description": _first_non_empty,
                "reparto": _first_non_empty,
                "categoria": _first_non_empty,
                "marchio": _first_non_empty,
                "tipologia": _first_non_empty,
                "color": _first_non_empty,
                "materiale": _first_non_empty,
            }
        )
        .reindex(columns=columns)
    )
    return grouped.sort_values(["season_code", "article_code"]).reset_index(drop=True)


def extract_article_metadata_from_color_csv_bytes(data: bytes) -> pd.DataFrame:
    rows_out: List[Dict[str, Optional[str]]] = []
    current_reparto = current_materiale = current_colore = current_marchio = None

    for row in _read_rows(data):
        if "ARTICOLO" not in row:
            continue
        elements = row[row.index("ARTICOLO") + 1 :]
        if "TOTALI :" in elements:
            elements = elements[: elements.index("TOTALI :")]
        article_indices = [
            idx
            for idx, val in enumerate(elements)
            if _split_article_cell(val)[0]
        ]
        last_idx = 0
        for idx in article_indices:
            pre_chunk = _clean_context_tokens(elements[last_idx:idx])
            reparto_candidates = [_normalize_reparto(item) for item in pre_chunk if _normalize_reparto(item)]
            if reparto_candidates:
                current_reparto = reparto_candidates[-1]
            color_candidates = [item for item in pre_chunk if _COLOR_CONTEXT_RE.match(item)]
            if color_candidates:
                current_colore = color_candidates[-1]
            residual = [item for item in pre_chunk if _normalize_reparto(item) is None and item not in color_candidates]
            if residual:
                current_marchio = residual[-1]
            if len(residual) >= 2:
                current_materiale = residual[-2]

            article_code, description = _split_article_cell(elements[idx])
            if not article_code:
                last_idx = idx + 1
                continue
            rows_out.append(
                {
                    "article_code": article_code,
                    "description": description,
                    "reparto": current_reparto,
                    "categoria": None,
                    "marchio": current_marchio,
                    "tipologia": None,
                    "color": current_colore,
                    "materiale": current_materiale,
                }
            )
            last_idx = idx + 1

    if not rows_out:
        return pd.DataFrame(columns=["article_code", "description", "reparto", "categoria", "marchio", "tipologia", "color", "materiale"])
    return pd.DataFrame(rows_out)


def extract_article_metadata_from_listino_csv_bytes(data: bytes) -> pd.DataFrame:
    rows_out: List[Dict[str, Optional[str]]] = []
    current_reparto = current_categoria = None

    for row in _read_rows(data):
        if "ARTICOLO" not in row:
            continue
        elements = row[row.index("ARTICOLO") + 1 :]
        if "TOTALI :" in elements:
            elements = elements[: elements.index("TOTALI :")]
        article_indices = [
            idx
            for idx, val in enumerate(elements)
            if _split_article_cell(val)[0]
        ]
        last_idx = 0
        for idx in article_indices:
            pre_chunk = _clean_context_tokens(elements[last_idx:idx])
            reparto_candidates = [_normalize_reparto(item) for item in pre_chunk if _normalize_reparto(item)]
            if reparto_candidates:
                current_reparto = reparto_candidates[-1]
            residual = [item for item in pre_chunk if _normalize_reparto(item) is None]
            if residual:
                current_categoria = residual[-1]

            article_code, description = _split_article_cell(elements[idx])
            if not article_code:
                last_idx = idx + 1
                continue
            rows_out.append(
                {
                    "article_code": article_code,
                    "description": description,
                    "reparto": current_reparto,
                    "categoria": current_categoria,
                    "marchio": None,
                    "tipologia": None,
                    "color": None,
                    "materiale": None,
                }
            )
            last_idx = idx + 1

    if not rows_out:
        return pd.DataFrame(columns=["article_code", "description", "reparto", "categoria", "marchio", "tipologia", "color", "materiale"])
    return pd.DataFrame(rows_out)


def extract_article_metadata_from_saldo_csv_bytes(data: bytes) -> pd.DataFrame:
    rows_out: List[Dict[str, Optional[str]]] = []
    current_reparto = current_categoria = None

    for row in _read_rows(data):
        if "ARTICOLO" not in row:
            continue
        elements = row[row.index("ARTICOLO") + 1 :]
        if "TOTALI :" in elements:
            elements = elements[: elements.index("TOTALI :")]
        article_indices = [
            idx
            for idx, val in enumerate(elements)
            if _split_article_cell(val)[0]
        ]
        last_idx = 0
        for idx in article_indices:
            pre_chunk = _clean_context_tokens(elements[last_idx:idx])
            reparto_candidates = [_normalize_reparto(item) for item in pre_chunk if _normalize_reparto(item)]
            if reparto_candidates:
                current_reparto = reparto_candidates[-1]
            residual = [item for item in pre_chunk if _normalize_reparto(item) is None]
            if residual:
                current_categoria = residual[-1]

            article_code, description = _split_article_cell(elements[idx])
            if not article_code:
                last_idx = idx + 1
                continue
            rows_out.append(
                {
                    "article_code": article_code,
                    "description": description,
                    "reparto": current_reparto,
                    "categoria": current_categoria,
                    "marchio": None,
                    "tipologia": None,
                    "color": None,
                    "materiale": None,
                }
            )
            last_idx = idx + 1

    if not rows_out:
        return pd.DataFrame(columns=["article_code", "description", "reparto", "categoria", "marchio", "tipologia", "color", "materiale"])
    return pd.DataFrame(rows_out)


def extract_listino_prices_from_csv_bytes(data: bytes) -> pd.DataFrame:
    out = []
    for row in _read_rows(data):
        code_idx = None
        code = ""
        for idx, value in enumerate(row):
            token = _first_token(value)
            if _is_article_code(token):
                code_idx = idx
                code = _normalize_code(token)
                break
        if code_idx is None:
            continue

        elements = row[code_idx + 1 :]
        if "TOTALI :" in elements:
            elements = elements[: elements.index("TOTALI :")]

        price = _to_float(elements[0]) if elements else None
        if price is None:
            for idx, value in enumerate(elements):
                if str(value or "").strip() != "%" or idx <= 0:
                    continue
                price = _to_float(elements[idx - 1])
                if price is not None:
                    break
        if price is not None:
            out.append({"article_code": code, "price_listino": float(price)})

    if not out:
        return pd.DataFrame(columns=["article_code", "price_listino"])
    return pd.DataFrame(out).drop_duplicates(subset=["article_code"], keep="last").sort_values("article_code")


def extract_saldo_prices_from_csv_bytes(data: bytes) -> pd.DataFrame:
    out = []
    for row in _read_rows(data):
        code_idx = None
        code = ""
        for idx, value in enumerate(row):
            token = _first_token(value)
            if _is_article_code(token):
                code_idx = idx
                code = _normalize_code(token)
                break
        if code_idx is None:
            continue

        elements = row[code_idx + 1 :]
        if "TOTALI :" in elements:
            elements = elements[: elements.index("TOTALI :")]

        price = _to_float(elements[1]) if len(elements) >= 2 else None
        if price is None:
            for value in elements:
                price = _to_float(value)
                if price is not None:
                    break
        if price is not None:
            out.append({"article_code": code, "price_saldo": float(price)})

    if not out:
        return pd.DataFrame(columns=["article_code", "price_saldo"])
    return pd.DataFrame(out).drop_duplicates(subset=["article_code"], keep="last").sort_values("article_code")


def build_price_snapshot_from_files(
    csv_files: Sequence[Path],
    progress_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    listino_frames: list[pd.DataFrame] = []
    saldo_frames: list[pd.DataFrame] = []

    stats = {
        "input_files": 0,
        "listino_files": 0,
        "saldo_files": 0,
        "metadata_files": 0,
        "skipped_files": 0,
        "merged_rows": 0,
    }

    total_files = len(csv_files)
    for idx, path in enumerate(csv_files, start=1):
        stats["input_files"] += 1
        _emit_progress(
            progress_cb,
            stage="parsing_price",
            file_name=path.name,
            current=idx,
            total=total_files,
            message=f"Analisi CSV prezzi {idx}/{total_files}: {path.name}",
        )
        data = Path(path).read_bytes()
        season_code = _extract_season_code(Path(path).name, data)
        kind = _detect_price_kind(data)
        if kind == "metadata_color":
            stats["metadata_files"] += 1
            _emit_progress(
                progress_cb,
                stage="parsing_price",
                file_name=path.name,
                current=idx,
                total=total_files,
                detected_kind="metadata_color_skipped",
                season_code=season_code,
                rows=0,
                message=f"CSV metadati escluso dai prezzi {idx}/{total_files}: {path.name}",
            )
            continue
        if kind == "unknown":
            listino_probe = extract_listino_prices_from_csv_bytes(data)
            saldo_probe = extract_saldo_prices_from_csv_bytes(data)
            if listino_probe.empty and saldo_probe.empty:
                stats["skipped_files"] += 1
                _emit_progress(
                    progress_cb,
                    stage="parsing_price",
                    file_name=path.name,
                    current=idx,
                    total=total_files,
                    detected_kind="ignored",
                    season_code=season_code,
                    rows=0,
                    message=f"CSV ignorato {idx}/{total_files}: {path.name}",
                )
                continue
            kind = "listino" if len(listino_probe) >= len(saldo_probe) else "saldo"

        if kind == "listino":
            df = extract_listino_prices_from_csv_bytes(data)
            if not df.empty:
                df["season_code"] = season_code
                listino_frames.append(df)
            stats["listino_files"] += 1
            _emit_progress(
                progress_cb,
                stage="parsing_price",
                file_name=path.name,
                current=idx,
                total=total_files,
                detected_kind="listino",
                season_code=season_code,
                rows=int(len(df)),
                message=f"CSV listino {idx}/{total_files}: {path.name}",
            )
            continue

        df = extract_saldo_prices_from_csv_bytes(data)
        if not df.empty:
            df["season_code"] = season_code
            saldo_frames.append(df)
        stats["saldo_files"] += 1
        _emit_progress(
            progress_cb,
            stage="parsing_price",
            file_name=path.name,
            current=idx,
            total=total_files,
            detected_kind="saldo",
            season_code=season_code,
            rows=int(len(df)),
            message=f"CSV saldo {idx}/{total_files}: {path.name}",
        )

    if listino_frames:
        listino_df = pd.concat(listino_frames, ignore_index=True)
        listino_df = listino_df.drop_duplicates(subset=["season_code", "article_code"], keep="last")
    else:
        listino_df = pd.DataFrame(columns=["season_code", "article_code", "price_listino"])

    if saldo_frames:
        saldo_df = pd.concat(saldo_frames, ignore_index=True)
        saldo_df = saldo_df.drop_duplicates(subset=["season_code", "article_code"], keep="last")
    else:
        saldo_df = pd.DataFrame(columns=["season_code", "article_code", "price_saldo"])

    if not listino_df.empty and not saldo_df.empty:
        merged = listino_df.merge(saldo_df, on=["season_code", "article_code"], how="outer")
    elif not listino_df.empty:
        merged = listino_df.copy()
        merged["price_saldo"] = pd.NA
    elif not saldo_df.empty:
        merged = saldo_df.copy()
        merged["price_listino"] = pd.NA
    else:
        merged = pd.DataFrame(columns=["season_code", "article_code", "price_listino", "price_saldo"])

    merged["season_code"] = merged["season_code"].astype(str).str.strip().str.upper().replace("", "UNKNOWN")
    merged["article_code"] = merged["article_code"].astype(str).map(_normalize_code)
    merged["price_listino"] = pd.to_numeric(merged["price_listino"], errors="coerce")
    merged["price_saldo"] = pd.to_numeric(merged["price_saldo"], errors="coerce")
    merged = merged.drop_duplicates(subset=["season_code", "article_code"], keep="last")
    merged = merged.sort_values(["season_code", "article_code"]).reset_index(drop=True)
    stats["merged_rows"] = int(len(merged))
    _emit_progress(
        progress_cb,
        stage="parsing_price",
        current=total_files,
        total=total_files,
        merged_rows=int(len(merged)),
        message=f"CSV prezzi elaborati: {int(len(merged))} righe aggregate",
    )
    return merged, stats


def build_article_metadata_from_files(
    csv_files: Sequence[Path],
    progress_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    frames: list[pd.DataFrame] = []
    stats = {
        "input_files": 0,
        "color_files": 0,
        "saldo_files": 0,
        "listino_files": 0,
        "skipped_files": 0,
        "merged_rows": 0,
    }

    total_files = len(csv_files)
    for idx, path in enumerate(csv_files, start=1):
        stats["input_files"] += 1
        _emit_progress(
            progress_cb,
            stage="parsing_article_meta",
            file_name=path.name,
            current=idx,
            total=total_files,
            message=f"Analisi CSV metadati {idx}/{total_files}: {path.name}",
        )
        data = Path(path).read_bytes()
        season_code = _extract_season_code(Path(path).name, data)
        kind = _detect_price_kind(data)

        if kind == "metadata_color":
            df = extract_article_metadata_from_color_csv_bytes(data)
            stats["color_files"] += 1
            detected_kind = "metadata_color"
        elif kind == "saldo":
            df = extract_article_metadata_from_saldo_csv_bytes(data)
            stats["saldo_files"] += 1
            detected_kind = "saldo"
        elif kind == "listino":
            df = extract_article_metadata_from_listino_csv_bytes(data)
            stats["listino_files"] += 1
            detected_kind = "listino"
        else:
            stats["skipped_files"] += 1
            _emit_progress(
                progress_cb,
                stage="parsing_article_meta",
                file_name=path.name,
                current=idx,
                total=total_files,
                detected_kind="ignored",
                season_code=season_code,
                rows=0,
                message=f"CSV metadati ignorato {idx}/{total_files}: {path.name}",
            )
            continue

        if not df.empty:
            df["season_code"] = season_code
            df["source_rank"] = 0 if kind == "metadata_color" else 1 if kind == "saldo" else 2
            frames.append(df)

        _emit_progress(
            progress_cb,
            stage="parsing_article_meta",
            file_name=path.name,
            current=idx,
            total=total_files,
            detected_kind=detected_kind,
            season_code=season_code,
            rows=int(len(df)),
            message=f"CSV metadati {detected_kind} {idx}/{total_files}: {path.name}",
        )

    if not frames:
        merged = pd.DataFrame(columns=["season_code", "article_code", "description", "reparto", "categoria", "marchio", "tipologia", "color", "materiale"])
    else:
        merged_raw = pd.concat(frames, ignore_index=True)
        if "source_rank" in merged_raw.columns:
            merged_raw = merged_raw.sort_values(["season_code", "article_code", "source_rank"]).reset_index(drop=True)
            merged_raw = merged_raw.drop(columns=["source_rank"])
        merged = _aggregate_article_metadata(merged_raw)

    stats["merged_rows"] = int(len(merged))
    _emit_progress(
        progress_cb,
        stage="parsing_article_meta",
        current=total_files,
        total=total_files,
        merged_rows=int(len(merged)),
        message=f"CSV metadati elaborati: {int(len(merged))} righe aggregate",
    )
    return merged, stats
