from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

import pandas as pd


_CODE_RE = re.compile(r"^\d{1,3}/[A-Z0-9]{2,}$", re.IGNORECASE)
_DELIM_CANDIDATES = (";", ",", "\t", "|")
_LISTINO_MARKERS = ("ANALISI ARTICOLI",)
_SALDO_MARKERS = ("ANALISI LISTINI E RICARICHI",)
_SEASON_RE = re.compile(r"(?<!\d)(?:20)?(\d{2})[\s_\-]*([A-Z])(?![A-Z0-9])", re.IGNORECASE)


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
