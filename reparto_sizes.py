from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Optional


REPARTO_DONNA = "SCARPE DONNA"
REPARTO_UOMO = "SCARPE UOMO"
SUPPORTED_SIZES = list(range(35, 49))
DEFAULT_SIZE_COLUMNS = [f"Size_{size}" for size in SUPPORTED_SIZES]
DB_SIZE_COLUMNS = [f"size_{size}" for size in SUPPORTED_SIZES]


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("_", " ").replace("-", " ").split()).strip()


def normalize_reparto(value: Any) -> Optional[str]:
    text = _clean_text(value).upper()
    if not text:
        return None
    if "UOMO" in text:
        return REPARTO_UOMO
    if "DONNA" in text:
        return REPARTO_DONNA
    return text


def infer_reparto_from_values(*values: Any) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        candidates: list[Any] = []
        if isinstance(value, Path):
            candidates.extend([value.as_posix(), value.name, value.stem, *value.parts])
        elif isinstance(value, (list, tuple, set)):
            candidates.extend(list(value))
        else:
            candidates.append(value)
        for candidate in candidates:
            reparto = normalize_reparto(candidate)
            if reparto in {REPARTO_DONNA, REPARTO_UOMO}:
                return reparto
    return None


def infer_reparto_from_path(path: Any) -> Optional[str]:
    if path is None:
        return None
    try:
        return infer_reparto_from_values(Path(path))
    except Exception:
        return infer_reparto_from_values(path)


def supported_sizes_from_values(values: Optional[Iterable[Any]]) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for value in values or []:
        try:
            size = int(str(value).strip())
        except Exception:
            continue
        if size not in seen and size in SUPPORTED_SIZES:
            seen.add(size)
            out.append(size)
    out.sort()
    return out


def size_column_name(size: int, prefix: str = "Size_") -> str:
    return f"{prefix}{int(size)}"


def size_columns(prefix: str = "Size_", sizes: Optional[Iterable[int]] = None) -> list[str]:
    return [size_column_name(size, prefix=prefix) for size in (sizes or SUPPORTED_SIZES)]


def infer_size_columns(columns: Iterable[Any], prefix: str = "Size_") -> list[str]:
    parsed: list[tuple[int, str]] = []
    for raw in columns:
        text = str(raw).strip()
        if not text.startswith(prefix):
            continue
        try:
            size = int(text.split("_", 1)[1])
        except Exception:
            continue
        if size in SUPPORTED_SIZES:
            parsed.append((size, text))
    parsed.sort(key=lambda item: item[0])
    return [text for _, text in parsed]


def _center_sizes(available_sizes: list[int], count: int) -> list[int]:
    if not available_sizes:
        return []
    safe_count = max(1, min(int(count), len(available_sizes)))
    start = max(0, (len(available_sizes) - safe_count) // 2)
    return available_sizes[start : start + safe_count]


def required_core_sizes(
    fascia: Any,
    *,
    reparto: Any = None,
    available_sizes: Optional[Iterable[Any]] = None,
) -> list[int]:
    try:
        fascia_rank = int(float(fascia))
    except Exception:
        fascia_rank = 99
    core_count = 5 if fascia_rank in (1, 2) else 3
    reparto_norm = normalize_reparto(reparto)
    sizes = supported_sizes_from_values(available_sizes) or list(SUPPORTED_SIZES)

    preferred: list[int] = []
    if reparto_norm == REPARTO_UOMO:
        preferred = [40, 41, 42, 43, 44] if core_count >= 5 else [41, 42, 43]
    elif reparto_norm == REPARTO_DONNA:
        preferred = [36, 37, 38, 39, 40] if core_count >= 5 else [37, 38, 39]

    filtered = [size for size in preferred if size in sizes]
    if filtered:
        return filtered
    return _center_sizes(sizes, core_count)
