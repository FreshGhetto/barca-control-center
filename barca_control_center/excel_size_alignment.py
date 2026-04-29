from __future__ import annotations

from typing import Any, Callable, Mapping, Sequence, TypeVar


K = TypeVar("K")


def detect_size_data_shift(
    row: Sequence[Any],
    size_col_map: Mapping[K, int],
    *,
    normalize: Callable[[Any], str] | None = None,
) -> int:
    if not size_col_map:
        return 0

    first_size_col = min(size_col_map.values())
    if first_size_col >= len(row):
        return 0

    raw_value = row[first_size_col]
    if normalize is None:
        text = "" if raw_value is None else str(raw_value).strip()
    else:
        text = normalize(raw_value).strip()

    return 1 if text in {"%", "％"} else 0
