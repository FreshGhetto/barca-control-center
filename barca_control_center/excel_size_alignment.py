from __future__ import annotations

from typing import Any, Callable, Mapping, Sequence, TypeVar


K = TypeVar("K")


def detect_size_data_shift(
    row: Sequence[Any],
    size_col_map: Mapping[K, int],
    *,
    normalize: Callable[[Any], str] | None = None,
) -> int:
    """Restituisce sempre 0: il simbolo '%' nella prima colonna taglia
    è un residuo del campo %VEN, NON un indicatore di shift.
    I dati delle taglie sono sempre allineati con le posizioni dell'header.

    Bug precedente: il '%' veniva interpretato come shift +1, causando
    la lettura sbagliata di tutte le quantità per taglia nelle righe
    con percentuale vendita > 0 (le taglie risultavano scalate di 1).
    """
    return 0
