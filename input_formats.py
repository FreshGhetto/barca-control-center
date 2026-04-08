from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import unicodedata
from typing import Dict, List, Sequence


@dataclass(frozen=True)
class InputFormatSpec:
    kind: str
    target_group: str
    description: str
    required_groups: tuple[tuple[str, ...], ...]
    optional_groups: tuple[tuple[str, ...], ...] = ()
    filename_hints: tuple[str, ...] = ()


KNOWN_INPUT_FORMATS: tuple[InputFormatSpec, ...] = (
    InputFormatSpec(
        kind="stock_report",
        target_group="distribution",
        description="Raw stock snapshot per negozio con giacenza e taglie.",
        required_groups=(
            ("SITUAZIONE ARTICOLI PER NEGOZIO", "SITUAZIONE ARTICOLI", "SITUAZ ARTICOLI"),
            ("NEG", "NEGOZIO", "SHOP"),
            ("GIAC", "GIACENZA"),
            ("VEN", "VEND"),
        ),
        optional_groups=(
            ("RIC", "RICEVUTO"),
            ("% VEN", "SELL OUT", "SELLOUT"),
        ),
        filename_hints=("STOCK", "SITUAZ", "GIAC"),
    ),
    InputFormatSpec(
        kind="orders_prices",
        target_group="orders",
        description="Listino acquisto-vendita e marginalita' per articolo.",
        required_groups=(
            ("ANALISI LISTINI E RICARICHI", "LISTINI E RICARICHI"),
            ("PREZZO", "ACQUIS"),
            ("VENDITA", "LISTINO"),
            ("FASCE PRZ", "RIC", "MARGINE"),
        ),
        filename_hints=("PREZZO", "LISTINO", "RICARICHI"),
    ),
    InputFormatSpec(
        kind="orders_sd_3",
        target_group="orders",
        description="Analisi per singola taglia.",
        required_groups=(
            ("ANALISI PER SINGOLA TAGLIA", "SINGOLA TAGLIA"),
            ("TAG",),
            ("TOT",),
        ),
        optional_groups=(
            ("ARTICOLO",),
            ("COLORE",),
            ("MATERIALE",),
        ),
        filename_hints=("_SD_3", "TAGLIE", "TAGLIA"),
    ),
    InputFormatSpec(
        kind="orders_sd_4",
        target_group="orders",
        description="Listino/fasce prezzo articolo.",
        required_groups=(
            ("ANALISI ARTICOLI",),
            ("FASCE PRZ",),
            ("LISTINO", "PREZZO"),
        ),
        optional_groups=(("ARTICOLO",),),
        filename_hints=("_SD_4", "LISTINO", "FASCE"),
    ),
    InputFormatSpec(
        kind="orders_history_detail",
        target_group="orders",
        description="Storico analisi articoli con marchio, colore, materiale e venduto periodo.",
        required_groups=(
            ("ANALISI ARTICOLI",),
            ("RAFFRONTA CON VENDUTO NEL PERIODO",),
            ("COLORE", "COLOR"),
            ("MATERIALE", "MATERIAL"),
            ("MARCHIO", "BRAND", "FORNITORE"),
        ),
        optional_groups=(
            ("ARTICOLO",),
            ("CATEGORIA",),
        ),
        filename_hints=("VENDUTO_PERIODO", "HISTORY_DETAIL"),
    ),
    InputFormatSpec(
        kind="sales_report",
        target_group="distribution",
        description="Vendite/flow per articolo-negozio da Analisi Articoli.",
        required_groups=(
            ("ANALISI ARTICOLI",),
            ("NEGOZIO", "SHOP"),
            ("CON", "CONSEGNATO"),
            ("VEND", "VENDUTO"),
        ),
        optional_groups=(
            ("FORNITORE", "MARCHIO", "BRAND"),
            ("PERIO", "PERIODO"),
            ("%", "SELL OUT", "SELLOUT"),
        ),
        filename_hints=("VENDITE", "SELL", "SALES"),
    ),
    InputFormatSpec(
        kind="orders_sd_1",
        target_group="orders",
        description="Analisi articoli totali per stagione/reparto/tipologia/marchio.",
        required_groups=(
            ("ANALISI ARTICOLI",),
            ("TIPOLOGIA", "TIPOLOGIE"),
            ("MARCHIO", "BRAND", "FORNITORE"),
            ("ARTICOLO",),
        ),
        optional_groups=(
            ("CATEGORIA",),
            ("RAFFRONTA CON VENDUTO NEL PERIODO",),
        ),
        filename_hints=("_SD_1", "TOTALI", "TOT"),
    ),
    InputFormatSpec(
        kind="orders_sd_2",
        target_group="orders",
        description="Analisi articoli per colore e materiale.",
        required_groups=(
            ("ANALISI ARTICOLI",),
            ("COLORE", "COLOR"),
            ("MATERIALE", "MATERIAL"),
            ("ARTICOLO",),
        ),
        optional_groups=(
            ("RAFFRONTA CON VENDUTO NEL PERIODO",),
            ("CATEGORIA",),
        ),
        filename_hints=("_SD_2", "COLORI", "COLORE", "MATERIALE"),
    ),
)


def _normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.upper()
    text = re.sub(r"[\r\n\t]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _match_group(text: str, variants: Sequence[str]) -> str | None:
    for variant in variants:
        token = _normalize_text(variant)
        if token and token in text:
            return token
    return None


def classify_input_file(path: Path, preview: str) -> Dict[str, object]:
    normalized_preview = _normalize_text(preview)
    normalized_name = _normalize_text(path.name)

    best: Dict[str, object] = {
        "kind": "unknown",
        "target_group": "quarantine",
        "confidence": 0.0,
        "reasons": [],
        "description": "Formato non riconosciuto.",
    }
    best_score = -1.0

    for spec in KNOWN_INPUT_FORMATS:
        reasons: List[str] = []
        required_hits = 0
        for group in spec.required_groups:
            hit = _match_group(normalized_preview, group)
            if hit is None:
                required_hits = -1
                break
            required_hits += 1
            reasons.append(f"header:{hit}")
        if required_hits < 0:
            continue

        optional_hits = 0
        for group in spec.optional_groups:
            hit = _match_group(normalized_preview, group)
            if hit is None:
                continue
            optional_hits += 1
            reasons.append(f"optional:{hit}")

        filename_hits = 0
        for hint in spec.filename_hints:
            token = _normalize_text(hint)
            if token and token in normalized_name:
                filename_hits += 1
                reasons.append(f"filename:{token}")

        score = float(required_hits * 10 + optional_hits * 2 + filename_hits * 3)
        max_score = float(len(spec.required_groups) * 10 + len(spec.optional_groups) * 2 + len(spec.filename_hints) * 3)
        confidence = round(score / max_score, 3) if max_score > 0 else 0.0

        if score <= best_score:
            continue

        best_score = score
        best = {
            "kind": spec.kind,
            "target_group": spec.target_group,
            "confidence": confidence,
            "reasons": reasons,
            "description": spec.description,
        }

    return best


def describe_known_input_formats() -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for spec in KNOWN_INPUT_FORMATS:
        rows.append(
            {
                "kind": spec.kind,
                "target_group": spec.target_group,
                "description": spec.description,
                "required_groups": [list(group) for group in spec.required_groups],
                "optional_groups": [list(group) for group in spec.optional_groups],
                "filename_hints": list(spec.filename_hints),
            }
        )
    return rows
