from __future__ import annotations

import csv
import datetime as dt
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from sklearn.ensemble import RandomForestRegressor
except Exception:  # pragma: no cover - optional dependency
    RandomForestRegressor = None


@dataclass
class SeasonBundle:
    code: str
    folder: Path
    totali: Path
    colori: Path
    taglie: Path
    listino: Optional[Path] = None
    prezzi: Optional[Path] = None


class StepLogger:
    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.events: List[str] = []

    def log(self, message: str):
        line = f"[{dt.datetime.now().strftime('%H:%M:%S')}] {message}"
        self.events.append(line)
        if self.verbose:
            print(line)

    def write(self, path: Path):
        path.write_text("\n".join(self.events) + ("\n" if self.events else ""), encoding="utf-8")


def has_order_inputs(root: Path) -> bool:
    if not root.exists():
        return False
    pattern = re.compile(r".+_sd_[123]\.csv$", re.IGNORECASE)
    return any(pattern.match(p.name) for p in root.rglob("*.csv"))


def _season_sort_key(code: str) -> Tuple[int, int, str]:
    code = str(code).strip().lower()
    m = re.search(r"(\d{2,4})", code)
    year = int(m.group(1)) if m else -1
    if 0 <= year < 100:
        year += 2000
    season_char = next((ch for ch in reversed(code) if ch.isalpha()), "")
    season_rank = {"y": 0, "g": 0, "i": 1, "e": 1}.get(season_char, 9)
    return year, season_rank, code


def _find_related_price_file(folder: Path, season_code: str, all_csvs: List[Path]) -> Optional[Path]:
    exact = folder / f"{season_code}_prezzo_acq-ven.csv"
    if exact.exists():
        return exact
    generic = folder / "prezzo_acq-ven.csv"
    if generic.exists():
        return generic

    prefix = f"{season_code}_"
    candidates = [p for p in all_csvs if p.name.lower().endswith("prezzo_acq-ven.csv")]
    season_hits = [p for p in candidates if p.name.lower().startswith(prefix.lower())]
    if season_hits:
        season_hits.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return season_hits[0]
    if candidates:
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return candidates[0]
    return None


def discover_order_bundles(root: Path) -> Dict[str, Any]:
    all_csvs = sorted(root.rglob("*.csv"))
    pattern = re.compile(r"^(?P<season>.+?)_sd_(?P<part>[1234])\.csv$", re.IGNORECASE)

    grouped: Dict[str, Dict[int, Path]] = {}
    folder_by_season: Dict[str, Path] = {}
    for p in all_csvs:
        m = pattern.match(p.name)
        if not m:
            continue
        season = m.group("season")
        part = int(m.group("part"))
        grouped.setdefault(season, {})[part] = p
        folder_by_season[season] = p.parent

    bundles: List[SeasonBundle] = []
    for season, parts in grouped.items():
        if not ({1, 2, 3} <= set(parts.keys())):
            continue
        folder = folder_by_season[season]
        price_file = _find_related_price_file(folder, season, all_csvs)
        listino_file = parts.get(4)
        bundles.append(
            SeasonBundle(
                code=season,
                folder=folder,
                totali=parts[1],
                colori=parts[2],
                taglie=parts[3],
                listino=listino_file,
                prezzi=price_file,
            )
        )

    current = []
    continuative = []
    for b in bundles:
        season_char = next((ch for ch in reversed(b.code.lower()) if ch.isalpha()), "")
        if season_char in ("i", "e"):
            current.append(b)
        elif season_char in ("y", "g"):
            continuative.append(b)

    current.sort(key=lambda x: _season_sort_key(x.code))
    continuative.sort(key=lambda x: _season_sort_key(x.code))

    return {
        "all": bundles,
        "current_latest": current[-1] if current else None,
        "continuative_latest": continuative[-1] if continuative else None,
        "continuative_last3": continuative[-3:] if len(continuative) >= 3 else continuative,
    }


def _bundle_module(code: str) -> Optional[str]:
    season_char = next((ch for ch in reversed(str(code).strip().lower()) if ch.isalpha()), "")
    if season_char in ("i", "e"):
        return "current"
    if season_char in ("y", "g"):
        return "continuativa"
    return None


def _bundle_token(code: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(code or "").strip().lower()).strip("_")
    return text or "unknown"


def _export_historical_source_bundles(
    bundles: List[SeasonBundle],
    *,
    output_orders: Path,
    logger: StepLogger,
) -> List[Dict[str, Any]]:
    history_dir = output_orders / "history_source"
    history_dir.mkdir(parents=True, exist_ok=True)

    jobs: List[Dict[str, Any]] = []
    for bundle in sorted(bundles, key=lambda item: _season_sort_key(item.code)):
        module = _bundle_module(bundle.code)
        if not module:
            continue
        df_input = _estrai_matematico(bundle.totali, bundle.colori, bundle.taglie)
        if bundle.listino and bundle.listino.exists():
            df_listino = _estrai_listino_fasce(bundle.listino)
            df_input = pd.merge(df_input, df_listino, on="Codice_Articolo", how="left")
        if bundle.prezzi and bundle.prezzi.exists():
            df_prezzi = _estrai_prezzi_acquisto(bundle.prezzi)
            df_input = pd.merge(df_input, df_prezzi, on="Codice_Articolo", how="left")
        df_input = _apply_price_band(df_input)
        file_name = f"orders_source_{_bundle_token(bundle.code)}.csv"
        out_path = history_dir / file_name
        _save_csv(df_input, out_path)
        jobs.append(
            {
                "season": bundle.code,
                "module": module,
                "file": f"history_source/{file_name}",
                "articles_input": int(len(df_input)),
                "listino_file": str(bundle.listino) if bundle.listino else "",
                "prezzi_file": str(bundle.prezzi) if bundle.prezzi else "",
            }
        )

    if jobs:
        logger.log(f"Storico sorgenti ordini: esportati {len(jobs)} bundle stagionali.")
    else:
        logger.log("Storico sorgenti ordini: nessun bundle esportato.")
    return jobs


def _normalizza_taglia(v: str) -> str:
    v = str(v).strip()
    v = v.replace("½", "5").replace("\xbd", "5").replace("�", "5")
    return re.sub(r"[^\d]", "", v)


def _taglia_sort_key(t: str):
    try:
        return int(t)
    except Exception:
        return float("inf")


def _estrai_rf(file_totali: Path, file_colori: Path, file_taglie: Path) -> pd.DataFrame:
    data_totali = []
    current_reparto = current_categoria = current_tipologia = current_marchio = None
    with open(file_totali, "r", encoding="utf-8", errors="replace") as f:
        for row in csv.reader(f):
            if "ARTICOLO" not in row:
                continue
            elements = row[row.index("ARTICOLO") + 1 :]
            if "TOTALI :" in elements:
                elements = elements[: elements.index("TOTALI :")]
            article_indices = [
                j
                for j, val in enumerate(elements)
                if "/" in (val.strip().split()[0] if val.strip().split() else "")
                and any(c.isdigit() for c in (val.strip().split()[0] if val.strip().split() else ""))
            ]
            last_idx = 0
            for idx in article_indices:
                pre_chunk = elements[last_idx:idx]
                pre_elements = [
                    e.strip()
                    for e in pre_chunk
                    if e.strip()
                    and not re.match(r"^-?\d+(?:,\d+)?$|^%$", e.strip())
                    and not e.startswith("SUBTOTALE")
                    and not e.startswith("VALORE")
                    and not e.startswith("COSTO")
                ]
                if len(pre_elements) >= 1:
                    current_marchio = pre_elements[-1]
                if len(pre_elements) >= 2:
                    current_tipologia = pre_elements[-2]
                if len(pre_elements) >= 3:
                    current_categoria = pre_elements[-3]
                if len(pre_elements) >= 4:
                    current_reparto = pre_elements[-4]
                codice = elements[idx].strip().split()[0]
                descrizione = re.sub(r"\s+", " ", elements[idx][len(codice) :].strip())
                vend, vend_periodo, giac = 0, 0, 0
                if idx + 4 < len(elements):
                    try:
                        vend = int(elements[idx + 2].strip().replace(".", ""))
                        vend_periodo = int(elements[idx + 3].strip().replace(".", ""))
                        giac = int(elements[idx + 4].strip().replace(".", ""))
                    except Exception:
                        pass
                data_totali.append(
                    {
                        "Codice_Articolo": codice,
                        "Categoria": re.sub(r"\s+", " ", current_categoria) if current_categoria else None,
                        "Tipologia": re.sub(r"\s+", " ", current_tipologia) if current_tipologia else None,
                        "Marchio": re.sub(r"\s+", " ", current_marchio) if current_marchio else None,
                        "Descrizione": descrizione,
                        "Venduto_Totale": vend,
                        "Venduto_Periodo": vend_periodo,
                        "Giacenza": giac,
                    }
                )
                last_idx = idx + 1
    df_totali = pd.DataFrame(data_totali).drop_duplicates(subset=["Codice_Articolo"])

    data_colori = []
    current_colore = current_materiale = None
    with open(file_colori, "r", encoding="utf-8", errors="replace") as f:
        for row in csv.reader(f):
            if "ARTICOLO" not in row:
                continue
            elements = row[row.index("ARTICOLO") + 1 :]
            if "TOTALI :" in elements:
                elements = elements[: elements.index("TOTALI :")]
            article_indices = [
                j
                for j, val in enumerate(elements)
                if "/" in (val.strip().split()[0] if val.strip().split() else "")
                and any(c.isdigit() for c in (val.strip().split()[0] if val.strip().split() else ""))
            ]
            last_idx = 0
            for idx in article_indices:
                pre_chunk = elements[last_idx:idx]
                pre_elements = [
                    e.strip()
                    for e in pre_chunk
                    if e.strip()
                    and not re.match(r"^-?\d+(?:,\d+)?$|^%$", e.strip())
                    and not e.startswith("SUBTOTALE")
                    and not e.startswith("VALORE")
                    and not e.startswith("COSTO")
                ]
                if len(pre_elements) >= 1:
                    current_materiale = pre_elements[-1]
                if len(pre_elements) >= 2:
                    current_colore = pre_elements[-2]
                codice = elements[idx].strip().split()[0]
                data_colori.append(
                    {
                        "Codice_Articolo": codice,
                        "Colore": re.sub(r"\s+", " ", current_colore) if current_colore else None,
                        "Materiale": re.sub(r"\s+", " ", current_materiale) if current_materiale else None,
                    }
                )
                last_idx = idx + 1
    df_colori = pd.DataFrame(data_colori).drop_duplicates(subset=["Codice_Articolo"])

    TAGLIE_TARGET = set()
    with open(file_taglie, "r", encoding="utf-8", errors="replace") as f:
        for row in csv.reader(f):
            if "TAG" not in row or "TOT" not in row:
                continue
            tag_idx = row.index("TAG")
            tot_idx = row.index("TOT")
            for v in row[tag_idx + 1 : tot_idx]:
                base = _normalizza_taglia(v)
                if base:
                    TAGLIE_TARGET.add(base)
            if TAGLIE_TARGET:
                break

    taglie_ordinate_rf = sorted(TAGLIE_TARGET, key=_taglia_sort_key)

    data_taglie = []
    with open(file_taglie, "r", encoding="utf-8", errors="replace") as f:
        for row in csv.reader(f):
            if "TAG" not in row or "VEN" not in row or "GIA" not in row or "TOT" not in row:
                continue
            tag_idx = row.index("TAG")
            tot_idx = row.index("TOT")
            ven_idx = row.index("VEN")
            gia_idx = row.index("GIA")
            etichette = [v.strip() for v in row[tag_idx + 1 : tot_idx] if v.strip()]
            n = len(etichette)
            valori = []
            for v in row[ven_idx + 1 : ven_idx + 1 + n]:
                try:
                    valori.append(int(float(v.replace(".", "").replace(",", "."))))
                except Exception:
                    valori.append(0)
            codice = None
            for v in row[ven_idx + 1 + n : gia_idx]:
                v_c = v.strip()
                if "/" in v_c and any(c.isdigit() for c in v_c):
                    codice = v_c.split()[0]
                    break
            if not codice:
                continue
            vendite = {t: 0 for t in TAGLIE_TARGET}
            venduto_extra = 0
            for etichetta, valore in zip(etichette, valori):
                base = _normalizza_taglia(etichetta)
                if base in TAGLIE_TARGET:
                    vendite[base] += valore
                else:
                    venduto_extra += valore
            entry = {"Codice_Articolo": codice}
            for t in taglie_ordinate_rf:
                entry[f"Venduto_{t}"] = vendite[t]
            entry["Venduto_Extra"] = venduto_extra
            data_taglie.append(entry)

    if data_taglie:
        df_taglie = pd.DataFrame(data_taglie).drop_duplicates(subset=["Codice_Articolo"])
    else:
        df_taglie = pd.DataFrame(columns=["Codice_Articolo", "Venduto_Extra"])

    df_fin = pd.merge(df_totali, df_colori, on="Codice_Articolo", how="left")
    df_fin = pd.merge(df_fin, df_taglie, on="Codice_Articolo", how="left").fillna(0)
    colonne_taglie = [f"Venduto_{t}" for t in taglie_ordinate_rf]
    colonne_ordinate = [
        "Codice_Articolo",
        "Categoria",
        "Tipologia",
        "Marchio",
        "Colore",
        "Materiale",
        "Descrizione",
        "Venduto_Totale",
        "Venduto_Periodo",
        "Giacenza",
    ] + colonne_taglie + ["Venduto_Extra"]
    df_fin = df_fin.reindex(columns=colonne_ordinate, fill_value=0)
    for col in colonne_ordinate[7:]:
        df_fin[col] = pd.to_numeric(df_fin[col], errors="coerce").fillna(0).astype(int)
    return df_fin


def _estrai_matematico(file_totali: Path, file_colori: Path, file_taglie: Path) -> pd.DataFrame:
    data_totali = []
    current_reparto = current_categoria = current_tipologia = current_marchio = None
    with open(file_totali, "r", encoding="utf-8", errors="replace") as f:
        for row in csv.reader(f):
            if "ARTICOLO" not in row:
                continue
            elements = row[row.index("ARTICOLO") + 1 :]
            if "TOTALI :" in elements:
                elements = elements[: elements.index("TOTALI :")]
            article_indices = [
                j
                for j, val in enumerate(elements)
                if "/" in (val.strip().split()[0] if val.strip().split() else "")
                and any(c.isdigit() for c in (val.strip().split()[0] if val.strip().split() else ""))
            ]
            last_idx = 0
            for idx in article_indices:
                pre_chunk = elements[last_idx:idx]
                pre_elements = [
                    e.strip()
                    for e in pre_chunk
                    if e.strip()
                    and not re.match(r"^-?\d+(?:,\d+)?$|^%$", e.strip())
                    and not e.startswith("SUBTOTALE")
                    and not e.startswith("VALORE")
                    and not e.startswith("COSTO")
                ]
                if len(pre_elements) >= 1:
                    current_marchio = pre_elements[-1]
                if len(pre_elements) >= 2:
                    current_tipologia = pre_elements[-2]
                if len(pre_elements) >= 3:
                    current_categoria = pre_elements[-3]
                if len(pre_elements) >= 4:
                    current_reparto = pre_elements[-4]
                codice = elements[idx].strip().split()[0]
                descrizione = re.sub(r"\s+", " ", elements[idx][len(codice) :].strip())
                vend, vend_periodo, giac = 0, 0, 0
                if idx + 4 < len(elements):
                    try:
                        vend = int(elements[idx + 2].strip().replace(".", ""))
                        vend_periodo = int(elements[idx + 3].strip().replace(".", ""))
                        giac = int(elements[idx + 4].strip().replace(".", ""))
                    except Exception:
                        pass
                data_totali.append(
                    {
                        "Codice_Articolo": codice,
                        "Categoria": re.sub(r"\s+", " ", current_categoria) if current_categoria else None,
                        "Tipologia": re.sub(r"\s+", " ", current_tipologia) if current_tipologia else None,
                        "Marchio": re.sub(r"\s+", " ", current_marchio) if current_marchio else None,
                        "Descrizione": descrizione,
                        "Venduto_Totale": vend,
                        "Venduto_Periodo": vend_periodo,
                        "Giacenza": giac,
                    }
                )
                last_idx = idx + 1
    df_totali = pd.DataFrame(data_totali).drop_duplicates(subset=["Codice_Articolo"])

    data_colori = []
    current_colore = current_materiale = None
    with open(file_colori, "r", encoding="utf-8", errors="replace") as f:
        for row in csv.reader(f):
            if "ARTICOLO" not in row:
                continue
            elements = row[row.index("ARTICOLO") + 1 :]
            if "TOTALI :" in elements:
                elements = elements[: elements.index("TOTALI :")]
            article_indices = [
                j
                for j, val in enumerate(elements)
                if "/" in (val.strip().split()[0] if val.strip().split() else "")
                and any(c.isdigit() for c in (val.strip().split()[0] if val.strip().split() else ""))
            ]
            last_idx = 0
            for idx in article_indices:
                pre_chunk = elements[last_idx:idx]
                pre_elements = [
                    e.strip()
                    for e in pre_chunk
                    if e.strip()
                    and not re.match(r"^-?\d+(?:,\d+)?$|^%$", e.strip())
                    and not e.startswith("SUBTOTALE")
                    and not e.startswith("VALORE")
                    and not e.startswith("COSTO")
                ]
                if len(pre_elements) >= 1:
                    current_materiale = pre_elements[-1]
                if len(pre_elements) >= 2:
                    current_colore = pre_elements[-2]
                codice = elements[idx].strip().split()[0]
                data_colori.append(
                    {
                        "Codice_Articolo": codice,
                        "Colore": re.sub(r"\s+", " ", current_colore) if current_colore else None,
                        "Materiale": re.sub(r"\s+", " ", current_materiale) if current_materiale else None,
                    }
                )
                last_idx = idx + 1
    df_colori = pd.DataFrame(data_colori).drop_duplicates(subset=["Codice_Articolo"])

    TAGLIE_TARGET = set()
    with open(file_taglie, "r", encoding="utf-8", errors="replace") as f:
        for row in csv.reader(f):
            if "TAG" not in row or "TOT" not in row:
                continue
            tag_idx = row.index("TAG")
            tot_idx = row.index("TOT")
            for v in row[tag_idx + 1 : tot_idx]:
                base = _normalizza_taglia(v)
                if base:
                    TAGLIE_TARGET.add(base)
    taglie_ordinate = sorted(TAGLIE_TARGET, key=_taglia_sort_key)

    data_taglie = []
    with open(file_taglie, "r", encoding="utf-8", errors="replace") as f:
        for row in csv.reader(f):
            if "TAG" not in row or "VEN" not in row or "GIA" not in row or "TOT" not in row:
                continue
            tag_idx = row.index("TAG")
            tot_idx = row.index("TOT")
            ven_idx = row.index("VEN")
            gia_idx = row.index("GIA")
            etichette = [v.strip() for v in row[tag_idx + 1 : tot_idx] if v.strip()]
            n = len(etichette)
            valori = []
            for v in row[ven_idx + 1 : ven_idx + 1 + n]:
                try:
                    valori.append(int(float(v.replace(".", "").replace(",", "."))))
                except Exception:
                    valori.append(0)
            codice = None
            for v in row[ven_idx + 1 + n : gia_idx]:
                v_c = v.strip()
                if "/" in v_c and any(c.isdigit() for c in v_c):
                    codice = v_c.split()[0]
                    break
            if not codice:
                continue
            vendite = {t: 0 for t in TAGLIE_TARGET}
            venduto_extra = 0
            for etichetta, valore in zip(etichette, valori):
                base = _normalizza_taglia(etichetta)
                if base in TAGLIE_TARGET:
                    vendite[base] += valore
                else:
                    venduto_extra += valore
            entry = {"Codice_Articolo": codice}
            for t in taglie_ordinate:
                entry[f"Venduto_{t}"] = vendite[t]
            entry["Venduto_Extra"] = venduto_extra
            data_taglie.append(entry)

    if data_taglie:
        df_taglie = pd.DataFrame(data_taglie).drop_duplicates(subset=["Codice_Articolo"])
    else:
        df_taglie = pd.DataFrame(columns=["Codice_Articolo", "Venduto_Extra"])

    df_fin = pd.merge(df_totali, df_colori, on="Codice_Articolo", how="left")
    df_fin = pd.merge(df_fin, df_taglie, on="Codice_Articolo", how="left").fillna(0)
    colonne_taglie = [f"Venduto_{t}" for t in taglie_ordinate]
    colonne_ordinate = [
        "Codice_Articolo",
        "Categoria",
        "Tipologia",
        "Marchio",
        "Colore",
        "Materiale",
        "Descrizione",
        "Venduto_Totale",
        "Venduto_Periodo",
        "Giacenza",
    ] + colonne_taglie + ["Venduto_Extra"]
    df_fin = df_fin.reindex(columns=colonne_ordinate, fill_value=0)
    for col in colonne_ordinate[7:]:
        df_fin[col] = pd.to_numeric(df_fin[col], errors="coerce").fillna(0).astype(int)
    return df_fin


def _estrai_prezzi_acquisto(file_prezzi: Path) -> pd.DataFrame:
    data = []
    with open(file_prezzi, "r", encoding="utf-8", errors="replace") as f:
        for row in csv.reader(f):
            art_idx = None
            for j, val in enumerate(row):
                if val.strip() == "ARTICOLO":
                    art_idx = j
                    break
            if art_idx is None:
                continue
            elements = row[art_idx + 1 :]
            if "TOTALI :" in elements:
                elements = elements[: elements.index("TOTALI :")]
            for j, val in enumerate(elements):
                v = val.strip().split()[0] if val.strip().split() else ""
                if "/" in v and any(c.isdigit() for c in v) and not re.match(r"^\d{2}/\d{2}/\d{4}$", v):
                    codice = v
                    prezzo_acquisto = None
                    prezzo_vendita = None
                    if j + 1 < len(elements):
                        try:
                            prezzo_acquisto = float(elements[j + 1].strip().replace(".", "").replace(",", "."))
                        except Exception:
                            prezzo_acquisto = None
                    if j + 2 < len(elements):
                        try:
                            prezzo_vendita = float(elements[j + 2].strip().replace(".", "").replace(",", "."))
                        except Exception:
                            prezzo_vendita = None
                    data.append(
                        {
                            "Codice_Articolo": codice,
                            "Prezzo_Acquisto": prezzo_acquisto,
                            "Prezzo_Vendita": prezzo_vendita,
                        }
                    )
                    break
    return pd.DataFrame(data).drop_duplicates(subset=["Codice_Articolo"])


def _estrai_listino_fasce(file_listino: Path) -> pd.DataFrame:
    data = []
    current_fascia = None
    with open(file_listino, "r", encoding="utf-8", errors="replace") as f:
        for row in csv.reader(f):
            art_idx = None
            for j, val in enumerate(row):
                if val.strip() == "ARTICOLO":
                    art_idx = j
                    break
            if art_idx is None:
                continue
            elements = row[art_idx + 1 :]
            if "TOTALI :" in elements:
                elements = elements[: elements.index("TOTALI :")]

            article_indices = [
                j
                for j, val in enumerate(elements)
                if "/" in (val.strip().split()[0] if val.strip().split() else "")
                and any(c.isdigit() for c in (val.strip().split()[0] if val.strip().split() else ""))
                and not re.match(r"^\d{2}/\d{2}/\d{4}$", val.strip().split()[0])
            ]
            last_idx = 0
            for idx in article_indices:
                pre_chunk = elements[last_idx:idx]
                pre_elements = [
                    e.strip()
                    for e in pre_chunk
                    if e.strip()
                    and not re.match(r"^-?\d+(?:,\d+)?$|^%$", e.strip())
                    and not e.startswith("SUBTOTALE")
                    and not e.startswith("VALORE")
                    and not e.startswith("COSTO")
                ]
                if pre_elements:
                    current_fascia = pre_elements[-1]

                codice = elements[idx].strip().split()[0]
                prezzo_listino = None
                if idx + 1 < len(elements):
                    try:
                        prezzo_listino = float(elements[idx + 1].strip().replace(".", "").replace(",", "."))
                    except Exception:
                        prezzo_listino = None

                data.append(
                    {
                        "Codice_Articolo": codice,
                        "Fascia_Prezzo": re.sub(r"\s+", " ", current_fascia).strip() if current_fascia else None,
                        "Prezzo_Listino": prezzo_listino,
                    }
                )
                last_idx = idx + 1

    return pd.DataFrame(data).drop_duplicates(subset=["Codice_Articolo"])


def _is_valid_price_band(value: Any) -> bool:
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    text = str(value).strip()
    return bool(re.match(r"^\d+\s*-\s*\d+$", text))


def _price_band_label(value: Any) -> Optional[str]:
    try:
        price = float(value)
    except Exception:
        return None
    if not np.isfinite(price) or price <= 0:
        return None
    whole = int(price)
    if whole < 20:
        return "0-19"
    upper = (whole // 10) * 10 + 9
    lower = upper - 9
    return f"{lower}-{upper}"


def _apply_price_band(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "Fascia_Prezzo" not in out.columns:
        out["Fascia_Prezzo"] = None

    base_price = pd.Series(np.nan, index=out.index, dtype="float64")
    for col in ("Prezzo_Listino", "Prezzo_Vendita", "Prezzo_Acquisto"):
        if col not in out.columns:
            continue
        values = pd.to_numeric(out[col], errors="coerce")
        base_price = base_price.where(base_price.notna(), values)

    derived = base_price.map(_price_band_label)
    current_band = out["Fascia_Prezzo"].map(lambda v: "" if pd.isna(v) else str(v).strip())
    needs_fill = current_band.eq("") | ~current_band.map(_is_valid_price_band)
    out.loc[needs_fill, "Fascia_Prezzo"] = derived.loc[needs_fill]
    return out


def _is_mezza_taglia(taglia: str) -> bool:
    return len(taglia) == 3 and taglia.endswith("5") and taglia[:2].isdigit()


def _rebalance_nonnegative_allocations(
    df: pd.DataFrame,
    *,
    total_col: str,
    allocation_cols: List[str],
    preferred_sizes: Optional[pd.Series] = None,
) -> None:
    if df.empty or total_col not in df.columns or not allocation_cols:
        return

    preferred_sizes = preferred_sizes if isinstance(preferred_sizes, pd.Series) else pd.Series("", index=df.index)

    for idx_row in df.index:
        try:
            target_total = max(0, int(round(float(df.loc[idx_row, total_col]))))
        except Exception:
            target_total = 0

        values: Dict[str, int] = {}
        for col in allocation_cols:
            try:
                values[col] = max(0, int(round(float(df.loc[idx_row, col]))))
            except Exception:
                values[col] = 0

        current_sum = sum(values.values())
        preferred_size = str(preferred_sizes.get(idx_row, "") or "").strip()
        preferred_col = f"Acquistare_{preferred_size}" if preferred_size else ""
        if preferred_col not in values:
            preferred_col = next((col for col, val in values.items() if val > 0), allocation_cols[0])

        if current_sum > target_total:
            excess = current_sum - target_total
            reduction_order = sorted(
                allocation_cols,
                key=lambda col: (values.get(col, 0), 0 if col != preferred_col else -1),
                reverse=True,
            )
            for col in reduction_order:
                if excess <= 0:
                    break
                dec = min(values.get(col, 0), excess)
                values[col] = values.get(col, 0) - dec
                excess -= dec
        elif current_sum < target_total:
            values[preferred_col] = values.get(preferred_col, 0) + (target_total - current_sum)

        for col in allocation_cols:
            df.loc[idx_row, col] = max(0, int(values.get(col, 0)))


def _run_math_forecast(df_input: pd.DataFrame, fattore_copertura: float, is_continuativa: bool) -> Dict[str, Any]:
    if not is_continuativa:
        df_scope = df_input[df_input["Giacenza"] >= 15].copy().sort_values("Giacenza", ascending=False).reset_index(drop=True)
    else:
        df_scope = df_input.copy()

    if df_scope.empty:
        return {
            "scope": df_scope,
            "forecast": pd.DataFrame(),
            "totale": 0,
        }

    taglie = sorted(
        [c.replace("Venduto_", "") for c in df_scope.columns if c.startswith("Venduto_") and c.replace("Venduto_", "").isdigit()],
        key=int,
    )
    colonne_venduto_taglie = [f"Venduto_{t}" for t in taglie]

    df_m = df_scope.copy()
    df_m["Venduto_Taglie_Totale"] = df_m[colonne_venduto_taglie].sum(axis=1)
    for t in taglie:
        df_m[f"Score_{t}"] = np.where(
            df_m["Venduto_Taglie_Totale"] > 0,
            (df_m[f"Venduto_{t}"] / df_m["Venduto_Taglie_Totale"]) * 100,
            0,
        )

    if is_continuativa:
        df_m["Predizione_Vendite"] = df_m["Venduto_Periodo"].fillna(0)
    else:
        df_m["Predizione_Vendite"] = df_m["Venduto_Totale"].fillna(0)
    df_m["Stock_Target"] = df_m["Predizione_Vendite"] * fattore_copertura
    df_m["Da_Acquistare_Totale"] = (df_m["Stock_Target"] - df_m["Giacenza"].fillna(0)).apply(
        lambda x: max(0, int(round(x)))
    )

    colonne_acquistare = []
    for t in taglie:
        col_acq = f"Acquistare_{t}"
        df_m[col_acq] = (df_m["Da_Acquistare_Totale"] * (df_m[f"Score_{t}"] / 100)).round().fillna(0).astype(int)
        colonne_acquistare.append(col_acq)

    df_m["Somma_Taglie_Assegnate"] = df_m[colonne_acquistare].sum(axis=1)
    df_m["Differenza_Arrotondamento"] = df_m["Da_Acquistare_Totale"] - df_m["Somma_Taglie_Assegnate"]
    taglia_migliore = df_m[[f"Score_{t}" for t in taglie]].idxmax(axis=1).str.replace("Score_", "")
    for idx_row in df_m.index:
        diff = int(df_m.loc[idx_row, "Differenza_Arrotondamento"])
        if diff != 0:
            col_acq_m = f"Acquistare_{taglia_migliore[idx_row]}"
            df_m.loc[idx_row, col_acq_m] = max(0, int(df_m.loc[idx_row, col_acq_m]) + diff)

    _rebalance_nonnegative_allocations(
        df_m,
        total_col="Da_Acquistare_Totale",
        allocation_cols=colonne_acquistare,
        preferred_sizes=taglia_migliore,
    )

    colonne_finali = (
        [
            "Codice_Articolo",
            "Categoria",
            "Tipologia",
            "Marchio",
            "Colore",
            "Materiale",
            "Descrizione",
            "Venduto_Totale",
            "Venduto_Periodo",
            "Giacenza",
            "Predizione_Vendite",
            "Da_Acquistare_Totale",
            "Fascia_Prezzo",
            "Prezzo_Listino",
            "Prezzo_Acquisto",
            "Prezzo_Vendita",
        ]
        + colonne_acquistare
    )
    colonne_finali = [c for c in colonne_finali if c in df_m.columns]
    df_finale = df_m[colonne_finali].copy()
    df_finale["Predizione_Vendite"] = df_finale["Predizione_Vendite"].round(1)

    cols_mat_mezze = [
        c for c in df_finale.columns if c.startswith("Acquistare_") and _is_mezza_taglia(c.replace("Acquistare_", ""))
    ]
    if cols_mat_mezze:
        df_finale = df_finale.drop(columns=cols_mat_mezze)
        acq_intere = [
            c
            for c in df_finale.columns
            if c.startswith("Acquistare_") and c.replace("Acquistare_", "").isdigit()
        ]
        df_finale["Da_Acquistare_Totale"] = df_finale[acq_intere].sum(axis=1)

    if "Prezzo_Acquisto" in df_finale.columns:
        df_finale["Budget_Acquisto"] = (df_finale["Da_Acquistare_Totale"] * df_finale["Prezzo_Acquisto"]).round(2)

    return {
        "scope": df_scope,
        "forecast": df_finale,
        "totale": int(df_finale["Da_Acquistare_Totale"].sum()) if not df_finale.empty else 0,
    }


def _run_rf_and_hybrid(
    df_s1: pd.DataFrame,
    df_s2: pd.DataFrame,
    df_s3: pd.DataFrame,
    anni: Tuple[int, int, int],
    fattore_copertura: float,
    df_math_latest: pd.DataFrame,
) -> Dict[str, Any]:
    if RandomForestRegressor is None:
        return {"enabled": False, "reason": "scikit-learn non installato"}

    colonne_target_rf = sorted(
        [c for c in df_s3.columns if c.startswith("Venduto_") and c.replace("Venduto_", "").isdigit()],
        key=lambda c: int(c.replace("Venduto_", "")),
    )
    if not colonne_target_rf:
        return {"enabled": False, "reason": "Nessuna colonna taglia valida per RF"}

    colonne_caratteristiche = ["Categoria", "Tipologia", "Marchio", "Colore", "Materiale"]
    anno_1, anno_2, anno_3 = anni
    df_s1 = df_s1.copy()
    df_s2 = df_s2.copy()
    df_s3 = df_s3.copy()
    df_s1["anno"] = anno_1
    df_s2["anno"] = anno_2
    df_s3["anno"] = anno_3

    for df_tmp in (df_s1, df_s2, df_s3):
        ratio = df_tmp["Venduto_Periodo"] / df_tmp["Venduto_Totale"].replace(0, np.nan)
        ratio = ratio.fillna(0).clip(0, 1)
        for col in colonne_target_rf:
            df_tmp[col] = (df_tmp[col] * ratio).round().astype(int)

    storico_completo = pd.concat([df_s1, df_s2, df_s3], ignore_index=True)
    peso_map = {anno_1: 1, anno_2: 2, anno_3: 3}

    def _media_pesata(group):
        pesi = group["anno"].map(peso_map)
        res = {}
        for col in colonne_target_rf:
            res[f"Media_{col}"] = np.average(group[col], weights=pesi)
        res["Num_Stagioni"] = group["anno"].nunique()
        vend_s3 = group.loc[group["anno"] == anno_3, "Venduto_Periodo"]
        vend_pre = group.loc[group["anno"] < anno_3, "Venduto_Periodo"]
        if len(vend_s3) == 0:
            res["Flag_Crescita_Esplosiva"] = 0
        elif len(vend_pre) == 0 or vend_pre.sum() == 0:
            res["Flag_Crescita_Esplosiva"] = 1
        else:
            res["Flag_Crescita_Esplosiva"] = 1 if vend_s3.values[0] > vend_pre.mean() * 3 else 0
        return pd.Series(res)

    storico_medio = (
        storico_completo.groupby("Codice_Articolo")
        .apply(_media_pesata, include_groups=False)
        .reset_index()
    )
    dataset_totale = (
        pd.concat([df_s1, df_s2, df_s2, df_s3, df_s3, df_s3], ignore_index=True).merge(storico_medio, on="Codice_Articolo")
    )

    X_cat = pd.get_dummies(dataset_totale[colonne_caratteristiche])
    X_num = dataset_totale[[f"Media_{c}" for c in colonne_target_rf] + ["Num_Stagioni", "Flag_Crescita_Esplosiva"]]
    X = pd.concat([X_cat, X_num], axis=1).fillna(0)
    Y = dataset_totale[colonne_target_rf].fillna(0)

    model = RandomForestRegressor(n_estimators=200, random_state=42)
    model.fit(X, Y)

    X_futuro_cat = pd.get_dummies(df_s3[colonne_caratteristiche]).reindex(columns=X_cat.columns, fill_value=False)
    df_s3_storico = df_s3.merge(storico_medio, on="Codice_Articolo", how="left")
    for c in colonne_target_rf:
        df_s3_storico[f"Media_{c}"] = df_s3_storico[f"Media_{c}"].fillna(0)
    df_s3_storico["Num_Stagioni"] = df_s3_storico["Num_Stagioni"].fillna(0)
    df_s3_storico["Flag_Crescita_Esplosiva"] = df_s3_storico["Flag_Crescita_Esplosiva"].fillna(0)
    X_futuro_num = df_s3_storico[[f"Media_{c}" for c in colonne_target_rf] + ["Num_Stagioni", "Flag_Crescita_Esplosiva"]]
    X_futuro = pd.concat([X_futuro_cat.reset_index(drop=True), X_futuro_num.reset_index(drop=True)], axis=1)

    previsioni_rf = model.predict(X_futuro)
    nomi_previsti = [f"Previsto_{c.split('_')[1]}" for c in colonne_target_rf]
    df_prev_rf = pd.DataFrame(previsioni_rf, columns=nomi_previsti).round().astype(int)

    risultato_rf = df_s3[
        ["Codice_Articolo", "Categoria", "Tipologia", "Colore", "Descrizione", "Giacenza"]
    ].copy()
    risultato_rf = pd.concat([risultato_rf.reset_index(drop=True), df_prev_rf], axis=1)
    risultato_rf["Vendita_Totale_Prevista"] = df_prev_rf.sum(axis=1)
    risultato_rf["Stock_Target_RF"] = (risultato_rf["Vendita_Totale_Prevista"] * fattore_copertura).round().astype(int)
    risultato_rf["Da_Acquistare_Totale"] = np.maximum(0, risultato_rf["Stock_Target_RF"] - risultato_rf["Giacenza"])

    for c in nomi_previsti:
        taglia = c.replace("Previsto_", "")
        quota = risultato_rf[c] / risultato_rf["Vendita_Totale_Prevista"].replace(0, np.nan)
        risultato_rf[f"Acquistare_{taglia}"] = (quota * risultato_rf["Da_Acquistare_Totale"]).fillna(0).round().astype(int)

    acq_cols_rf = [f"Acquistare_{c.replace('Previsto_', '')}" for c in nomi_previsti]
    risultato_rf["_somma_acq"] = risultato_rf[acq_cols_rf].sum(axis=1)
    risultato_rf["_diff_acq"] = risultato_rf["Da_Acquistare_Totale"] - risultato_rf["_somma_acq"]
    taglia_top_rf = df_prev_rf[nomi_previsti].idxmax(axis=1).str.replace("Previsto_", "")
    for idx_row in risultato_rf.index:
        diff = int(risultato_rf.loc[idx_row, "_diff_acq"])
        if diff != 0:
            col_fix = f"Acquistare_{taglia_top_rf[idx_row]}"
            risultato_rf.loc[idx_row, col_fix] = max(0, int(risultato_rf.loc[idx_row, col_fix]) + diff)
    risultato_rf = risultato_rf.drop(columns=["_somma_acq", "_diff_acq"])

    _rebalance_nonnegative_allocations(
        risultato_rf,
        total_col="Da_Acquistare_Totale",
        allocation_cols=acq_cols_rf,
        preferred_sizes=taglia_top_rf,
    )

    enrich_cols_rf = [
        c
        for c in ("Fascia_Prezzo", "Prezzo_Listino", "Prezzo_Acquisto", "Prezzo_Vendita")
        if c in df_math_latest.columns
    ]
    if enrich_cols_rf:
        risultato_rf = risultato_rf.merge(
            df_math_latest[["Codice_Articolo"] + enrich_cols_rf].drop_duplicates(),
            on="Codice_Articolo",
            how="left",
        )
    if "Prezzo_Acquisto" in risultato_rf.columns:
        risultato_rf["Budget_Acquisto"] = (risultato_rf["Da_Acquistare_Totale"] * risultato_rf["Prezzo_Acquisto"]).round(2)

    df_mat_for_ib = df_math_latest.drop(columns=[c for c in ("Prezzo_Acquisto", "Budget_Acquisto") if c in df_math_latest.columns], errors="ignore")
    df_rf_for_ib = risultato_rf.drop(columns=[c for c in ("Prezzo_Acquisto", "Budget_Acquisto") if c in risultato_rf.columns], errors="ignore")
    df_ib = pd.merge(df_mat_for_ib, df_rf_for_ib, on="Codice_Articolo", suffixes=("_Math", "_RF"))

    col_mat_tot = next((c for c in ("Da_Acquistare_Totale_Math", "Da_Acquistare_Totale_x") if c in df_ib.columns), "Da_Acquistare_Totale")
    col_rf_tot = next((c for c in ("Da_Acquistare_Totale_RF", "Da_Acquistare_Totale_y") if c in df_ib.columns), "Da_Acquistare_Totale")

    taglie_math = sorted(
        [c.replace("Acquistare_", "") for c in df_mat_for_ib.columns if c.startswith("Acquistare_") and c.replace("Acquistare_", "").isdigit()],
        key=int,
    )
    taglie_rf = sorted(
        [c.replace("Previsto_", "") for c in risultato_rf.columns if c.startswith("Previsto_") and c.replace("Previsto_", "").isdigit()],
        key=int,
    )
    taglie_ib = sorted(set(taglie_math) | set(taglie_rf), key=int)

    tot_rf_prev = (
        df_ib["Vendita_Totale_Prevista"]
        if "Vendita_Totale_Prevista" in df_ib.columns
        else df_ib[[f"Previsto_{t}" for t in taglie_rf if f"Previsto_{t}" in df_ib.columns]].sum(axis=1)
    )

    for t in taglie_ib:
        col_prev = f"Previsto_{t}"
        col_rf_t = f"_Acquistare_RF_{t}"
        if col_prev in df_ib.columns:
            quota = df_ib[col_prev] / tot_rf_prev.replace(0, np.nan)
            df_ib[col_rf_t] = (quota * df_ib[col_rf_tot]).fillna(0)
        else:
            df_ib[col_rf_t] = 0.0

    col_ibrido = []
    for t in taglie_ib:
        col_math_plain = f"Acquistare_{t}"
        col_math_suffix = f"Acquistare_{t}_Math"
        col_math = col_math_suffix if col_math_suffix in df_ib.columns else (col_math_plain if col_math_plain in df_ib.columns else None)
        col_rf_t = f"_Acquistare_RF_{t}"
        col_ib_t = f"Ibrido_{t}"
        val_math = df_ib[col_math].fillna(0) if col_math else 0
        val_rf = df_ib[col_rf_t].fillna(0)
        df_ib[col_ib_t] = ((val_math + val_rf) / 2).round().astype(int)
        col_ibrido.append(col_ib_t)

    df_ib["Ibrido_Totale_Grezzo"] = ((df_ib[col_mat_tot] + df_ib[col_rf_tot]) / 2).round().astype(int)
    df_ib["_Somma_Taglie"] = df_ib[col_ibrido].sum(axis=1)
    df_ib["_Differenza"] = df_ib["Ibrido_Totale_Grezzo"] - df_ib["_Somma_Taglie"]
    prev_cols_disp = [f"Previsto_{t}" for t in taglie_ib if f"Previsto_{t}" in df_ib.columns]
    taglia_top = df_ib[prev_cols_disp].idxmax(axis=1).str.replace("Previsto_", "")
    for idx_row in df_ib.index:
        diff = int(df_ib.loc[idx_row, "_Differenza"])
        if diff != 0:
            col_fix = f"Ibrido_{taglia_top[idx_row]}"
            df_ib.loc[idx_row, col_fix] = max(0, int(df_ib.loc[idx_row, col_fix]) + diff)
    df_ib["Ibrido_Totale"] = df_ib[col_ibrido].sum(axis=1).astype(int)

    _rebalance_nonnegative_allocations(
        df_ib,
        total_col="Ibrido_Totale_Grezzo",
        allocation_cols=col_ibrido,
        preferred_sizes=taglia_top,
    )
    df_ib["Ibrido_Totale"] = df_ib[col_ibrido].sum(axis=1).astype(int)

    desc_col = next((c for c in ("Descrizione_Math", "Descrizione_x", "Descrizione") if c in df_ib.columns), None)
    cat_col = next((c for c in ("Categoria_Math", "Categoria_x", "Categoria") if c in df_ib.columns), None)
    tip_col = next((c for c in ("Tipologia_Math", "Tipologia_x", "Tipologia") if c in df_ib.columns), None)
    extra_cols = [c for c in (cat_col, tip_col) if c is not None]
    extra_names = []
    if cat_col:
        extra_names.append("Categoria")
    if tip_col:
        extra_names.append("Tipologia")
    colonne_out = ["Codice_Articolo"] + extra_cols + ([desc_col] if desc_col else []) + [col_mat_tot, col_rf_tot, "Ibrido_Totale"] + col_ibrido
    df_ibrido = df_ib[colonne_out].copy()
    new_names = ["Codice_Articolo"] + extra_names + (["Descrizione"] if desc_col else []) + ["Math_Totale", "RF_Totale", "Ibrido_Totale"] + [f"Ibrido_{t}" for t in taglie_ib]
    df_ibrido.columns = new_names

    enrich_cols_ib = [
        c
        for c in ("Fascia_Prezzo", "Prezzo_Listino", "Prezzo_Acquisto", "Prezzo_Vendita")
        if c in df_math_latest.columns
    ]
    if enrich_cols_ib:
        df_ibrido = df_ibrido.merge(
            df_math_latest[["Codice_Articolo"] + enrich_cols_ib].drop_duplicates(),
            on="Codice_Articolo",
            how="left",
        )
    if "Prezzo_Acquisto" in df_ibrido.columns:
        df_ibrido["Budget_Acquisto"] = (df_ibrido["Ibrido_Totale"] * df_ibrido["Prezzo_Acquisto"]).round(2)

    return {
        "enabled": True,
        "risultato_rf": risultato_rf,
        "df_ibrido": df_ibrido,
        "totale_rf": int(risultato_rf["Da_Acquistare_Totale"].sum()) if not risultato_rf.empty else 0,
        "totale_ibrido": int(df_ibrido["Ibrido_Totale"].sum()) if not df_ibrido.empty else 0,
    }


def _bundle_year(bundle: SeasonBundle) -> int:
    y = _season_sort_key(bundle.code)[0]
    return y if y > 0 else dt.datetime.now().year


def _save_csv(df: pd.DataFrame, path: Path):
    if df is None:
        return
    df.to_csv(path, index=False)


def run_orders_pipeline(
    orders_root: Path,
    output_dir: Path,
    fattore_copertura: float = 1.20,
    enable_full: bool = True,
    verbose: bool = True,
) -> Dict[str, Any]:
    logger = StepLogger(verbose=verbose)
    output_orders = output_dir / "orders"
    output_orders.mkdir(parents=True, exist_ok=True)

    logger.log(f"Modulo ordini: scansione file in {orders_root}")
    discovered = discover_order_bundles(orders_root)
    bundles_all = discovered["all"]
    if not bundles_all:
        logger.log("Modulo ordini: nessun bundle valido trovato (richiesti *_sd_1/2/3.csv).")
        logger.write(output_orders / "orders_run_log.txt")
        summary = {
            "enabled": False,
            "reason": "no_bundles",
            "orders_root": str(orders_root),
            "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
        }
        (output_orders / "orders_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        return summary

    summary: Dict[str, Any] = {
        "enabled": True,
        "orders_root": str(orders_root),
        "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
        "bundles_detected": [b.code for b in bundles_all],
        "historical_sources": [],
        "current": {},
        "continuativa": {},
    }

    summary["historical_sources"] = _export_historical_source_bundles(
        bundles_all,
        output_orders=output_orders,
        logger=logger,
    )

    current_bundle: Optional[SeasonBundle] = discovered["current_latest"]
    if current_bundle:
        logger.log(f"Corrente: uso stagione {current_bundle.code}")
        df_input = _estrai_matematico(current_bundle.totali, current_bundle.colori, current_bundle.taglie)
        if current_bundle.listino and current_bundle.listino.exists():
            logger.log(f"Corrente: merge listino/fasce da {current_bundle.listino.name}")
            df_listino = _estrai_listino_fasce(current_bundle.listino)
            df_input = pd.merge(df_input, df_listino, on="Codice_Articolo", how="left")
        if current_bundle.prezzi and current_bundle.prezzi.exists():
            logger.log(f"Corrente: merge prezzi acquisto da {current_bundle.prezzi.name}")
            df_prezzi = _estrai_prezzi_acquisto(current_bundle.prezzi)
            df_input = pd.merge(df_input, df_prezzi, on="Codice_Articolo", how="left")
        df_input = _apply_price_band(df_input)

        math_res = _run_math_forecast(df_input, fattore_copertura=fattore_copertura, is_continuativa=False)
        _save_csv(df_input, output_orders / "orders_current_dati_originali.csv")
        _save_csv(math_res["scope"], output_orders / "orders_current_futuri_continuativi.csv")
        _save_csv(math_res["forecast"], output_orders / "orders_current_previsione_math.csv")
        logger.log(
            f"Corrente: articoli input={len(df_input)}, futuri_continuativi={len(math_res['scope'])}, totale_acquisto={math_res['totale']}"
        )
        summary["current"] = {
            "season": current_bundle.code,
            "articles_input": int(len(df_input)),
            "futuri_continuativi": int(len(math_res["scope"])),
            "totale_math": int(math_res["totale"]),
            "listino_file": str(current_bundle.listino) if current_bundle.listino else "",
            "prezzi_file": str(current_bundle.prezzi) if current_bundle.prezzi else "",
            "output_files": [
                "orders_current_dati_originali.csv",
                "orders_current_futuri_continuativi.csv",
                "orders_current_previsione_math.csv",
            ],
        }
    else:
        logger.log("Corrente: nessuna stagione i/e trovata, step saltato.")
        summary["current"] = {"enabled": False, "reason": "no_current_bundle"}

    cont_bundle: Optional[SeasonBundle] = discovered["continuative_latest"]
    if cont_bundle:
        logger.log(f"Continuativa: uso stagione latest {cont_bundle.code} (math)")
        df_input = _estrai_matematico(cont_bundle.totali, cont_bundle.colori, cont_bundle.taglie)
        if cont_bundle.listino and cont_bundle.listino.exists():
            logger.log(f"Continuativa: merge listino/fasce da {cont_bundle.listino.name}")
            df_listino = _estrai_listino_fasce(cont_bundle.listino)
            df_input = pd.merge(df_input, df_listino, on="Codice_Articolo", how="left")
        if cont_bundle.prezzi and cont_bundle.prezzi.exists():
            logger.log(f"Continuativa: merge prezzi acquisto da {cont_bundle.prezzi.name}")
            df_prezzi = _estrai_prezzi_acquisto(cont_bundle.prezzi)
            df_input = pd.merge(df_input, df_prezzi, on="Codice_Articolo", how="left")
        df_input = _apply_price_band(df_input)

        math_res = _run_math_forecast(df_input, fattore_copertura=fattore_copertura, is_continuativa=True)
        _save_csv(df_input, output_orders / "orders_continuativa_dati_originali.csv")
        _save_csv(math_res["forecast"], output_orders / "orders_continuativa_previsione_math.csv")
        logger.log(f"Continuativa math: articoli={len(df_input)}, totale_acquisto={math_res['totale']}")

        cont_summary: Dict[str, Any] = {
            "season": cont_bundle.code,
            "articles_input": int(len(df_input)),
            "totale_math": int(math_res["totale"]),
            "listino_file": str(cont_bundle.listino) if cont_bundle.listino else "",
            "prezzi_file": str(cont_bundle.prezzi) if cont_bundle.prezzi else "",
            "output_files": [
                "orders_continuativa_dati_originali.csv",
                "orders_continuativa_previsione_math.csv",
            ],
        }

        cont_last3: List[SeasonBundle] = discovered["continuative_last3"]
        if enable_full and len(cont_last3) >= 3:
            s1, s2, s3 = cont_last3
            logger.log(f"Continuativa full: training su {s1.code}, {s2.code}, {s3.code}")
            df_s1 = _estrai_rf(s1.totali, s1.colori, s1.taglie)
            df_s2 = _estrai_rf(s2.totali, s2.colori, s2.taglie)
            df_s3 = _estrai_rf(s3.totali, s3.colori, s3.taglie)
            full = _run_rf_and_hybrid(
                df_s1=df_s1,
                df_s2=df_s2,
                df_s3=df_s3,
                anni=(_bundle_year(s1), _bundle_year(s2), _bundle_year(s3)),
                fattore_copertura=fattore_copertura,
                df_math_latest=df_input,
            )
            if full.get("enabled"):
                _save_csv(full["risultato_rf"], output_orders / "orders_continuativa_previsione_rf.csv")
                _save_csv(full["df_ibrido"], output_orders / "orders_continuativa_previsione_ibrida.csv")
                logger.log(
                    f"Continuativa full: totale_rf={full['totale_rf']}, totale_ibrido={full['totale_ibrido']}"
                )
                cont_summary["full"] = {
                    "enabled": True,
                    "seasons": [s1.code, s2.code, s3.code],
                    "totale_rf": int(full["totale_rf"]),
                    "totale_ibrido": int(full["totale_ibrido"]),
                    "output_files": [
                        "orders_continuativa_previsione_rf.csv",
                        "orders_continuativa_previsione_ibrida.csv",
                    ],
                }
            else:
                reason = full.get("reason", "full_mode_disabled")
                logger.log(f"Continuativa full: saltata ({reason})")
                cont_summary["full"] = {"enabled": False, "reason": reason}
        else:
            reason = "missing_3_continuative_seasons" if len(cont_last3) < 3 else "full_mode_disabled"
            logger.log(f"Continuativa full: saltata ({reason})")
            cont_summary["full"] = {"enabled": False, "reason": reason}

        summary["continuativa"] = cont_summary
    else:
        logger.log("Continuativa: nessuna stagione y/g trovata, step saltato.")
        summary["continuativa"] = {"enabled": False, "reason": "no_continuativa_bundle"}

    logger.write(output_orders / "orders_run_log.txt")
    (output_orders / "orders_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.log("Modulo ordini completato.")
    return summary
