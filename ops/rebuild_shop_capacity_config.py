from __future__ import annotations

import argparse
import math
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"
BASE_CONFIG_PATH = CONFIG_DIR / "lista-negozi.xlsx"
INTEGRATED_CONFIG_PATH = CONFIG_DIR / "lista-negozi_integrato.xlsx"
OVERRIDES_PATH = CONFIG_DIR / "shop_capacity_overrides.csv"
BACKUP_DIR = CONFIG_DIR / "backup"

DEFAULT_SOURCE_CANDIDATES = [
    CONFIG_DIR / "negozi-capacita_stimato_v2.xlsx",
    CONFIG_DIR / "negozi-capacita_stimato.xlsx",
    CONFIG_DIR / "negozi-capacita_originale.xlsx",
    Path(r"E:\Negozi apacità contenitiva ed espositiva_STIMATO_v2.xlsx"),
    Path(r"E:\Negozi apacità contenitiva ed espositiva_STIMATO.xlsx"),
    Path(r"E:\Negozi apacità contenitiva ed espositiva.xlsx"),
]

MANUAL_STORE_MAP: Dict[str, str] = {
    "AR": "Arese/Il Centro",
    "EU": "RM/Euroma2",
    "RM": "RM/Porte di Roma",
    "BO": "Casalecchio/Granreno",
    "BS": "BS/Elnos",
    "CO": "Conegliano",
    "NV": "Mestre/Nave de Vero",
    "VR": "VR/Adigeo",
    "RI": "RN/Le Befane",
    "OR": "BG/Oriocenter",
    "LN": "Lonato/Il Leone",
    "PM": "SA/Maximall Pompeii",
    "PD": "PD/via Manin",
    "ME2": "Mestre/Ferretto",
    "TV": "TV/C.so Popolo",
    "CA": "Castelfranco V",
    "MC": "Marcon",
    "MI": "Mirano",
    "SM": "S.Maria di Sala",
    "SC": "Scorze Factory",
    "SD": "San Dona'",
}

NON_STORE_SIGLAS = {"M4", "WEB", "MR", "SP", "MP"}
RAW_FIELDS = [
    "cap_scaff_paia",
    "cap_eff_paia",
    "ratio_eff_vs_scaff",
    "diff_paia",
    "vendite_24_paia",
    "giro_stock",
    "stock_medio_ideale_paia",
    "diff_eff_vs_ideale_paia",
    "linee_eff",
    "linee_ideali",
    "diff_linee",
]
OVERRIDE_FIELDS = [
    "cap_scaff_paia",
    "cap_eff_paia",
    "ratio_eff_vs_scaff",
    "vendite_24_paia",
    "giro_stock",
    "stock_medio_ideale_paia",
    "linee_eff",
    "linee_ideali",
]


def _round_to_step(value: float, step: float = 5.0) -> float:
    if not np.isfinite(value):
        return float("nan")
    return float(step * round(float(value) / step))


def _coerce_num(value: object) -> float:
    num = pd.to_numeric(value, errors="coerce")
    return float(num) if pd.notna(num) else float("nan")


def _trimmed_median(values: Iterable[float], low: float | None = None, high: float | None = None) -> float:
    series = pd.Series(list(values), dtype="float64").replace([np.inf, -np.inf], np.nan).dropna()
    if low is not None:
        series = series[series >= low]
    if high is not None:
        series = series[series <= high]
    if series.empty:
        return float("nan")
    if len(series) < 4:
        return float(series.median())
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    if iqr > 0:
        lo = q1 - 1.5 * iqr
        hi = q3 + 1.5 * iqr
        trimmed = series[(series >= lo) & (series <= hi)]
        if not trimmed.empty:
            series = trimmed
    return float(series.median())


def _pick_metric(
    stats: pd.DataFrame,
    metric: str,
    fascia: float | None,
    family: str,
    default: float,
) -> float:
    if pd.notna(fascia):
        fascia_rows = stats[(stats["scope"] == "fascia") & (stats["key"] == str(int(float(fascia)))) & stats[metric].notna()]
        if not fascia_rows.empty:
            return float(fascia_rows.iloc[0][metric])
    family_rows = stats[(stats["scope"] == "family") & (stats["key"] == family) & stats[metric].notna()]
    if not family_rows.empty:
        return float(family_rows.iloc[0][metric])
    global_rows = stats[(stats["scope"] == "global") & (stats["key"] == "all") & stats[metric].notna()]
    if not global_rows.empty:
        return float(global_rows.iloc[0][metric])
    return float(default)


def _source_candidates(extra: Optional[Path]) -> list[Path]:
    candidates = []
    if extra is not None:
        candidates.append(extra)
    candidates.extend(DEFAULT_SOURCE_CANDIDATES)
    return candidates


def _find_source(path_hint: Optional[Path]) -> Path:
    for candidate in _source_candidates(path_hint):
        if candidate and candidate.exists():
            return candidate
    raise FileNotFoundError("Nessun file sorgente capacità trovato.")


def load_base_config(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0)
    df["Sigla"] = df["Sigla"].astype(str).str.strip().str.upper().replace({"W": "WEB", "NU": "NV", "M2": "ME2"})
    df["Mq"] = pd.to_numeric(df["Mq"], errors="coerce")
    df["Fascia"] = pd.to_numeric(df["Fascia"], errors="coerce")
    return df


def load_capacity_source(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    xls = pd.ExcelFile(path)
    if "dati_stimati" in xls.sheet_names:
        df = pd.read_excel(path, sheet_name="dati_stimati")
        method = pd.read_excel(path, sheet_name="metodo_stima") if "metodo_stima" in xls.sheet_names else pd.DataFrame()
    else:
        df = pd.read_excel(path, sheet_name=xls.sheet_names[0], header=2)
        cols = list(df.columns)
        rename_map = {
            cols[0]: "idx",
            cols[1]: "negozio",
            cols[2]: "cap_scaff_paia",
            cols[3]: "cap_eff_paia",
            cols[4]: "ratio_eff_vs_scaff",
            cols[5]: "diff_paia",
            cols[6]: "vendite_24_paia",
            cols[7]: "giro_stock",
            cols[8]: "stock_medio_ideale_paia",
            cols[9]: "diff_eff_vs_ideale_paia",
            cols[10]: "linee_eff",
            cols[11]: "linee_ideali",
            cols[12]: "diff_linee",
        }
        df = df.rename(columns=rename_map)
        method = pd.DataFrame()

    df = df.copy()
    df["negozio"] = df["negozio"].astype(str).str.strip()
    df = df[df["negozio"].notna() & df["negozio"].ne("")].copy()
    for col in RAW_FIELDS:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ("est_cap_scaff_paia", "est_diff_paia", "est_diff_eff_vs_ideale_paia", "est_linee_eff", "est_linee_ideali", "est_diff_linee"):
        if col not in df.columns:
            df[col] = False
        df[col] = df[col].fillna(False).astype(bool)
    return df, method


def load_manual_overrides(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["Sigla", "enabled", *OVERRIDE_FIELDS, "note"])
    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame(columns=["Sigla", "enabled", *OVERRIDE_FIELDS, "note"])
    cols = {str(c).strip().lower(): c for c in df.columns}
    sig_col = cols.get("sigla")
    if not sig_col:
        raise ValueError("Il file override capacità deve contenere la colonna 'Sigla'.")
    df = df.rename(columns={sig_col: "Sigla"})
    df["Sigla"] = df["Sigla"].astype(str).str.strip().str.upper()
    if "enabled" not in df.columns:
        df["enabled"] = True
    df["enabled"] = (
        df["enabled"]
        .apply(lambda v: str(v).strip().lower() if pd.notna(v) else "true")
        .map({"true": True, "1": True, "yes": True, "y": True, "false": False, "0": False, "no": False, "n": False})
        .fillna(True)
    )
    for field in OVERRIDE_FIELDS:
        if field not in df.columns:
            df[field] = np.nan
        df[field] = pd.to_numeric(df[field], errors="coerce")
    if "note" not in df.columns:
        df["note"] = ""
    df["note"] = df["note"].fillna("").astype(str)
    return df[["Sigla", "enabled", *OVERRIDE_FIELDS, "note"]]


def build_reference_stats(linked: pd.DataFrame) -> pd.DataFrame:
    ref = linked[(linked["is_store"]) & linked["cap_eff_paia_source"].notna() & (linked["cap_eff_paia_source"] > 0)].copy()
    ref["family"] = np.where(ref["Fascia"] >= 6, "outlet", "fullprice")
    ref["cap_eff_over_ideal"] = ref["cap_eff_paia_source"] / ref["stock_medio_ideale_paia_source"]
    ref["cap_eff_per_mq"] = ref["cap_eff_paia_source"] / ref["Mq"]
    ref["ratio_eff_vs_scaff_clean"] = ref["ratio_eff_vs_scaff_source"]
    ref["linee_eff_over_cap"] = ref["linee_eff_source"] / ref["cap_eff_paia_source"]
    ref["linee_ideal_over_stock"] = ref["linee_ideali_source"] / ref["stock_medio_ideale_paia_source"]
    ref["linee_ideal_over_linee_eff"] = ref["linee_ideali_source"] / ref["linee_eff_source"]

    rows: list[dict[str, object]] = []
    group_specs = [("global", ["all"]), ("family", sorted(ref["family"].dropna().unique())), ("fascia", sorted(ref["Fascia"].dropna().unique()))]
    for scope, keys in group_specs:
        for key in keys:
            if scope == "global":
                subset = ref
                key_label = "all"
            elif scope == "family":
                subset = ref[ref["family"] == key]
                key_label = str(key)
            else:
                subset = ref[ref["Fascia"] == key]
                key_label = str(int(float(key)))
            if subset.empty:
                continue
            rows.append(
                {
                    "scope": scope,
                    "key": key_label,
                    "cap_eff_over_ideal": _trimmed_median(subset["cap_eff_over_ideal"], 0.8, 8.0),
                    "cap_eff_per_mq": _trimmed_median(
                        subset["cap_eff_per_mq"],
                        10.0 if scope != "family" or key_label != "outlet" else 8.0,
                        60.0 if scope != "family" or key_label != "outlet" else 35.0,
                    ),
                    "ratio_eff_vs_scaff": _trimmed_median(subset["ratio_eff_vs_scaff_clean"], 0.8, 1.6),
                    "linee_eff_over_cap": _trimmed_median(subset["linee_eff_over_cap"], 0.08, 0.18),
                    "linee_ideal_over_stock": _trimmed_median(subset["linee_ideal_over_stock"], 0.08, 0.18),
                    "linee_ideal_over_linee_eff": _trimmed_median(subset["linee_ideal_over_linee_eff"], 0.10, 1.10),
                    "reference_mq": _trimmed_median(subset["Mq"], 50.0, 1000.0),
                }
            )
    return pd.DataFrame(rows)


def build_linked_config(base_cfg: pd.DataFrame, source_df: pd.DataFrame) -> pd.DataFrame:
    source_map = source_df.set_index("negozio").to_dict(orient="index")
    rows = []
    for _, row in base_cfg.iterrows():
        sigla = str(row["Sigla"]).strip().upper()
        matched_store = MANUAL_STORE_MAP.get(sigla)
        source = source_map.get(matched_store or "", {})
        is_store = sigla not in NON_STORE_SIGLAS and pd.notna(row["Fascia"])
        out = {
            "Negozi": row["Negozi"],
            "Sigla": sigla,
            "Mq": row["Mq"],
            "Fascia": row["Fascia"],
            "is_store": bool(is_store),
            "matched_store_name": matched_store,
            "match_method": "manual_map" if matched_store else "no_map",
            "match_found": bool(source),
            "source_row_has_estimates": bool(source and any(bool(source.get(c, False)) for c in source_df.columns if str(c).startswith("est_"))),
        }
        for field in RAW_FIELDS:
            out[f"{field}_source"] = _coerce_num(source.get(field))
        for flag in ("est_cap_scaff_paia", "est_diff_paia", "est_diff_eff_vs_ideale_paia", "est_linee_eff", "est_linee_ideali", "est_diff_linee"):
            out[flag] = bool(source.get(flag, False))
        rows.append(out)
    return pd.DataFrame(rows)


def resolve_capacities(linked: pd.DataFrame, stats: pd.DataFrame, overrides: pd.DataFrame) -> pd.DataFrame:
    out = linked.copy()
    override_map = (
        overrides[overrides["enabled"].fillna(False)].drop_duplicates(subset=["Sigla"], keep="last").set_index("Sigla").to_dict(orient="index")
        if overrides is not None and not overrides.empty
        else {}
    )
    default_eff_over_ideal = _pick_metric(stats, "cap_eff_over_ideal", np.nan, "fullprice", 2.4)
    default_cap_per_mq = _pick_metric(stats, "cap_eff_per_mq", np.nan, "fullprice", 35.0)
    default_ratio_eff_vs_scaff = _pick_metric(stats, "ratio_eff_vs_scaff", np.nan, "fullprice", 1.3)
    default_linee_eff_over_cap = _pick_metric(stats, "linee_eff_over_cap", np.nan, "fullprice", 0.12)
    default_linee_ideal_over_stock = _pick_metric(stats, "linee_ideal_over_stock", np.nan, "fullprice", 0.13)
    default_linee_ideal_over_linee_eff = _pick_metric(stats, "linee_ideal_over_linee_eff", np.nan, "fullprice", 0.42)

    for idx, row in out.iterrows():
        sigla = row["Sigla"]
        is_store = bool(row["is_store"])
        if not is_store:
            out.at[idx, "capacity_link_status"] = "NON_STORE"
            out.at[idx, "capacity_estimation_method"] = "not_applicable"
            continue

        fascia = row["Fascia"]
        family = "outlet" if pd.notna(fascia) and float(fascia) >= 6 else "fullprice"
        eff_over_ideal = _pick_metric(stats, "cap_eff_over_ideal", fascia, family, default_eff_over_ideal)
        cap_per_mq = _pick_metric(stats, "cap_eff_per_mq", fascia, family, default_cap_per_mq)
        ratio_eff_vs_scaff = _pick_metric(stats, "ratio_eff_vs_scaff", fascia, family, default_ratio_eff_vs_scaff)
        linee_eff_over_cap = _pick_metric(stats, "linee_eff_over_cap", fascia, family, default_linee_eff_over_cap)
        linee_ideal_over_stock = _pick_metric(stats, "linee_ideal_over_stock", fascia, family, default_linee_ideal_over_stock)
        linee_ideal_over_linee_eff = _pick_metric(stats, "linee_ideal_over_linee_eff", fascia, family, default_linee_ideal_over_linee_eff)
        ref_mq = _pick_metric(stats, "reference_mq", fascia, family, float(row["Mq"]) if pd.notna(row["Mq"]) else 150.0)
        override = override_map.get(sigla)

        raw_cap_eff = row["cap_eff_paia_source"]
        raw_cap_scaff = row["cap_scaff_paia_source"]
        raw_stock_ideal = row["stock_medio_ideale_paia_source"]
        raw_giro_stock = row["giro_stock_source"] if pd.notna(row["giro_stock_source"]) and row["giro_stock_source"] > 0 else 2.5
        raw_linee_eff = row["linee_eff_source"]
        raw_linee_ideali = row["linee_ideali_source"]
        has_positive_source_capacity = pd.notna(raw_cap_eff) and raw_cap_eff > 0

        if has_positive_source_capacity:
            cap_eff = float(raw_cap_eff)
            cap_scaff = float(raw_cap_scaff) if pd.notna(raw_cap_scaff) and raw_cap_scaff > 0 else cap_eff / ratio_eff_vs_scaff
            stock_ideal = (
                float(raw_stock_ideal)
                if pd.notna(raw_stock_ideal) and raw_stock_ideal > 0
                else cap_eff / eff_over_ideal
            )
            vendite_24 = (
                float(row["vendite_24_paia_source"])
                if pd.notna(row["vendite_24_paia_source"]) and row["vendite_24_paia_source"] > 0
                else stock_ideal * raw_giro_stock
            )
            linee_eff = float(raw_linee_eff) if pd.notna(raw_linee_eff) and raw_linee_eff > 0 else cap_eff * linee_eff_over_cap
            linee_ideali = (
                float(raw_linee_ideali)
                if pd.notna(raw_linee_ideali) and raw_linee_ideali > 0
                else stock_ideal * linee_ideal_over_stock
            )
            status = "OK_MANUAL_MATCH"
            method = "source_manual_map"
        else:
            mq = float(row["Mq"]) if pd.notna(row["Mq"]) and row["Mq"] > 0 else float("nan")
            stock_ideal_from_source = float(raw_stock_ideal) if pd.notna(raw_stock_ideal) and raw_stock_ideal > 0 else float("nan")
            linee_ideali_from_source = float(raw_linee_ideali) if pd.notna(raw_linee_ideali) and raw_linee_ideali > 0 else float("nan")
            mq_based = float("nan")
            if np.isfinite(mq):
                damped_mq = math.sqrt(max(mq, 1.0) * max(ref_mq, 1.0))
                mq_based = damped_mq * cap_per_mq
            stock_based = stock_ideal_from_source * eff_over_ideal if np.isfinite(stock_ideal_from_source) else float("nan")
            lines_based = float("nan")
            if (
                np.isfinite(linee_ideali_from_source)
                and linee_ideali_from_source > 0
                and np.isfinite(linee_ideal_over_linee_eff)
                and linee_ideal_over_linee_eff > 0
                and np.isfinite(linee_eff_over_cap)
                and linee_eff_over_cap > 0
            ):
                est_linee_eff_from_ideal = linee_ideali_from_source / linee_ideal_over_linee_eff
                lines_based = est_linee_eff_from_ideal / linee_eff_over_cap

            candidates = [v for v in (stock_based, lines_based) if np.isfinite(v) and v > 0]
            if np.isfinite(mq_based) and mq_based > 0:
                if candidates:
                    mq_cap = max(candidates) * 1.35
                    candidates.append(min(mq_based, mq_cap))
                else:
                    candidates.append(mq_based)

            if candidates:
                cap_eff = float(pd.Series(candidates, dtype="float64").median())
                if np.isfinite(stock_based) and np.isfinite(lines_based):
                    method = "estimated_from_stock_and_space"
                    status = "ESTIMATED_FROM_STOCK_AND_SPACE"
                elif np.isfinite(stock_based):
                    method = "estimated_from_stock_ideal"
                    status = "ESTIMATED_FROM_STOCK_IDEAL"
                elif np.isfinite(lines_based):
                    method = "estimated_from_space_lines"
                    status = "ESTIMATED_FROM_SPACE_LINES"
                else:
                    method = "estimated_from_mq_family"
                    status = "ESTIMATED_FROM_MQ"
            elif np.isfinite(mq_based):
                cap_eff = mq_based
                method = "estimated_from_mq_family"
                status = "ESTIMATED_FROM_MQ"
            else:
                cap_eff = ref_mq * cap_per_mq
                method = "estimated_from_family_default"
                status = "ESTIMATED_FROM_FAMILY_DEFAULT"

            cap_eff = _round_to_step(cap_eff, 5.0)
            cap_scaff = _round_to_step(cap_eff / ratio_eff_vs_scaff, 5.0)
            if np.isfinite(stock_ideal_from_source):
                stock_ideal = _round_to_step(stock_ideal_from_source, 1.0)
            else:
                stock_ideal = _round_to_step(cap_eff / eff_over_ideal, 1.0)
            vendite_24 = _round_to_step(stock_ideal * raw_giro_stock, 1.0)
            if np.isfinite(linee_ideali_from_source):
                linee_ideali = _round_to_step(linee_ideali_from_source, 5.0)
                linee_eff = _round_to_step(linee_ideali / linee_ideal_over_linee_eff, 5.0)
            else:
                linee_eff = _round_to_step(cap_eff * linee_eff_over_cap, 5.0)
            if not np.isfinite(linee_ideali_from_source) and np.isfinite(stock_ideal) and stock_ideal > 0:
                linee_ideali = _round_to_step(stock_ideal * linee_ideal_over_stock, 5.0)
            elif not np.isfinite(linee_ideali_from_source):
                linee_ideali = _round_to_step(linee_eff * linee_ideal_over_linee_eff, 5.0)

        diff_paia = cap_eff - cap_scaff
        diff_eff_vs_ideale = cap_eff - stock_ideal
        diff_linee = linee_eff - linee_ideali

        if override:
            override_ratio = _coerce_num(override.get("ratio_eff_vs_scaff"))
            if not np.isfinite(override_ratio) or override_ratio <= 0:
                override_ratio = ratio_eff_vs_scaff

            if pd.notna(override.get("cap_eff_paia")) and float(override.get("cap_eff_paia")) > 0:
                cap_eff = float(override.get("cap_eff_paia"))
            if pd.notna(override.get("cap_scaff_paia")) and float(override.get("cap_scaff_paia")) > 0:
                cap_scaff = float(override.get("cap_scaff_paia"))
            else:
                cap_scaff = cap_eff / override_ratio

            if pd.notna(override.get("stock_medio_ideale_paia")) and float(override.get("stock_medio_ideale_paia")) > 0:
                stock_ideal = float(override.get("stock_medio_ideale_paia"))
            elif not (np.isfinite(stock_ideal) and stock_ideal > 0):
                stock_ideal = cap_eff / eff_over_ideal

            if pd.notna(override.get("giro_stock")) and float(override.get("giro_stock")) > 0:
                raw_giro_stock = float(override.get("giro_stock"))

            if pd.notna(override.get("vendite_24_paia")) and float(override.get("vendite_24_paia")) > 0:
                vendite_24 = float(override.get("vendite_24_paia"))
            else:
                vendite_24 = stock_ideal * raw_giro_stock

            if pd.notna(override.get("linee_eff")) and float(override.get("linee_eff")) > 0:
                linee_eff = float(override.get("linee_eff"))
            else:
                linee_eff = cap_eff * linee_eff_over_cap

            if pd.notna(override.get("linee_ideali")) and float(override.get("linee_ideali")) > 0:
                linee_ideali = float(override.get("linee_ideali"))
            else:
                linee_ideali = stock_ideal * linee_ideal_over_stock

            ratio_eff_vs_scaff = override_ratio
            diff_paia = cap_eff - cap_scaff
            diff_eff_vs_ideale = cap_eff - stock_ideal
            diff_linee = linee_eff - linee_ideali
            status = "OVERRIDE_MANUAL"
            method = "manual_override"

        out.at[idx, "cap_scaff_paia_linked"] = _round_to_step(cap_scaff, 5.0)
        out.at[idx, "cap_eff_paia_linked"] = _round_to_step(cap_eff, 5.0)
        out.at[idx, "ratio_eff_vs_scaff_linked"] = round(float(cap_eff) / float(cap_scaff), 6) if cap_scaff else np.nan
        out.at[idx, "diff_paia_linked"] = _round_to_step(diff_paia, 5.0)
        out.at[idx, "vendite_24_paia_linked"] = _round_to_step(vendite_24, 1.0)
        out.at[idx, "giro_stock_linked"] = round(float(raw_giro_stock), 4)
        out.at[idx, "stock_medio_ideale_paia_linked"] = _round_to_step(stock_ideal, 1.0)
        out.at[idx, "diff_eff_vs_ideale_paia_linked"] = _round_to_step(diff_eff_vs_ideale, 1.0)
        out.at[idx, "linee_eff_linked"] = _round_to_step(linee_eff, 5.0)
        out.at[idx, "linee_ideali_linked"] = _round_to_step(linee_ideali, 5.0)
        out.at[idx, "diff_linee_linked"] = _round_to_step(diff_linee, 5.0)
        out.at[idx, "capacity_link_status"] = status
        out.at[idx, "capacity_estimation_method"] = method
        out.at[idx, "capacity_override_note"] = str(override.get("note", "")).strip() if override else ""

    return out


def build_sigla_summary(linked: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "Sigla",
        "Negozi",
        "Mq",
        "Fascia",
        "matched_store_name",
        "match_found",
        "source_row_has_estimates",
        "cap_scaff_paia_source",
        "cap_eff_paia_source",
        "vendite_24_paia_source",
        "stock_medio_ideale_paia_source",
        "linee_eff_source",
        "linee_ideali_source",
        "cap_scaff_paia_linked",
        "cap_eff_paia_linked",
        "vendite_24_paia_linked",
        "stock_medio_ideale_paia_linked",
        "linee_eff_linked",
        "linee_ideali_linked",
        "capacity_link_status",
        "capacity_estimation_method",
        "capacity_override_note",
    ]
    return linked[cols].copy()


def build_store_sheet(source_df: pd.DataFrame, linked: pd.DataFrame) -> pd.DataFrame:
    mapped = linked[["Sigla", "matched_store_name", "capacity_link_status", "capacity_estimation_method", "capacity_override_note"]].copy()
    mapped = mapped[mapped["matched_store_name"].notna()].rename(columns={"matched_store_name": "negozio"})
    out = source_df.merge(mapped, on="negozio", how="left")
    return out


def build_method_sheet(source_method: pd.DataFrame, estimated_rows: pd.DataFrame, source_path: Path) -> pd.DataFrame:
    notes = pd.DataFrame(
        [
            {
                "campo": "matching_sigla",
                "metodo": "abbinamento manuale negozio BARCA -> nome negozio file capacità",
                "parametro": "mapping esplicito",
            },
            {
                "campo": "cap_eff_paia_linked",
                "metodo": "usa il dato sorgente se presente; altrimenti stock ideale * rapporto fascia/family; fallback mq smorzati",
                "parametro": "mediane robuste",
            },
            {
                "campo": "cap_scaff_paia_linked",
                "metodo": "cap_eff_paia_linked / ratio_eff_vs_scaff",
                "parametro": "mediana robusta rapporto",
            },
            {
                "campo": "linee_eff_linked",
                "metodo": "cap_eff_paia_linked * mediana(linee_eff/cap_eff)",
                "parametro": "mediana robusta",
            },
            {
                "campo": "linee_ideali_linked",
                "metodo": "stock_medio_ideale_paia_linked * mediana(linee_ideali/stock_ideale), fallback su linee_eff",
                "parametro": "mediana robusta",
            },
            {
                "campo": "source_file",
                "metodo": "sorgente capacità usata per rigenerare il file integrato",
                "parametro": str(source_path),
            },
            {
                "campo": "stores_estimated_count",
                "metodo": "numero negozi store con almeno un campo linked stimato o corretto",
                "parametro": int(
                    estimated_rows["capacity_link_status"].fillna("").str.startswith("ESTIMATED").sum()
                ),
            },
            {
                "campo": "manual_override_file",
                "metodo": "override manuali per negozi specifici applicati dopo la stima",
                "parametro": str(OVERRIDES_PATH),
            },
        ]
    )
    if source_method is None or source_method.empty:
        return notes
    return pd.concat([source_method, notes], ignore_index=True)


def backup_existing_file(path: Path) -> Optional[Path]:
    if not path.exists():
        return None
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"{path.stem}_{timestamp}{path.suffix}"
    shutil.copy2(path, backup_path)
    return backup_path


def write_integrated_workbook(
    base_cfg: pd.DataFrame,
    store_sheet: pd.DataFrame,
    sigla_summary: pd.DataFrame,
    linked_sheet: pd.DataFrame,
    method_sheet: pd.DataFrame,
    overrides_sheet: pd.DataFrame,
    output_path: Path,
) -> None:
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        base_cfg.to_excel(writer, sheet_name="lista_negozi_original", index=False)
        store_sheet.to_excel(writer, sheet_name="capacita_stimate_by_store", index=False)
        sigla_summary.to_excel(writer, sheet_name="capacita_stimate_by_sigla", index=False)
        linked_sheet.to_excel(writer, sheet_name="lista_negozi_linked", index=False)
        overrides_sheet.to_excel(writer, sheet_name="manual_overrides", index=False)
        method_sheet.to_excel(writer, sheet_name="metodo_stima", index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Rigenera config/lista-negozi_integrato.xlsx con capacità negozi risolte.")
    parser.add_argument("--source", type=Path, default=None, help="Path sorgente Excel capacità.")
    parser.add_argument("--base-config", type=Path, default=BASE_CONFIG_PATH, help="Path config base negozi.")
    parser.add_argument("--output", type=Path, default=INTEGRATED_CONFIG_PATH, help="Path output workbook integrato.")
    args = parser.parse_args()

    source_path = _find_source(args.source)
    base_cfg = load_base_config(args.base_config)
    source_df, source_method = load_capacity_source(source_path)
    overrides = load_manual_overrides(OVERRIDES_PATH)
    linked = build_linked_config(base_cfg, source_df)
    stats = build_reference_stats(linked)
    linked_resolved = resolve_capacities(linked, stats, overrides)
    sigla_summary = build_sigla_summary(linked_resolved)
    store_sheet = build_store_sheet(source_df, linked_resolved)
    method_sheet = build_method_sheet(source_method, linked_resolved, source_path)

    backup_path = backup_existing_file(args.output)
    write_integrated_workbook(base_cfg, store_sheet, sigla_summary, linked_resolved, method_sheet, overrides, args.output)

    estimated = linked_resolved[linked_resolved["capacity_link_status"].fillna("").str.startswith("ESTIMATED")]
    print(f"Sorgente usata: {source_path}")
    if backup_path is not None:
        print(f"Backup creato: {backup_path}")
    print(f"Workbook aggiornato: {args.output}")
    if not estimated.empty:
        print("Negozi stimati:")
        print(
            estimated[["Sigla", "Negozi", "cap_eff_paia_linked", "capacity_link_status", "capacity_estimation_method"]]
            .to_string(index=False)
        )


if __name__ == "__main__":
    main()
