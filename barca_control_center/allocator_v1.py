
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np
from .analysis import build_article_shop_transfer_signals, validate_stock_snapshot_integrity
from .hybrid_demand import compute_hybrid_demand
from .reparto_sizes import SUPPORTED_SIZES, normalize_reparto, required_core_sizes

SIZES = list(SUPPORTED_SIZES)
EXCLUDE_SHOPS = {"MR", "MP", "SP", "SPW"}
WAREHOUSE = "M4"
ONLINE = "WEB"

# Capacity policy: keep stores below full saturation to preserve in-store maneuverability.
CAPACITY_UTILIZATION_TARGET = 0.85
CAPACITY_MIN_HEADROOM = 20.0

# Stockout protection for donor stores.
DONOR_SAFETY_FACTOR = 0.35
DONOR_SAFETY_MAX = 4.0

# Operational realism: limit moved pairs per shop per run.
OPS_MOVE_RATIO = 0.22
OPS_MOVE_MIN = 40.0
OPS_MOVE_MAX = 700.0
ENABLE_OPS_MOVE_BUDGET = False
ENABLE_TOP_UP_TO_TARGET = False

# Coverage-first mode: distribute to as many shops as possible (fascia-priority).
# When True the main loop iterates SIZE first then RECEIVERS (fascia order),
# maximising the number of shops covered before concentrating in one.
COVERAGE_FIRST_MODE = True

# Logistics schedule (current organization).
ROUTE_WEEKDAY_BY_SHOP = {
    "BS": 1, "LN": 1,  # Tuesday
    "RI": 2, "BO": 2, "AU": 2, "MC": 2, "NV": 2, "ME2": 2, "VR": 2,  # Wednesday
    "OR": 3, "AR": 3, "CO": 3, "TV": 3, "PD": 3, "CA": 3,  # Thursday
    "MI": 4, "SM": 4,  # Friday
}
COURIER_2D_SHOPS = {"RM", "EU"}
PM_SHOPS = {"PM"}
SD_SHOPS = {"SD"}
PM_MIN_CONSOLIDATION_QTY = 18.0
SD_MIN_CONSOLIDATION_QTY = 10.0


def read_settings(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def role_for_shop(shop: str) -> str:
    if shop in EXCLUDE_SHOPS:
        return "EXCLUDE"
    if shop == WAREHOUSE:
        return "WAREHOUSE"
    if shop == ONLINE:
        return "ONLINE"
    return "STORE"


def _pick_column(columns: List[str], *keys: str):
    for key in keys:
        for col in columns:
            if key in col:
                return col
    return None


def _normalize_shop(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.upper().replace({"W": "WEB", "NU": "NV", "M2": "ME2"})


def _extract_capacity_table(path: Path) -> pd.DataFrame:
    with pd.ExcelFile(path) as xls:
        preferred = ["lista_negozi_linked", "capacita_stimate_by_sigla", "capacita_stimate_by_store"]
        ordered_sheets = [s for s in preferred if s in xls.sheet_names]
        ordered_sheets.extend([s for s in xls.sheet_names if s not in ordered_sheets])

        for sheet in ordered_sheets:
            df = pd.read_excel(xls, sheet_name=sheet)
            df.columns = [str(c).strip().lower() for c in df.columns]

            c_shop = _pick_column(df.columns.tolist(), "sigla", "sig", "cod", "shop", "negozio")
            c_cap = _pick_column(
                df.columns.tolist(),
                "cap_eff_paia_linked",
                "cap_eff_paia_sum",
                "cap_eff_paia",
                "cap_scaff_paia_linked",
                "cap_scaff_paia_sum",
                "cap_scaff_paia",
            )
            c_status = _pick_column(df.columns.tolist(), "capacity_link_status", "capacity_status")

            if not c_shop or not c_cap:
                continue

            cap = pd.DataFrame(
                {
                    "Shop": _normalize_shop(df[c_shop]),
                    "CapacityPairs": pd.to_numeric(df[c_cap], errors="coerce"),
                    "CapacityStatus": df[c_status].astype(str) if c_status else np.nan,
                    "CapacitySource": sheet,
                }
            )
            cap = cap.dropna(subset=["CapacityPairs"])
            cap = cap[cap["CapacityPairs"] > 0]
            if cap.empty:
                continue

            cap = cap.groupby("Shop", as_index=False).agg(
                {"CapacityPairs": "max", "CapacityStatus": "first", "CapacitySource": "first"}
            )
            return cap

    return pd.DataFrame(columns=["Shop", "CapacityPairs", "CapacityStatus", "CapacitySource"])


def load_shops_xlsx(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0)
    df.columns = [str(c).strip().lower() for c in df.columns]

    c_shop = _pick_column(df.columns.tolist(), "sigla", "sig", "cod", "shop", "negozio")
    c_fascia = _pick_column(df.columns.tolist(), "fascia")
    c_mq = _pick_column(df.columns.tolist(), "mq")
    c_name = _pick_column(df.columns.tolist(), "nome", "name")
    if not c_shop:
        raise ValueError("Non trovo colonna sigla/codice negozio nel file config.")

    out = pd.DataFrame(
        {
            "Shop": _normalize_shop(df[c_shop]),
            "Fascia": pd.to_numeric(df[c_fascia], errors="coerce") if c_fascia else np.nan,
            "Mq": pd.to_numeric(df[c_mq], errors="coerce") if c_mq else np.nan,
            "Name": df[c_name].astype(str) if c_name else "",
        }
    )

    cap = _extract_capacity_table(path)
    out = out.merge(cap, on="Shop", how="left")
    out["CapacitySource"] = out.get("CapacitySource", "none").fillna("none")

    # Fill missing capacities with same-fascia median where possible.
    known_cap = out["CapacityPairs"].notna() & (out["CapacityPairs"] > 0)
    fascia_median = out.loc[known_cap & out["Fascia"].notna()].groupby("Fascia")["CapacityPairs"].median()
    global_median = float(out.loc[known_cap, "CapacityPairs"].median()) if known_cap.any() else np.nan

    for idx, row in out.iterrows():
        cap_value = row.get("CapacityPairs", np.nan)
        if not pd.isna(cap_value) and cap_value > 0:
            continue
        if role_for_shop(row["Shop"]) != "STORE":
            continue

        est = np.nan
        source = "none"
        fascia = row.get("Fascia", np.nan)
        if not pd.isna(fascia) and fascia in fascia_median.index:
            est = float(fascia_median.loc[fascia])
            source = "fascia_median"
        elif not pd.isna(global_median):
            est = global_median
            source = "global_median"

        if not pd.isna(est):
            out.at[idx, "CapacityPairs"] = est
            out.at[idx, "CapacitySource"] = source

    return out


def fascia_weight(fascia: Any) -> float:
    if pd.isna(fascia):
        return 0.0
    f = int(float(fascia))
    return {1: 1.40, 2: 1.20, 3: 1.00, 4: 0.85, 5: 0.75, 6: 0.60, 7: 0.55}.get(f, 0.70)


def is_outlet(fascia: Any) -> bool:
    return (not pd.isna(fascia)) and int(float(fascia)) in (6, 7)


def required_run_sizes(
    fascia: Any,
    *,
    reparto: Any = None,
    available_sizes: Optional[List[int]] = None,
) -> List[int]:
    return required_core_sizes(fascia, reparto=reparto, available_sizes=available_sizes)

def build_lookup(articles: pd.DataFrame, sales: pd.DataFrame, shops: pd.DataFrame):
    # Normalize
    articles = articles.copy()
    sales = sales.copy()
    articles["Shop"] = _normalize_shop(articles["Shop"])
    if "Reparto" not in articles.columns:
        articles["Reparto"] = ""
    articles["Reparto"] = articles["Reparto"].map(lambda value: normalize_reparto(value) or "")
    sales["Shop"] = _normalize_shop(sales["Shop"])
    shops = shops.copy()
    shops["Shop"] = _normalize_shop(shops["Shop"])

    # Defensive typing for numeric columns used by allocator.
    for c in ("Periodo_Qty", "Venduto_Qty"):
        if c not in sales.columns:
            sales[c] = 0.0
        sales[c] = pd.to_numeric(sales[c], errors="coerce").fillna(0.0).clip(lower=0.0)

    # shop meta
    for c in ("CapacityPairs", "CapacitySource", "CapacityStatus"):
        if c not in shops.columns:
            shops[c] = np.nan if c != "CapacitySource" else "none"
    meta = shops.set_index("Shop")[["Fascia", "Mq", "CapacityPairs", "CapacitySource", "CapacityStatus"]].to_dict(
        orient="index"
    )

    # demand per (article, shop)
    sales["DemandRaw"] = 0.75 * sales["Periodo_Qty"] + 0.25 * sales["Venduto_Qty"]
    demand = {(r.Article, r.Shop): float(r.DemandRaw) for r in sales.itertuples(index=False)}
    periodo = {(r.Article, r.Shop): float(getattr(r, "Periodo_Qty", 0.0)) for r in sales.itertuples(index=False)}
    sales_signal = {
        (r.Article, r.Shop): float(
            max(
                float(getattr(r, "Periodo_Qty", 0.0) or 0.0),
                float(getattr(r, "Venduto_Qty", 0.0) or 0.0),
            )
        )
        for r in sales.itertuples(index=False)
    }

    # stock per (article, shop) sizes
    stock = {}
    total = {}
    article_reparto = {}
    article_available_sizes: Dict[str, set[int]] = {}
    for r in articles.itertuples(index=False):
        if r.Shop in EXCLUDE_SHOPS:
            continue
        article = str(r.Article).strip()
        reparto = normalize_reparto(getattr(r, "Reparto", ""))
        if reparto and article not in article_reparto:
            article_reparto[article] = reparto
        key = (r.Article, r.Shop)
        sizes = {}
        for s in SIZES:
            qty = pd.to_numeric(getattr(r, f"Size_{s}", 0.0), errors="coerce")
            qty = 0.0 if pd.isna(qty) else float(qty)
            sizes[s] = max(0.0, qty)
            if sizes[s] > 0.0:
                article_available_sizes.setdefault(article, set()).add(int(s))
        stock[key] = sizes
        # Keep total aligned to transferable size buckets to avoid negative drift.
        total[key] = float(sum(sizes.values()))

    # Preserve article/shop pairs seen in sales even when stock export has no explicit row.
    for (article, shop), _ in demand.items():
        if shop in EXCLUDE_SHOPS:
            continue
        key = (article, shop)
        if key in stock:
            continue
        stock[key] = {s: 0.0 for s in SIZES}
        total[key] = 0.0

    return (
        meta,
        demand,
        periodo,
        sales_signal,
        stock,
        total,
        article_reparto,
        {article: sorted(list(sizes)) for article, sizes in article_available_sizes.items()},
    )


def compute_targets(meta, demand, article_shops):
    # Base target rule: weighted demand + presence minimum.
    targets = {}
    presence = {}
    for (article, shop) in article_shops:
        fascia = meta.get(shop, {}).get("Fascia", np.nan)
        role = role_for_shop(shop)
        pres = 1.0 if role in ("STORE", "ONLINE") and not is_outlet(fascia) else 0.0
        if role == "WAREHOUSE":
            pres = 0.0
        presence[(article, shop)] = pres
        d = max(0.0, demand.get((article, shop), 0.0))
        demand_target = round(d * (1.0 + fascia_weight(fascia)))
        safety = 1.0 if d > 0 else 0.0
        targets[(article, shop)] = float(max(pres + safety, float(demand_target)))
    return targets, presence


def fascia_rank_value(fascia: Any) -> int:
    if pd.isna(fascia):
        return 99
    try:
        return int(float(fascia))
    except Exception:
        return 99


def pick_outlet(meta, shops_for_article, demand, sales_signal, article):
    outlet_set = {
        s
        for s in shops_for_article
        if is_outlet(meta.get(s, {}).get("Fascia", np.nan))
    }
    outlet_set.update(
        {
            s
            for s, m in meta.items()
            if is_outlet(m.get("Fascia", np.nan))
        }
    )
    outlets = sorted(outlet_set)
    if not outlets:
        return None
    outlets.sort(
        key=lambda s: (
            -fascia_rank_value(meta.get(s, {}).get("Fascia", np.nan)),
            -int(float(sales_signal.get((article, s), 0.0)) > 0.0),
            -float(sales_signal.get((article, s), 0.0)),
            -float(demand.get((article, s), 0.0)),
            s,
        ),
    )
    return outlets[0]


def can_make_run(meta, stock, article, shop, *, reparto=None, available_sizes=None):
    req = required_run_sizes(
        meta.get(shop, {}).get("Fascia", np.nan),
        reparto=reparto,
        available_sizes=available_sizes,
    )
    sizes = stock.get((article, shop), {})
    return all(sizes.get(sz, 0.0) >= 1.0 for sz in req)


def build_shop_capacity_state(meta, total):
    shop_total_stock = {}
    for (_, shop), qty in total.items():
        if shop in EXCLUDE_SHOPS:
            continue
        shop_total_stock[shop] = shop_total_stock.get(shop, 0.0) + float(qty)

    shop_capacity = {}
    shop_capacity_target = {}
    all_shops = set(shop_total_stock.keys()) | set(meta.keys())
    for shop in all_shops:
        cap = pd.to_numeric(meta.get(shop, {}).get("CapacityPairs", np.nan), errors="coerce")
        cap = np.nan if pd.isna(cap) else float(cap)
        shop_capacity[shop] = cap
        if pd.isna(cap) or cap <= 0:
            shop_capacity_target[shop] = float("inf")
            continue
        target = min(cap * CAPACITY_UTILIZATION_TARGET, cap - CAPACITY_MIN_HEADROOM)
        shop_capacity_target[shop] = max(0.0, float(target))

    return shop_total_stock, shop_capacity, shop_capacity_target


def free_capacity(shop: str, shop_total_stock: Dict[str, float], shop_capacity_target: Dict[str, float]) -> float:
    cap_target = shop_capacity_target.get(shop, float("inf"))
    if np.isinf(cap_target):
        return float("inf")
    return cap_target - shop_total_stock.get(shop, 0.0)


def donor_keep_min(meta, demand, article: str, shop: str) -> float:
    if shop == WAREHOUSE:
        return 0.0
    fascia = meta.get(shop, {}).get("Fascia", np.nan)
    if role_for_shop(shop) != "STORE" or is_outlet(fascia):
        return 0.0
    d = max(0.0, demand.get((article, shop), 0.0))
    safety = min(DONOR_SAFETY_MAX, float(np.ceil(d * DONOR_SAFETY_FACTOR)))
    return max(1.0, safety)


def local_sales_signal_for_shop(sales_signal, signal_lookup, article: str, shop: str) -> float:
    sig = signal_lookup.get((article, shop), {})
    return float(
        sig.get(
            "NotebookVendutoSignal",
            sig.get("ObservedSalesSignal", sales_signal.get((article, shop), 0.0)),
        )
        or 0.0
    )


def donor_can_spare_size(
    meta,
    stock,
    total,
    demand,
    sales_signal,
    signal_lookup,
    article: str,
    donor: str,
    size: int,
    *,
    reparto=None,
    available_sizes=None,
) -> bool:
    qty = float(stock.get((article, donor), {}).get(size, 0.0) or 0.0)
    if qty < 1.0:
        return False
    if donor == WAREHOUSE:
        return True

    fascia = meta.get(donor, {}).get("Fascia", np.nan)
    role = role_for_shop(donor)
    if role != "STORE" or is_outlet(fascia):
        return True

    if local_sales_signal_for_shop(sales_signal, signal_lookup, article, donor) > 0.0 and not can_make_run(
        meta,
        stock,
        article,
        donor,
        reparto=reparto,
        available_sizes=available_sizes,
    ):
        return False

    keep_min = donor_keep_min(meta, demand, article, donor)
    if float(total.get((article, donor), 0.0) or 0.0) - 1.0 < keep_min:
        return False

    required_sizes = required_run_sizes(
        fascia,
        reparto=reparto,
        available_sizes=available_sizes,
    )
    if size in required_sizes and (qty - 1.0) < 1.0:
        return False
    return True


def duplicate_pairs_for_size(stock, article: str, shop: str, size: int) -> float:
    qty = float(stock.get((article, shop), {}).get(size, 0.0) or 0.0)
    return max(0.0, qty - 1.0)


def duplicate_pairs_total(stock, article: str, shop: str) -> float:
    sizes = stock.get((article, shop), {})
    return float(sum(max(0.0, float(qty or 0.0) - 1.0) for qty in sizes.values()))


def _find_excess_gap_receivers(
    meta,
    stock,
    sales_signal,
    signal_lookup,
    article: str,
    donor: str,
    size: int,
    shops_for_article: List[str],
    *,
    shop_total_stock: Optional[Dict] = None,
    shop_capacity_target: Optional[Dict] = None,
    donor_fascia_rank: int = 99,
) -> List[Dict]:
    """
    Trova negozi di fascia *migliore* del donatore (rank più basso) che:
    - hanno un buco sulla taglia ``size`` per ``article``
    - hanno segnale vendite attivo
    - hanno capacità disponibile

    Usato nella fase di consolidamento per preferire negozi di fascia alta
    rispetto all'invio diretto a M4.
    Ordinati per fascia crescente (migliore prima), poi venduto decrescente.
    """
    candidates = []
    for recv in shops_for_article:
        if recv in EXCLUDE_SHOPS or recv in (WAREHOUSE, ONLINE, donor):
            continue
        fascia = meta.get(recv, {}).get("Fascia", np.nan)
        if is_outlet(fascia):
            continue
        if role_for_shop(recv) != "STORE":
            continue
        recv_fascia_rank = fascia_rank_value(fascia)
        # Solo negozi con fascia *migliore* (numero più basso) del donatore
        if recv_fascia_rank >= donor_fascia_rank:
            continue
        local_sales_signal = local_sales_signal_for_shop(sales_signal, signal_lookup, article, recv)
        if local_sales_signal <= 0.0:
            continue  # solo negozi attivi
        if float(stock.get((article, recv), {}).get(size, 0.0) or 0.0) >= 1.0:
            continue  # taglia già coperta
        if shop_total_stock is not None and shop_capacity_target is not None:
            free = free_capacity(recv, shop_total_stock, shop_capacity_target)
            if not np.isinf(free) and free < 1.0:
                continue
        candidates.append(
            {
                "shop": recv,
                "fascia_rank": recv_fascia_rank,
                "sales_signal": local_sales_signal,
            }
        )
    candidates.sort(key=lambda x: (x["fascia_rank"], -x["sales_signal"], x["shop"]))
    return candidates


def prioritized_donors_for_size(
    meta,
    stock,
    total,
    demand,
    sales_signal,
    signal_lookup,
    article: str,
    recv: str,
    size: int,
    shops_for_article: List[str],
    *,
    reparto=None,
    available_sizes=None,
):
    warehouse = []
    duplicates = []
    singles = []
    for donor in shops_for_article:
        if donor in EXCLUDE_SHOPS or donor == ONLINE or donor == recv:
            continue
        if is_outlet(meta.get(donor, {}).get("Fascia", np.nan)):
            continue
        size_qty = float(stock.get((article, donor), {}).get(size, 0.0) or 0.0)
        if size_qty < 1.0:
            continue
        if donor == WAREHOUSE:
            warehouse.append({"shop": donor, "stage": "warehouse"})
            continue
        if not donor_can_spare_size(
            meta,
            stock,
            total,
            demand,
            sales_signal,
            signal_lookup,
            article,
            donor,
            size,
            reparto=reparto,
            available_sizes=available_sizes,
        ):
            continue

        fascia_rank = fascia_rank_value(meta.get(donor, {}).get("Fascia", np.nan))
        sig = signal_lookup.get((article, donor), {})
        local_sales_signal = local_sales_signal_for_shop(sales_signal, signal_lookup, article, donor)
        source_priority_score = float(sig.get("SourcePriorityScore", 0.0))
        duplicate_units = duplicate_pairs_for_size(stock, article, donor, size)
        duplicate_total = duplicate_pairs_total(stock, article, donor)

        if duplicate_units >= 1.0:
            duplicates.append(
                {
                    "shop": donor,
                    "stage": "duplicate",
                    "duplicate_units": duplicate_units,
                    "duplicate_total": duplicate_total,
                    "fascia_rank": fascia_rank,
                    "sales_signal": local_sales_signal,
                    "source_priority_score": source_priority_score,
                }
            )
            continue

        singles.append(
            {
                "shop": donor,
                "stage": "single",
                "fascia_rank": fascia_rank,
                "sales_signal": local_sales_signal,
                "source_priority_score": source_priority_score,
                "stock_total": float(total.get((article, donor), 0.0) or 0.0),
            }
        )

    duplicates.sort(
        key=lambda item: (
            -item["fascia_rank"],
            -item["duplicate_units"],
            -item["duplicate_total"],
            item["sales_signal"],
            -item["source_priority_score"],
            item["shop"],
        )
    )
    singles.sort(
        key=lambda item: (
            -item["fascia_rank"],
            item["sales_signal"],
            -item["source_priority_score"],
            -item["stock_total"],
            item["shop"],
        )
    )
    return warehouse + duplicates + singles


def should_reallocate_broken_line(meta, sales_signal, signal_lookup, article: str, shop: str) -> bool:
    fascia = meta.get(shop, {}).get("Fascia", np.nan)
    if role_for_shop(shop) != "STORE" or is_outlet(fascia):
        return False
    return local_sales_signal_for_shop(sales_signal, signal_lookup, article, shop) <= 0.0


def prioritized_receivers_for_broken_line(
    meta,
    stock,
    total,
    demand,
    sales_signal,
    signal_lookup,
    article: str,
    donor: str,
    shops_for_article: List[str],
    *,
    reparto=None,
    available_sizes=None,
    shop_total_stock=None,
    shop_capacity_target=None,
):
    donor_total = float(total.get((article, donor), 0.0) or 0.0)
    if donor_total < 1.0:
        return []

    candidates = []
    for recv in shops_for_article:
        if recv in EXCLUDE_SHOPS or recv in (WAREHOUSE, ONLINE, donor):
            continue
        fascia = meta.get(recv, {}).get("Fascia", np.nan)
        if is_outlet(fascia):
            continue
        local_sales_signal = local_sales_signal_for_shop(sales_signal, signal_lookup, article, recv)
        if local_sales_signal <= 0.0:
            continue
        current_total = float(total.get((article, recv), 0.0) or 0.0)
        req_sizes = required_run_sizes(
            meta.get(recv, {}).get("Fascia", np.nan),
            reparto=reparto,
            available_sizes=available_sizes,
        )
        missing_sizes = [
            sz
            for sz in req_sizes
            if float(stock.get((article, recv), {}).get(sz, 0.0) or 0.0) < 1.0
            and float(stock.get((article, donor), {}).get(sz, 0.0) or 0.0) >= 1.0
        ]
        if not missing_sizes:
            continue
        if shop_total_stock is not None and shop_capacity_target is not None:
            free = free_capacity(recv, shop_total_stock, shop_capacity_target)
            if not np.isinf(free) and free < 1.0:
                continue
        candidates.append(
            {
                "shop": recv,
                "fascia_rank": fascia_rank_value(fascia),
                "has_presence": current_total > 0.0,
                "current_total": current_total,
                "sales_signal": local_sales_signal,
                "demand": float(demand.get((article, recv), 0.0) or 0.0),
                "destination_priority_score": float(
                    signal_lookup.get((article, recv), {}).get("DestinationPriorityScore", local_sales_signal)
                ),
                "missing_sizes": missing_sizes,
            }
        )

    if not candidates:
        return []

    candidates.sort(
        key=lambda item: (
            0 if item["has_presence"] else 1,
            len(item["missing_sizes"]),
            item["fascia_rank"],
            -item["sales_signal"],
            -item["demand"],
            -item["destination_priority_score"],
            -item["current_total"],
            item["shop"],
        )
    )
    return candidates


def apply_transfer_move(
    stock,
    total,
    shop_total_stock,
    ops_out_used,
    ops_in_used,
    transfers,
    article: str,
    size: int,
    donor: str,
    recv: str,
    reason: str,
):
    stock[(article, donor)][size] -= 1.0
    stock.setdefault((article, recv), {}).setdefault(size, 0.0)
    stock[(article, recv)][size] = stock[(article, recv)].get(size, 0.0) + 1.0
    total[(article, donor)] = total.get((article, donor), 0.0) - 1.0
    total[(article, recv)] = total.get((article, recv), 0.0) + 1.0
    shop_total_stock[donor] = shop_total_stock.get(donor, 0.0) - 1.0
    shop_total_stock[recv] = shop_total_stock.get(recv, 0.0) + 1.0
    ops_out_used[donor] = ops_out_used.get(donor, 0.0) + 1.0
    ops_in_used[recv] = ops_in_used.get(recv, 0.0) + 1.0
    transfers.append({"Article": article, "Size": size, "Qty": 1, "From": donor, "To": recv, "Reason": reason})


def build_ops_budgets(meta, shop_total_stock, shop_capacity_target):
    in_budget = {}
    out_budget = {}
    for shop, stock_qty in shop_total_stock.items():
        role = role_for_shop(shop)
        if role == "WAREHOUSE":
            in_budget[shop] = float("inf")
            out_budget[shop] = float("inf")
            continue
        if role == "ONLINE":
            base = max(OPS_MOVE_MIN, min(OPS_MOVE_MAX, stock_qty * OPS_MOVE_RATIO))
            in_budget[shop] = float(base)
            out_budget[shop] = 0.0
            continue

        base = max(OPS_MOVE_MIN, min(OPS_MOVE_MAX, stock_qty * OPS_MOVE_RATIO))
        if is_outlet(meta.get(shop, {}).get("Fascia", np.nan)):
            base = min(OPS_MOVE_MAX * 1.5, base * 1.4)
        out_budget[shop] = float(base)

        free = free_capacity(shop, shop_total_stock, shop_capacity_target)
        if np.isinf(free):
            in_budget[shop] = float(base)
        else:
            extra = max(0.0, min(OPS_MOVE_MAX, free * 0.20))
            in_budget[shop] = float(min(OPS_MOVE_MAX, base + extra))
    return in_budget, out_budget


def ops_budget_left(shop: str, used: Dict[str, float], budget: Dict[str, float]) -> float:
    b = budget.get(shop, 0.0)
    if np.isinf(b):
        return float("inf")
    return b - used.get(shop, 0.0)


def ops_budget_allows_move(shop: str, used: Dict[str, float], budget: Dict[str, float]) -> bool:
    if not ENABLE_OPS_MOVE_BUDGET:
        return True
    return ops_budget_left(shop, used, budget) >= 1.0


def _next_weekday_date(base_date: pd.Timestamp, weekday: int) -> pd.Timestamp:
    days_ahead = (weekday - base_date.weekday()) % 7
    return base_date + pd.Timedelta(days=int(days_ahead))


def _next_courier_2d_date(base_date: pd.Timestamp) -> pd.Timestamp:
    # Anchor Monday 2026-01-05; then every 2 days.
    base_norm = base_date.normalize()
    anchor = pd.Timestamp("2026-01-05", tz=base_norm.tz) if base_norm.tz is not None else pd.Timestamp("2026-01-05")
    delta = int((base_norm - anchor).days)
    rem = delta % 2
    return base_norm if rem == 0 else base_norm + pd.Timedelta(days=1)


def _resolve_route_weekday(from_shop: str, to_shop: str) -> Optional[int]:
    d_from = ROUTE_WEEKDAY_BY_SHOP.get(from_shop)
    d_to = ROUTE_WEEKDAY_BY_SHOP.get(to_shop)
    if d_from is not None and d_to is not None and d_from == d_to:
        return d_to
    if d_to is not None:
        return d_to
    if d_from is not None:
        return d_from
    return None


def build_shipment_plan(transfers_df: pd.DataFrame, base_date: pd.Timestamp) -> pd.DataFrame:
    if transfers_df.empty:
        return pd.DataFrame(
            columns=[
                "Article",
                "Size",
                "Qty",
                "From",
                "To",
                "Reason",
                "DispatchPolicy",
                "DispatchWeekday",
                "DispatchDate",
                "EtaDate",
                "ConsolidationStatus",
                "RouteCluster",
                "PlanningNote",
            ]
        )

    plan = transfers_df.copy()
    plan["From"] = plan["From"].astype(str).str.strip().str.upper().replace({"NU": "NV", "M2": "ME2"})
    plan["To"] = plan["To"].astype(str).str.strip().str.upper().replace({"NU": "NV", "M2": "ME2"})
    plan["Qty"] = pd.to_numeric(plan["Qty"], errors="coerce").fillna(0.0)

    pm_total_qty = float(plan.loc[(plan["From"].isin(PM_SHOPS)) | (plan["To"].isin(PM_SHOPS)), "Qty"].sum())
    sd_total_qty = float(plan.loc[(plan["From"].isin(SD_SHOPS)) | (plan["To"].isin(SD_SHOPS)), "Qty"].sum())

    rows = []
    for r in plan.itertuples(index=False):
        from_shop = str(r.From)
        to_shop = str(r.To)
        qty = float(r.Qty)

        policy = "FLEX_STANDARD"
        route_cluster = ""
        status = "PLANNED"
        note = ""
        dispatch_date = base_date.normalize() + pd.Timedelta(days=1)
        eta_date = dispatch_date + pd.Timedelta(days=1)

        if from_shop in PM_SHOPS or to_shop in PM_SHOPS:
            policy = "COURIER_2D_PM_CONSOLIDATED"
            if pm_total_qty < PM_MIN_CONSOLIDATION_QTY:
                status = "HOLD_ACCUMULATION"
                dispatch_date = pd.NaT
                eta_date = pd.NaT
                note = f"PM in attesa consolidamento >= {PM_MIN_CONSOLIDATION_QTY:.0f} paia."
            else:
                dispatch_date = _next_courier_2d_date(base_date)
                eta_date = dispatch_date + pd.Timedelta(days=2)
                note = "PM spedito con logica RM/EU a consolidamento raggiunto."
        elif from_shop in SD_SHOPS or to_shop in SD_SHOPS:
            policy = "SD_QTY_TRIGGER"
            if sd_total_qty < SD_MIN_CONSOLIDATION_QTY:
                status = "HOLD_QTY_TRIGGER"
                dispatch_date = pd.NaT
                eta_date = pd.NaT
                note = f"SD in attesa merce pronta >= {SD_MIN_CONSOLIDATION_QTY:.0f} paia."
            else:
                dispatch_date = base_date.normalize() + pd.Timedelta(days=1)
                eta_date = dispatch_date + pd.Timedelta(days=1)
                note = "SD rilasciato per soglia quantità pronta."
        elif from_shop in COURIER_2D_SHOPS or to_shop in COURIER_2D_SHOPS:
            policy = "COURIER_2D_PALLET"
            dispatch_date = _next_courier_2d_date(base_date)
            eta_date = dispatch_date + pd.Timedelta(days=2)
            note = "RM/EU corriere + bancali ogni 2 giorni."
        else:
            weekday = _resolve_route_weekday(from_shop, to_shop)
            if weekday is not None:
                policy = "ROUTE_WEEKLY"
                dispatch_date = _next_weekday_date(base_date, weekday)
                eta_date = dispatch_date + pd.Timedelta(days=1)
                route_cluster = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"][weekday]
                note = "Pianificato su giro settimanale negozi."
            else:
                policy = "FLEX_STANDARD"
                dispatch_date = base_date.normalize() + pd.Timedelta(days=1)
                eta_date = dispatch_date + pd.Timedelta(days=1)
                note = "Negozio fuori cluster fisso: instradamento flessibile."

        rows.append(
            {
                "Article": r.Article,
                "Size": r.Size,
                "Qty": qty,
                "From": from_shop,
                "To": to_shop,
                "Reason": r.Reason,
                "DispatchPolicy": policy,
                "DispatchWeekday": dispatch_date.day_name() if not pd.isna(dispatch_date) else "",
                "DispatchDate": dispatch_date.date().isoformat() if not pd.isna(dispatch_date) else "",
                "EtaDate": eta_date.date().isoformat() if not pd.isna(eta_date) else "",
                "ConsolidationStatus": status,
                "RouteCluster": route_cluster,
                "PlanningNote": note,
            }
        )

    return pd.DataFrame(rows)

def run_allocation_frames(
    sales: pd.DataFrame,
    articles: pd.DataFrame,
    shops_xlsx: Path,
    output_dir: Path,
    *,
    write_outputs: bool = True,
    demand_math_only: bool = True,
):
    sales = sales.copy()
    articles = articles.copy()
    shops = load_shops_xlsx(shops_xlsx)
    run_date = pd.Timestamp.today().normalize()
    if "snapshot_at" in sales.columns:
        ts = pd.to_datetime(sales["snapshot_at"], errors="coerce")
        if ts.notna().any():
            run_date = ts.dropna().max().normalize()

    (
        meta,
        demand_raw,
        periodo,
        sales_signal,
        stock,
        total,
        article_reparto,
        article_available_sizes,
    ) = build_lookup(articles, sales, shops)
    signal_frame = build_article_shop_transfer_signals(articles, sales, shop_meta=meta)
    signal_lookup = {
        (str(r.Article), str(r.Shop)): {
            "ObservedSalesSignal": float(getattr(r, "ObservedSalesSignal", 0.0) or 0.0),
            "NotebookVendutoSignal": float(getattr(r, "NotebookVendutoSignal", 0.0) or 0.0),
            "ReceiverEligibleBySales": bool(getattr(r, "ReceiverEligibleBySales", False)),
            "StockDepth": float(getattr(r, "StockDepth", 0.0) or 0.0),
            "ShopPriorityRank": int(getattr(r, "ShopPriorityRank", 99) or 99),
            "MissingCoreSizes": int(getattr(r, "MissingCoreSizes", 0) or 0),
            "CoreSizeCoverageRatio": float(getattr(r, "CoreSizeCoverageRatio", 0.0) or 0.0),
            "LowStockActiveCandidate": bool(getattr(r, "LowStockActiveCandidate", False)),
            "ZeroSalesSourceCandidate": bool(getattr(r, "ZeroSalesSourceCandidate", False)),
            "NotebookDestinationCandidate": bool(getattr(r, "NotebookDestinationCandidate", False)),
            "NotebookSourceCandidate": bool(getattr(r, "NotebookSourceCandidate", False)),
            "DoublePairsAvailable": float(getattr(r, "DoublePairsAvailable", 0.0) or 0.0),
            "HasDoublePairs": bool(getattr(r, "HasDoublePairs", False)),
            "DestinationPriorityScore": float(getattr(r, "DestinationPriorityScore", 0.0) or 0.0),
            "SourcePriorityScore": float(getattr(r, "SourcePriorityScore", 0.0) or 0.0),
        }
        for r in signal_frame.itertuples(index=False)
    }
    stock_integrity = validate_stock_snapshot_integrity(articles)
    demand, demand_diag = compute_hybrid_demand(
        sales,
        articles,
        meta,
        force_formula_only=bool(demand_math_only),
    )
    diag_lookup = {(r.Article, r.Shop): r for r in demand_diag.itertuples(index=False)}
    shop_total_stock, shop_capacity, shop_capacity_target = build_shop_capacity_state(meta, total)
    ops_in_budget, ops_out_budget = build_ops_budgets(meta, shop_total_stock, shop_capacity_target)
    ops_in_used = {s: 0.0 for s in shop_total_stock.keys()}
    ops_out_used = {s: 0.0 for s in shop_total_stock.keys()}

    # ── WAREHOUSE (M4) AUTO-INJECTION ─────────────────────────────────────────
    # Se M4 non compare nella lista negozi (lista-negozi.xlsx), lo aggiungiamo
    # come negozio virtuale con ruolo WAREHOUSE. In questo modo l'eccedenza di
    # stock da negozi con zero vendite viene consolidata a M4 invece di finire
    # all'ultimo outlet della lista (es. SD).
    m4_was_auto_injected = WAREHOUSE not in meta
    if m4_was_auto_injected:
        meta[WAREHOUSE] = {
            "Fascia": np.nan,
            "Mq": np.nan,
            "CapacityPairs": np.nan,
            "CapacitySource": "auto_injected",
            "CapacityStatus": np.nan,
        }
        shop_total_stock[WAREHOUSE] = shop_total_stock.get(WAREHOUSE, 0.0)
        shop_capacity[WAREHOUSE] = np.nan
        shop_capacity_target[WAREHOUSE] = float("inf")
        ops_in_budget[WAREHOUSE] = float("inf")
        ops_out_budget[WAREHOUSE] = float("inf")
        ops_in_used[WAREHOUSE] = 0.0
        ops_out_used[WAREHOUSE] = 0.0

    # Build list of (article,shop) present in stock
    article_to_shops = {}
    for (a, s) in stock.keys():
        article_to_shops.setdefault(a, set()).add(s)

    # Aggiungi WAREHOUSE a tutti gli articoli come destinazione virtuale
    # per la consolidazione dell'eccedenza.
    for a in list(article_to_shops.keys()):
        article_to_shops[a].add(WAREHOUSE)
        key_m4 = (a, WAREHOUSE)
        if key_m4 not in stock:
            stock[key_m4] = {sz: 0.0 for sz in SIZES}
            total[key_m4] = 0.0

    transfers = []
    capacity_blocks = 0
    ops_blocks = 0

    # Step: allocate to fill needs (fascia high first), donors from low priority & low periodo.
    for article, shops_set in article_to_shops.items():
        shops_for_article = sorted(list(shops_set))
        article_sizes = article_available_sizes.get(article) or list(SIZES)
        article_reparto_value = article_reparto.get(article)
        article_shops = [(article, s) for s in shops_for_article]
        targets, _presence = compute_targets(meta, demand, article_shops)

        # ── COVERAGE-FIRST RECEIVER BUILD ──────────────────────────────────────
        # Tutti i negozi con segnale vendite sono candidati ricevitori.
        # Nessun limite sul numero di taglie mancanti (copertura massima).
        receivers = []
        for s in shops_for_article:
            # WEB (canale online) escluso dalla logica di spostamento:
            # non riceve né cede merce fisica. Rimane visibile nel report.
            if s in EXCLUDE_SHOPS or s == WAREHOUSE or s == ONLINE:
                continue
            fascia = meta.get(s, {}).get("Fascia", np.nan)
            if is_outlet(fascia):
                continue
            role = role_for_shop(s)
            if role != "STORE":
                continue
            sig = signal_lookup.get((article, s), {})
            local_sales_signal = local_sales_signal_for_shop(sales_signal, signal_lookup, article, s)
            missing_core_sizes = int(sig.get("MissingCoreSizes", 0) or 0)
            # Un negozio è ricevitore se:
            # (a) NotebookDestinationCandidate=True  → vendite + stock basso (≤ threshold), oppure
            # (b) ha vendite attive E ha taglie core mancanti (anche se stock totale > threshold).
            # Il fallback nel sig.get non viene mai raggiunto perché
            # NotebookDestinationCandidate è sempre presente nel segnale: usiamo `or` esplicito.
            receiver_candidate = bool(
                sig.get("NotebookDestinationCandidate", False)
                or (
                    sig.get("ReceiverEligibleBySales", local_sales_signal > 0.0)
                    and missing_core_sizes > 0
                )
            )
            if not receiver_candidate:
                continue  # nessun segnale vendite o nessuna taglia mancante → salta
            current_total = float(total.get((article, s), 0.0) or 0.0)
            fascia_rank_s = int(float(fascia)) if not pd.isna(fascia) else 99
            receivers.append(
                {
                    "shop": s,
                    "fascia_rank": fascia_rank_s,
                    "sales_signal": local_sales_signal,
                    "has_presence": current_total > 0.0,
                    "current_total": current_total,
                    "low_stock_active": bool(sig.get("LowStockActiveCandidate", False)),
                    "notebook_destination": receiver_candidate,
                    "missing_core_sizes": missing_core_sizes,
                    "destination_priority_score": float(sig.get("DestinationPriorityScore", local_sales_signal)),
                }
            )

        # Ordina per fascia crescente (1 = più importante) poi per venduto decrescente.
        # I negozi più importanti ricevono per primi, poi si scende di fascia.
        receivers.sort(
            key=lambda item: (
                item["fascia_rank"],               # fascia 1 prima
                -item["sales_signal"],             # a parità di fascia: chi vende di più
                -item["destination_priority_score"],
                -item["missing_core_sizes"],       # più taglie mancanti = più bisogno
                item["shop"],
            )
        )

        outlet = pick_outlet(meta, shops_for_article, demand, sales_signal, article)

        if COVERAGE_FIRST_MODE:
            # ── LOGICA COVERAGE-FIRST ──────────────────────────────────────────
            # Itera PRIMA per taglia, POI per negozio in ordine di fascia.
            # Ogni taglia viene distribuita a TUTTI i negozi (in fascia order)
            # prima di passare alla taglia successiva.
            # Questo massimizza il numero di negozi coperti.

            # Precalcola le core sizes per ogni ricevitore
            req_sizes_by_recv: Dict[str, List[int]] = {}
            for rm in receivers:
                req_sizes_by_recv[rm["shop"]] = required_run_sizes(
                    meta.get(rm["shop"], {}).get("Fascia", np.nan),
                    reparto=article_reparto_value,
                    available_sizes=article_sizes,
                )

            # Priorità taglie: core sizes prima (unione di tutti i negozi), poi le altre
            all_core_sizes: set = set()
            for sl in req_sizes_by_recv.values():
                all_core_sizes.update(sl)
            size_priority = sorted(all_core_sizes) + [sz for sz in article_sizes if sz not in all_core_sizes]

            # Pass 1 – distribuisci 1 pezzo per taglia per negozio in ordine fascia
            for sz in size_priority:
                for recv_meta in receivers:
                    recv = recv_meta["shop"]
                    if stock[(article, recv)].get(sz, 0.0) >= 1.0:
                        continue  # già coperta
                    if free_capacity(recv, shop_total_stock, shop_capacity_target) < 1.0:
                        capacity_blocks += 1
                        continue
                    if not ops_budget_allows_move(recv, ops_in_used, ops_in_budget):
                        ops_blocks += 1
                        continue
                    donors_for_recv = prioritized_donors_for_size(
                        meta, stock, total, demand, sales_signal, signal_lookup,
                        article, recv, sz, shops_for_article,
                        reparto=article_reparto_value, available_sizes=article_sizes,
                    )
                    for donor_meta in donors_for_recv:
                        donor = donor_meta["shop"]
                        if donor == recv:
                            continue
                        if not ops_budget_allows_move(donor, ops_out_used, ops_out_budget):
                            continue
                        if stock[(article, donor)].get(sz, 0.0) >= 1.0:
                            reason = {
                                "warehouse": "Coverage-first: taglia da M4",
                                "duplicate": "Coverage-first: taglia da stock doppio",
                                "single": "Coverage-first: taglia da negozio bassa fascia",
                            }.get(donor_meta.get("stage"), "Coverage-first: fill taglia")
                            apply_transfer_move(
                                stock, total, shop_total_stock,
                                ops_out_used, ops_in_used, transfers,
                                article, sz, donor, recv, reason,
                            )
                            break  # passa al prossimo negozio per questa taglia

            # Pass 2 – top-up opzionale (solo se abilitato)
            if ENABLE_TOP_UP_TO_TARGET:
                for recv_meta in receivers:
                    recv = recv_meta["shop"]
                    need_left = int(max(0.0, targets[(article, recv)] - total.get((article, recv), 0.0)))
                    free = free_capacity(recv, shop_total_stock, shop_capacity_target)
                    if not np.isinf(free):
                        need_left = min(need_left, max(0, int(np.floor(free))))
                    if need_left <= 0:
                        continue
                    for sz in size_priority:
                        if need_left <= 0:
                            break
                        if free_capacity(recv, shop_total_stock, shop_capacity_target) < 1.0:
                            capacity_blocks += 1
                            break
                        donors_for_recv = prioritized_donors_for_size(
                            meta, stock, total, demand, sales_signal, signal_lookup,
                            article, recv, sz, shops_for_article,
                            reparto=article_reparto_value, available_sizes=article_sizes,
                        )
                        for donor_meta in donors_for_recv:
                            donor = donor_meta["shop"]
                            if donor == recv:
                                continue
                            if not ops_budget_allows_move(recv, ops_in_used, ops_in_budget):
                                ops_blocks += 1
                                break
                            if not ops_budget_allows_move(donor, ops_out_used, ops_out_budget):
                                continue
                            if stock[(article, donor)].get(sz, 0.0) >= 1.0:
                                reason = {
                                    "warehouse": "Top-up da M4",
                                    "duplicate": "Top-up da stock doppio",
                                    "single": "Top-up da negozio bassa fascia",
                                }.get(donor_meta.get("stage"), "Top-up")
                                apply_transfer_move(
                                    stock, total, shop_total_stock,
                                    ops_out_used, ops_in_used, transfers,
                                    article, sz, donor, recv, reason,
                                )
                                need_left -= 1
                                break

        else:
            # ── LOGICA LEGACY (receiver-first) ────────────────────────────────
            for recv_meta in receivers:
                recv = recv_meta["shop"]
                if free_capacity(recv, shop_total_stock, shop_capacity_target) < 1.0:
                    capacity_blocks += 1
                    continue
                fascia_recv = meta.get(recv, {}).get("Fascia", np.nan)
                req_sizes = required_run_sizes(
                    fascia_recv, reparto=article_reparto_value, available_sizes=article_sizes,
                )
                for sz in req_sizes:
                    if free_capacity(recv, shop_total_stock, shop_capacity_target) < 1.0:
                        capacity_blocks += 1
                        break
                    if stock[(article, recv)].get(sz, 0.0) >= 1.0:
                        continue
                    donors_for_recv = prioritized_donors_for_size(
                        meta, stock, total, demand, sales_signal, signal_lookup,
                        article, recv, sz, shops_for_article,
                        reparto=article_reparto_value, available_sizes=article_sizes,
                    )
                    moved = False
                    for donor_meta in donors_for_recv:
                        donor = donor_meta["shop"]
                        if donor == recv:
                            continue
                        if not ops_budget_allows_move(recv, ops_in_used, ops_in_budget):
                            ops_blocks += 1
                            break
                        if not ops_budget_allows_move(donor, ops_out_used, ops_out_budget):
                            continue
                        if stock[(article, donor)].get(sz, 0.0) >= 1.0:
                            apply_transfer_move(
                                stock, total, shop_total_stock,
                                ops_out_used, ops_in_used, transfers,
                                article, sz, donor, recv,
                                "Fill required run (legacy)",
                            )
                            moved = True
                            break
                    if not moved:
                        break

        # ── CONSOLIDAMENTO ECCEDENZA + REDISTRIBUZIONE FASCIA-ALTA ───────────────
        # Per negozi con ZERO vendite sull'articolo e qty >= 2 su una taglia:
        #
        # 1. PRIMA OPZIONE — negozio di fascia migliore con buco
        #    Sia per taglie CORE che NON-CORE: se esiste un negozio di fascia
        #    migliore (rank inferiore) con vendite attive e un buco su quella
        #    taglia, il paio in eccesso va lì invece che a M4.
        #    → si lascia sempre almeno 1 paio al negozio donatore.
        #
        # 2. SECONDA OPZIONE — M4 (fallback, solo taglie NON-CORE)
        #    Se nessun negozio di fascia migliore ha bisogno della taglia,
        #    le taglie NON-CORE in eccesso vengono consolidate a M4.
        #    Le taglie CORE restano al negozio (non vanno a M4).
        #
        # Questo evita il doppio giro del camion: invece di mandare a M4 e poi
        # ri-spedire quando serve, distribuiamo direttamente dove c'è bisogno.
        consolidation_target = WAREHOUSE if WAREHOUSE in meta else outlet
        for s in list(shops_for_article):
            if s in EXCLUDE_SHOPS or s in (WAREHOUSE, ONLINE):
                continue
            fascia_s = meta.get(s, {}).get("Fascia", np.nan)
            if is_outlet(fascia_s):
                continue
            if not should_reallocate_broken_line(meta, sales_signal, signal_lookup, article, s):
                continue
            req_sizes_s = required_run_sizes(
                fascia_s, reparto=article_reparto_value, available_sizes=article_sizes,
            )
            donor_fascia_rank_s = fascia_rank_value(fascia_s)

            for sz in article_sizes:
                if float(stock.get((article, s), {}).get(sz, 0.0) or 0.0) < 2.0:
                    continue  # serve almeno un duplicato per cedere (si lascia 1)
                is_core = sz in req_sizes_s

                # Fase A: invia l'eccedenza a negozi di fascia migliore con buco
                while stock.get((article, s), {}).get(sz, 0.0) >= 2.0:
                    if not ops_budget_allows_move(s, ops_out_used, ops_out_budget):
                        break
                    gap_recvs = _find_excess_gap_receivers(
                        meta, stock, sales_signal, signal_lookup,
                        article, s, sz, shops_for_article,
                        shop_total_stock=shop_total_stock,
                        shop_capacity_target=shop_capacity_target,
                        donor_fascia_rank=donor_fascia_rank_s,
                    )
                    moved = False
                    for recv_info in gap_recvs:
                        recv = recv_info["shop"]
                        if ops_budget_allows_move(recv, ops_in_used, ops_in_budget):
                            apply_transfer_move(
                                stock, total, shop_total_stock,
                                ops_out_used, ops_in_used, transfers,
                                article, sz, s, recv,
                                "Redistribuzione fascia-alta: eccesso a negozio con buco",
                            )
                            moved = True
                            break
                    if not moved:
                        break  # nessun candidato disponibile → esci

                # Fase B: eccedenza residua (solo taglie NON-CORE) → M4 come fallback
                if not is_core and consolidation_target:
                    while (
                        stock.get((article, s), {}).get(sz, 0.0) >= 2.0
                        and free_capacity(consolidation_target, shop_total_stock, shop_capacity_target) >= 1.0
                        and ops_budget_allows_move(s, ops_out_used, ops_out_budget)
                        and ops_budget_allows_move(consolidation_target, ops_in_used, ops_in_budget)
                    ):
                        reason = (
                            "Consolidamento M4: non-core eccesso"
                            if consolidation_target == WAREHOUSE
                            else "Outlet: duplicato non-core"
                        )
                        apply_transfer_move(
                            stock, total, shop_total_stock,
                            ops_out_used, ops_in_used, transfers,
                            article, sz, s, consolidation_target,
                            reason,
                        )
                # Taglie CORE residue (dopo fase A): restano al negozio, non a M4.

            if not ops_budget_allows_move(s, ops_out_used, ops_out_budget):
                ops_blocks += 1

    detailed_df = pd.DataFrame(transfers)

    if detailed_df.empty:
        transfers_df = detailed_df
    else:
        transfers_df = (
            detailed_df.groupby(["Article", "Size", "From", "To", "Reason"], as_index=False)["Qty"]
            .sum()
            .sort_values(["Article", "From", "To", "Size"])
        )

    shipment_plan = build_shipment_plan(transfers_df, run_date)
    if shipment_plan.empty:
        shipment_summary = shipment_plan
    else:
        shipment_summary = (
            shipment_plan.groupby(
                ["DispatchDate", "DispatchWeekday", "DispatchPolicy", "ConsolidationStatus", "From", "To"],
                as_index=False,
            )["Qty"]
            .sum()
            .sort_values(["DispatchDate", "DispatchPolicy", "From", "To"])
        )

    # Build features export (fast)
    rows = []
    for (article, shop), sizes in stock.items():
        if shop in EXCLUDE_SHOPS:
            continue
        # Salta le righe M4 iniettate artificialmente senza stock reale
        if m4_was_auto_injected and shop == WAREHOUSE and total.get((article, shop), 0.0) == 0.0:
            continue
        fascia = meta.get(shop, {}).get("Fascia", np.nan)
        cap = shop_capacity.get(shop, np.nan)
        cap_target = shop_capacity_target.get(shop, float("inf"))
        cap_target = np.nan if np.isinf(cap_target) else cap_target
        cap_free = free_capacity(shop, shop_total_stock, shop_capacity_target)
        cap_free = np.nan if np.isinf(cap_free) else cap_free
        diag = diag_lookup.get((article, shop))
        demand_rule = float(getattr(diag, "DemandRule", np.nan)) if diag is not None else np.nan
        demand_ai = float(getattr(diag, "DemandAI", np.nan)) if diag is not None else np.nan
        demand_blend = float(getattr(diag, "DemandBlendWeight", 0.0)) if diag is not None else 0.0
        demand_hybrid = float(getattr(diag, "DemandHybrid", demand.get((article, shop), 0.0))) if diag is not None else demand.get((article, shop), 0.0)
        demand_mode = str(getattr(diag, "DemandModelMode", "formula_only")) if diag is not None else "formula_only"
        demand_r2 = float(getattr(diag, "DemandModelQualityR2", 0.0)) if diag is not None else 0.0
        sig = signal_lookup.get((article, shop), {})
        rows.append(
            {
                "Article": article,
                "Reparto": article_reparto.get(article, ""),
                "Shop": shop,
                "Fascia": fascia,
                "IsOutlet": is_outlet(fascia),
                "Role": role_for_shop(shop),
                "DemandRaw": demand_raw.get((article, shop), 0.0),
                "DemandRule": demand_rule,
                "DemandAI": demand_ai,
                "DemandBlendWeight": demand_blend,
                "DemandHybrid": demand_hybrid,
                "DemandModelMode": demand_mode,
                "DemandModelQualityR2": demand_r2,
                "Periodo_Qty": periodo.get((article, shop), 0.0),
                "ObservedSalesSignal": sig.get("ObservedSalesSignal", sales_signal.get((article, shop), 0.0)),
                "NotebookVendutoSignal": sig.get("NotebookVendutoSignal", sig.get("ObservedSalesSignal", sales_signal.get((article, shop), 0.0))),
                "ReceiverEligibleBySales": sig.get("ReceiverEligibleBySales", sales_signal.get((article, shop), 0.0) > 0.0),
                "StockDepth": sig.get("StockDepth", total.get((article, shop), 0.0)),
                "ShopPriorityRank": sig.get("ShopPriorityRank", int(float(fascia)) if not pd.isna(fascia) else 99),
                "MissingCoreSizes": sig.get("MissingCoreSizes", 0),
                "CoreSizeCoverageRatio": sig.get("CoreSizeCoverageRatio", 0.0),
                "LowStockActiveCandidate": sig.get("LowStockActiveCandidate", False),
                "ZeroSalesSourceCandidate": sig.get("ZeroSalesSourceCandidate", False),
                "NotebookDestinationCandidate": sig.get("NotebookDestinationCandidate", False),
                "NotebookSourceCandidate": sig.get("NotebookSourceCandidate", False),
                "DoublePairsAvailable": sig.get("DoublePairsAvailable", 0.0),
                "HasDoublePairs": sig.get("HasDoublePairs", False),
                "DestinationPriorityScore": sig.get("DestinationPriorityScore", 0.0),
                "SourcePriorityScore": sig.get("SourcePriorityScore", 0.0),
                "Stock_after": total.get((article, shop), 0.0),
                "ShopCapacityPairs": cap,
                "ShopCapacityTarget": cap_target,
                "ShopFreeCapacityAfter": cap_free,
                "ShopCapacitySource": meta.get(shop, {}).get("CapacitySource", "none"),
                "CapacityBlockedMoves": capacity_blocks,
                "OpsBlockedMoves": ops_blocks,
                "ShopInboundBudget": ops_in_budget.get(shop, 0.0),
                "ShopOutboundBudget": ops_out_budget.get(shop, 0.0),
                "ShopInboundUsed": ops_in_used.get(shop, 0.0),
                "ShopOutboundUsed": ops_out_used.get(shop, 0.0),
                **{f"Size_{s}": sizes.get(s, 0.0) for s in SIZES},
            }
        )
    feat = pd.DataFrame(rows)
    if write_outputs:
        output_dir.mkdir(parents=True, exist_ok=True)
        demand_diag.to_csv(output_dir / "demand_diagnostics.csv", index=False)
        detailed_df.to_csv(output_dir / "suggested_transfers_detailed.csv", index=False)
        transfers_df.to_csv(output_dir / "suggested_transfers.csv", index=False)
        shipment_plan.to_csv(output_dir / "shipment_plan.csv", index=False)
        shipment_summary.to_csv(output_dir / "shipment_summary.csv", index=False)
        feat.to_csv(output_dir / "features_after.csv", index=False)
        signal_frame.to_csv(output_dir / "article_shop_signals.csv", index=False)
        stock_integrity.to_csv(output_dir / "stock_integrity_report.csv", index=False)

    return {
        "transfers": transfers_df,
        "transfers_detailed": detailed_df,
        "features": feat,
        "transfer_signals": signal_frame,
        "stock_integrity": stock_integrity,
        "shipment_plan": shipment_plan,
        "shipment_summary": shipment_summary,
        "demand_diagnostics": demand_diag,
        "run_date": run_date,
    }


def run_allocation(clean_sales_csv: Path, clean_articles_csv: Path, shops_xlsx: Path, output_dir: Path):
    sales = pd.read_csv(clean_sales_csv)
    articles = pd.read_csv(clean_articles_csv)
    return run_allocation_frames(sales, articles, shops_xlsx, output_dir, write_outputs=True)

if __name__ == "__main__":
    root = Path(__file__).resolve().parent.parent
    cfg = root / "config" / "lista-negozi_integrato.xlsx"
    if not cfg.exists():
        cfg = root / "config" / "lista-negozi.xlsx"
    run_allocation(root / "output" / "clean_sales.csv", root / "output" / "clean_articles.csv", cfg, root / "output")
