
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np
from hybrid_demand import compute_hybrid_demand

SIZES = [35, 36, 37, 38, 39, 40, 41, 42]
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
    xls = pd.ExcelFile(path)
    preferred = ["lista_negozi_linked", "capacita_stimate_by_sigla", "capacita_stimate_by_store"]
    ordered_sheets = [s for s in preferred if s in xls.sheet_names]
    ordered_sheets.extend([s for s in xls.sheet_names if s not in ordered_sheets])

    for sheet in ordered_sheets:
        df = pd.read_excel(path, sheet_name=sheet)
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


def required_run_sizes(fascia: Any) -> List[int]:
    if pd.isna(fascia):
        return [37, 38, 39]
    f = int(float(fascia))
    return [36, 37, 38, 39, 40] if f in (1, 2) else [37, 38, 39]

def build_lookup(articles: pd.DataFrame, sales: pd.DataFrame, shops: pd.DataFrame):
    # Normalize
    articles = articles.copy()
    sales = sales.copy()
    articles["Shop"] = _normalize_shop(articles["Shop"])
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

    # stock per (article, shop) sizes
    stock = {}
    total = {}
    for r in articles.itertuples(index=False):
        if r.Shop in EXCLUDE_SHOPS:
            continue
        key = (r.Article, r.Shop)
        sizes = {}
        for s in SIZES:
            qty = pd.to_numeric(getattr(r, f"Size_{s}", 0.0), errors="coerce")
            qty = 0.0 if pd.isna(qty) else float(qty)
            sizes[s] = max(0.0, qty)
        stock[key] = sizes
        # Keep total aligned to transferable size buckets to avoid negative drift.
        total[key] = float(sum(sizes.values()))

    return meta, demand, periodo, stock, total


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


def pick_outlet(meta, shops_for_article, demand, article):
    outlets = [s for s in shops_for_article if is_outlet(meta.get(s, {}).get("Fascia", np.nan))]
    if not outlets:
        # fallback: any outlet in meta
        outlets = [s for s, m in meta.items() if is_outlet(m.get("Fascia", np.nan))]
    if not outlets:
        return None
    outlets.sort(
        key=lambda s: (demand.get((article, s), 0.0), fascia_weight(meta.get(s, {}).get("Fascia", np.nan))),
        reverse=True,
    )
    return outlets[0]


def can_make_run(meta, stock, article, shop):
    req = required_run_sizes(meta.get(shop, {}).get("Fascia", np.nan))
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

def run_allocation(clean_sales_csv: Path, clean_articles_csv: Path, shops_xlsx: Path, output_dir: Path):
    sales = pd.read_csv(clean_sales_csv)
    articles = pd.read_csv(clean_articles_csv)
    shops = load_shops_xlsx(shops_xlsx)
    run_date = pd.Timestamp.today().normalize()
    if "snapshot_at" in sales.columns:
        ts = pd.to_datetime(sales["snapshot_at"], errors="coerce")
        if ts.notna().any():
            run_date = ts.dropna().max().normalize()

    meta, demand_raw, periodo, stock, total = build_lookup(articles, sales, shops)
    demand, demand_diag = compute_hybrid_demand(sales, articles, meta)
    diag_lookup = {(r.Article, r.Shop): r for r in demand_diag.itertuples(index=False)}
    shop_total_stock, shop_capacity, shop_capacity_target = build_shop_capacity_state(meta, total)
    ops_in_budget, ops_out_budget = build_ops_budgets(meta, shop_total_stock, shop_capacity_target)
    ops_in_used = {s: 0.0 for s in shop_total_stock.keys()}
    ops_out_used = {s: 0.0 for s in shop_total_stock.keys()}

    # Build list of (article,shop) present in stock
    article_to_shops = {}
    for (a, s) in stock.keys():
        article_to_shops.setdefault(a, set()).add(s)

    transfers = []
    capacity_blocks = 0
    ops_blocks = 0

    # Step: allocate to fill needs (fascia high first), donors from low priority & low periodo.
    for article, shops_set in article_to_shops.items():
        shops_for_article = sorted(list(shops_set))
        article_shops = [(article, s) for s in shops_for_article]
        targets, _presence = compute_targets(meta, demand, article_shops)

        # receivers: stores + online (not outlets), ordered by fascia asc then need desc
        receivers = []
        for s in shops_for_article:
            if s in EXCLUDE_SHOPS or s == WAREHOUSE:
                continue
            fascia = meta.get(s, {}).get("Fascia", np.nan)
            if is_outlet(fascia):
                continue
            role = role_for_shop(s)
            if role in ("ONLINE", "STORE"):
                need = max(0.0, targets[(article, s)] - total.get((article, s), 0.0))
                if need <= 0:
                    continue
                free = free_capacity(s, shop_total_stock, shop_capacity_target)
                if not np.isinf(free):
                    need = min(need, float(max(0, int(np.floor(free)))))
                if need <= 0:
                    capacity_blocks += 1
                    continue
                receivers.append((int(float(fascia)) if not pd.isna(fascia) else 99, -need, s))
        receivers.sort()

        # donors candidates (stores/outlets/warehouse), online never donates
        donors = []
        for s in shops_for_article:
            if s in EXCLUDE_SHOPS or s == ONLINE:
                continue
            if (article, s) not in total:
                continue
            fascia = meta.get(s, {}).get("Fascia", np.nan)
            # donor priority: higher fascia number first (lower priority shop), then low periodo
            donors.append((int(float(fascia)) if not pd.isna(fascia) else 99, periodo.get((article, s), 0.0), s))
        donors.sort(key=lambda x: (-x[0], x[1]))
        online_donors = sorted(donors, key=lambda x: (0 if x[2] == WAREHOUSE else 1, -x[0], x[1]))

        outlet = pick_outlet(meta, shops_for_article, demand, article)

        for _, _, recv in receivers:
            if free_capacity(recv, shop_total_stock, shop_capacity_target) < 1.0:
                capacity_blocks += 1
                continue

            fascia_recv = meta.get(recv, {}).get("Fascia", np.nan)
            req_sizes = required_run_sizes(fascia_recv)

            # fill required sizes first if missing
            for sz in req_sizes:
                if free_capacity(recv, shop_total_stock, shop_capacity_target) < 1.0:
                    capacity_blocks += 1
                    break
                if stock[(article, recv)].get(sz, 0.0) >= 1.0:
                    continue

                moved = False
                donors_for_recv = online_donors if recv == ONLINE else donors
                for _, _, donor in donors_for_recv:
                    if donor == recv:
                        continue
                    if ops_budget_left(recv, ops_in_used, ops_in_budget) < 1.0:
                        ops_blocks += 1
                        break
                    if ops_budget_left(donor, ops_out_used, ops_out_budget) < 1.0:
                        continue
                    if donor != WAREHOUSE:
                        keep = donor_keep_min(meta, demand, article, donor)
                        if total[(article, donor)] <= keep:
                            continue
                    if stock[(article, donor)].get(sz, 0.0) >= 1.0:
                        stock[(article,donor)][sz] -= 1.0
                        stock[(article,recv)][sz] = stock[(article,recv)].get(sz,0.0) + 1.0
                        total[(article,donor)] -= 1.0
                        total[(article,recv)] = total.get((article,recv),0.0) + 1.0
                        shop_total_stock[donor] = shop_total_stock.get(donor, 0.0) - 1.0
                        shop_total_stock[recv] = shop_total_stock.get(recv, 0.0) + 1.0
                        ops_out_used[donor] = ops_out_used.get(donor, 0.0) + 1.0
                        ops_in_used[recv] = ops_in_used.get(recv, 0.0) + 1.0
                        transfers.append(
                            {"Article": article, "Size": sz, "Qty": 1, "From": donor, "To": recv, "Reason": "Fill required run"}
                        )
                        moved = True
                        break
                if not moved:
                    # cannot fill this size for this receiver -> stop filling run on this receiver
                    break

            # after required fill, top-up towards target (micro)
            need_left = int(max(0.0, targets[(article, recv)] - total.get((article, recv), 0.0)))
            free = free_capacity(recv, shop_total_stock, shop_capacity_target)
            if not np.isinf(free):
                need_left = min(need_left, max(0, int(np.floor(free))))
            if need_left <= 0:
                continue

            # fill central core then rest
            pri = req_sizes + [s for s in SIZES if s not in req_sizes]
            for sz in pri:
                if need_left <= 0:
                    break
                if free_capacity(recv, shop_total_stock, shop_capacity_target) < 1.0:
                    capacity_blocks += 1
                    break
                donors_for_recv = online_donors if recv == ONLINE else donors
                for _, _, donor in donors_for_recv:
                    if donor == recv:
                        continue
                    if ops_budget_left(recv, ops_in_used, ops_in_budget) < 1.0:
                        ops_blocks += 1
                        break
                    if ops_budget_left(donor, ops_out_used, ops_out_budget) < 1.0:
                        continue
                    if donor != WAREHOUSE:
                        keep = donor_keep_min(meta, demand, article, donor)
                        if total[(article, donor)] <= keep:
                            continue
                    if stock[(article, donor)].get(sz, 0.0) >= 1.0:
                        stock[(article,donor)][sz] -= 1.0
                        stock[(article,recv)][sz] = stock[(article,recv)].get(sz,0.0) + 1.0
                        total[(article,donor)] -= 1.0
                        total[(article,recv)] = total.get((article,recv),0.0) + 1.0
                        shop_total_stock[donor] = shop_total_stock.get(donor, 0.0) - 1.0
                        shop_total_stock[recv] = shop_total_stock.get(recv, 0.0) + 1.0
                        ops_out_used[donor] = ops_out_used.get(donor, 0.0) + 1.0
                        ops_in_used[recv] = ops_in_used.get(recv, 0.0) + 1.0
                        transfers.append(
                            {"Article": article, "Size": sz, "Qty": 1, "From": donor, "To": recv, "Reason": "Top-up to target"}
                        )
                        need_left -= 1
                        break

        # Outlet fallback: if store cannot make run, drain extras to outlet
        if outlet:
            for s in list(shops_for_article):
                if s in EXCLUDE_SHOPS or s in (WAREHOUSE, ONLINE):
                    continue
                fascia_s = meta.get(s, {}).get("Fascia", np.nan)
                if is_outlet(fascia_s):
                    continue
                if (article, s) not in total:
                    continue
                if total[(article, s)] <= 0:
                    continue
                if can_make_run(meta, stock, article, s):
                    continue

                keep = donor_keep_min(meta, demand, article, s)
                req = required_run_sizes(fascia_s)
                order = [x for x in SIZES if x not in req] + req
                for sz in order:
                    while (
                        total[(article, s)] > keep
                        and stock[(article, s)].get(sz, 0.0) >= 1.0
                        and free_capacity(outlet, shop_total_stock, shop_capacity_target) >= 1.0
                        and ops_budget_left(s, ops_out_used, ops_out_budget) >= 1.0
                        and ops_budget_left(outlet, ops_in_used, ops_in_budget) >= 1.0
                    ):
                        stock[(article, s)][sz] -= 1.0
                        stock.setdefault((article, outlet), {}).setdefault(sz, 0.0)
                        stock[(article, outlet)][sz] = stock[(article, outlet)].get(sz, 0.0) + 1.0
                        total[(article, s)] -= 1.0
                        total[(article, outlet)] = total.get((article, outlet), 0.0) + 1.0
                        shop_total_stock[s] = shop_total_stock.get(s, 0.0) - 1.0
                        shop_total_stock[outlet] = shop_total_stock.get(outlet, 0.0) + 1.0
                        ops_out_used[s] = ops_out_used.get(s, 0.0) + 1.0
                        ops_in_used[outlet] = ops_in_used.get(outlet, 0.0) + 1.0
                        transfers.append(
                            {
                                "Article": article,
                                "Size": sz,
                                "Qty": 1,
                                "From": s,
                                "To": outlet,
                                "Reason": "Fallback to outlet (unfillable run)",
                            }
                        )
                    if ops_budget_left(s, ops_out_used, ops_out_budget) < 1.0 or ops_budget_left(outlet, ops_in_used, ops_in_budget) < 1.0:
                        ops_blocks += 1

    output_dir.mkdir(parents=True, exist_ok=True)
    demand_diag.to_csv(output_dir / "demand_diagnostics.csv", index=False)
    detailed_df = pd.DataFrame(transfers)
    detailed_df.to_csv(output_dir / "suggested_transfers_detailed.csv", index=False)

    if detailed_df.empty:
        transfers_df = detailed_df
    else:
        transfers_df = (
            detailed_df.groupby(["Article", "Size", "From", "To", "Reason"], as_index=False)["Qty"]
            .sum()
            .sort_values(["Article", "From", "To", "Size"])
        )
    transfers_df.to_csv(output_dir / "suggested_transfers.csv", index=False)

    shipment_plan = build_shipment_plan(transfers_df, run_date)
    shipment_plan.to_csv(output_dir / "shipment_plan.csv", index=False)
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
    shipment_summary.to_csv(output_dir / "shipment_summary.csv", index=False)

    # Build features export (fast)
    rows = []
    for (article, shop), sizes in stock.items():
        if shop in EXCLUDE_SHOPS:
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
        rows.append(
            {
                "Article": article,
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
    feat.to_csv(output_dir / "features_after.csv", index=False)

    return transfers_df, feat

if __name__ == "__main__":
    root = Path(__file__).resolve().parent
    cfg = root / "config" / "lista-negozi_integrato.xlsx"
    if not cfg.exists():
        cfg = root / "config" / "lista-negozi.xlsx"
    run_allocation(root / "output" / "clean_sales.csv", root / "output" / "clean_articles.csv", cfg, root / "output")
