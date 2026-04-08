from __future__ import annotations
from typing import Dict, Tuple
import pandas as pd
import numpy as np

AI_MIN_ROWS = 120
AI_BLEND_MAX = 0.45
RIDGE_LAMBDA = 12.0


def _norm_shop(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.upper().replace({"W": "WEB", "NU": "NV", "M2": "ME2"})


def _safe_num(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(np.zeros(len(df)), index=df.index, dtype=float)
    s = pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return s


def _service_factor(fascia: float) -> float:
    if pd.isna(fascia):
        return 1.00
    f = int(float(fascia))
    return {1: 1.20, 2: 1.15, 3: 1.10, 4: 1.06, 5: 1.03, 6: 1.00, 7: 0.98}.get(f, 1.00)


def _build_fold_ids(df: pd.DataFrame, n_folds: int = 5) -> np.ndarray:
    keys = df["Article"].astype(str) + "|" + df["Shop"].astype(str)
    h = pd.util.hash_pandas_object(keys, index=False).astype(np.int64)
    return (h % n_folds).values


def _ridge_oof_predict(x: np.ndarray, y: np.ndarray, fold_ids: np.ndarray, lam: float) -> Tuple[np.ndarray, float, str]:
    x = np.nan_to_num(x, nan=0.0, posinf=1e6, neginf=0.0)
    y = np.nan_to_num(y, nan=0.0, posinf=14.0, neginf=0.0)

    n = len(y)
    if n == 0:
        return np.array([]), 0.0, "empty"
    if n < AI_MIN_ROWS or float(np.var(y)) < 1e-9:
        mean_pred = np.full(n, float(np.mean(y)))
        return mean_pred, 0.0, "formula_only"

    unique_folds = np.unique(fold_ids)
    oof = np.zeros(n, dtype=float)

    for fold in unique_folds:
        train_mask = fold_ids != fold
        val_mask = fold_ids == fold
        if train_mask.sum() < 20 or val_mask.sum() == 0:
            oof[val_mask] = float(np.mean(y[train_mask])) if train_mask.any() else float(np.mean(y))
            continue

        x_train = x[train_mask]
        y_train = y[train_mask]
        x_val = x[val_mask]

        mu = x_train.mean(axis=0)
        sigma = x_train.std(axis=0)
        sigma[sigma < 1e-9] = 1.0

        x_train_n = (x_train - mu) / sigma
        x_val_n = (x_val - mu) / sigma

        x_train_d = np.hstack([np.ones((x_train_n.shape[0], 1)), x_train_n])
        x_val_d = np.hstack([np.ones((x_val_n.shape[0], 1)), x_val_n])

        reg = np.eye(x_train_d.shape[1], dtype=float) * lam
        reg[0, 0] = 0.0  # no penalty on intercept

        lhs = x_train_d.T @ x_train_d + reg
        rhs = x_train_d.T @ y_train
        try:
            beta = np.linalg.solve(lhs, rhs)
        except np.linalg.LinAlgError:
            beta = np.linalg.pinv(lhs) @ rhs

        oof[val_mask] = x_val_d @ beta

    y_mean = float(np.mean(y))
    ss_res = float(np.sum((y - oof) ** 2))
    ss_tot = float(np.sum((y - y_mean) ** 2))
    r2 = 0.0 if ss_tot <= 1e-9 else max(0.0, 1.0 - (ss_res / ss_tot))
    return oof, r2, "ridge_oof_blend"


def compute_hybrid_demand(
    sales: pd.DataFrame, articles: pd.DataFrame, meta: Dict[str, Dict[str, float]]
) -> Tuple[Dict[Tuple[str, str], float], pd.DataFrame]:
    sales = sales.copy()
    articles = articles.copy()
    sales["Shop"] = _norm_shop(sales["Shop"])
    articles["Shop"] = _norm_shop(articles["Shop"])

    sales_keys = sales[["Article", "Shop"]].drop_duplicates()
    article_keys = articles[["Article", "Shop"]].drop_duplicates()
    grid = pd.concat([sales_keys, article_keys], ignore_index=True).drop_duplicates()

    sales_agg = (
        sales.groupby(["Article", "Shop"], as_index=False)
        .agg(
            {
                "Periodo_Qty": "sum",
                "Venduto_Qty": "sum",
                "Consegnato_Qty": "sum",
                "Sellout_Clamped": "mean",
                "Sellout_Percent": "mean",
            }
        )
    )

    size_cols = [c for c in articles.columns if c.startswith("Size_")]
    if size_cols:
        stock_agg = articles.groupby(["Article", "Shop"], as_index=False)[size_cols].sum()
        stock_agg["StockNow"] = stock_agg[size_cols].sum(axis=1)
        stock_agg["SizeDepth"] = (stock_agg[size_cols] > 0).sum(axis=1).astype(float)
        stock_agg = stock_agg[["Article", "Shop", "StockNow", "SizeDepth"]]
    else:
        stock_agg = articles.groupby(["Article", "Shop"], as_index=False).agg({"Giacenza": "sum"})
        stock_agg["StockNow"] = _safe_num(stock_agg, "Giacenza")
        stock_agg["SizeDepth"] = np.where(stock_agg["StockNow"] > 0, 1.0, 0.0)
        stock_agg = stock_agg[["Article", "Shop", "StockNow", "SizeDepth"]]

    df = grid.merge(sales_agg, on=["Article", "Shop"], how="left").merge(stock_agg, on=["Article", "Shop"], how="left")
    for c in ["Periodo_Qty", "Venduto_Qty", "Consegnato_Qty", "Sellout_Clamped", "Sellout_Percent", "StockNow", "SizeDepth"]:
        df[c] = _safe_num(df, c)
    df["SelloutUsed"] = np.where(df["Sellout_Clamped"] > 0, df["Sellout_Clamped"], df["Sellout_Percent"])
    df["SelloutUsed"] = df["SelloutUsed"].clip(lower=0.0, upper=250.0)

    df["Fascia"] = df["Shop"].map(lambda s: meta.get(s, {}).get("Fascia", np.nan))
    df["FasciaNum"] = pd.to_numeric(df["Fascia"], errors="coerce").fillna(5.0)
    df["ServiceFactor"] = df["Fascia"].map(_service_factor).fillna(1.0)

    # Hierarchical priors for sparse observations (leave-one-out to reduce leakage).
    art_sum = df.groupby("Article")["Periodo_Qty"].transform("sum")
    art_cnt = df.groupby("Article")["Periodo_Qty"].transform("count")
    df["ArticleMeanPeriodo"] = np.where(art_cnt > 1, (art_sum - df["Periodo_Qty"]) / (art_cnt - 1), 0.0)

    shop_sum = df.groupby("Shop")["Periodo_Qty"].transform("sum")
    shop_cnt = df.groupby("Shop")["Periodo_Qty"].transform("count")
    df["ShopMeanPeriodo"] = np.where(shop_cnt > 1, (shop_sum - df["Periodo_Qty"]) / (shop_cnt - 1), 0.0)

    vend_sum = df.groupby("Shop")["Venduto_Qty"].transform("sum")
    vend_cnt = df.groupby("Shop")["Venduto_Qty"].transform("count")
    df["ShopVelocity"] = np.where(vend_cnt > 1, (vend_sum - df["Venduto_Qty"]) / (vend_cnt - 1), 0.0)

    df["ArticleCoverageCount"] = df.groupby("Article")["Shop"].transform("nunique").fillna(0.0)
    sales_obs = ((df["Periodo_Qty"] + df["Venduto_Qty"]) > 0).astype(float)
    df["ArticleSalesObs"] = sales_obs.groupby(df["Article"]).transform("sum")
    df["ShopSalesObs"] = sales_obs.groupby(df["Shop"]).transform("sum")

    # Rule-based demand (algorithmic core).
    observed = (
        0.60 * df["Periodo_Qty"]
        + 0.25 * df["Venduto_Qty"]
        + 0.15 * (df["SelloutUsed"] / 100.0) * np.maximum(df["Consegnato_Qty"], 1.0)
    )
    prior = 0.55 * df["ArticleMeanPeriodo"] + 0.45 * df["ShopMeanPeriodo"]
    df["DemandRuleBase"] = np.where(observed > 0, observed, 0.35 * prior)
    df["DemandRuleBase"] = np.maximum(df["DemandRuleBase"], 0.0)

    cover = df["StockNow"] / np.maximum(df["DemandRuleBase"], 1.0)
    boost = np.where(
        (df["SelloutUsed"] >= 65.0) & (cover < 1.5),
        1.0 + (1.5 - cover) * 0.22 + ((df["SelloutUsed"] - 65.0) / 35.0) * 0.12,
        1.0,
    )
    df["ScarcityBoost"] = np.clip(boost, 1.0, 1.8)
    df["DemandRule"] = df["DemandRuleBase"] * df["ScarcityBoost"] * df["ServiceFactor"]
    df["DemandRule"] = np.maximum(df["DemandRule"], 0.0)

    # AI model: ridge regression with OOF predictions.
    feature_cols = [
        "SelloutUsed",
        "StockNow",
        "SizeDepth",
        "ArticleMeanPeriodo",
        "ShopMeanPeriodo",
        "ArticleCoverageCount",
        "ArticleSalesObs",
        "ShopSalesObs",
        "ShopVelocity",
        "FasciaNum",
        "ServiceFactor",
        "ScarcityBoost",
    ]
    x = df[feature_cols].to_numpy(dtype=float)
    y_base = np.maximum(df["Periodo_Qty"], df["Venduto_Qty"]).to_numpy(dtype=float)
    y_base = np.nan_to_num(y_base, nan=0.0, posinf=1e6, neginf=0.0)
    y = np.log1p(np.clip(y_base, 0.0, 1e6))
    folds = _build_fold_ids(df, n_folds=5)
    y_pred_log, quality_r2, model_mode = _ridge_oof_predict(x, y, folds, lam=RIDGE_LAMBDA)
    df["DemandAI"] = np.maximum(np.expm1(y_pred_log), 0.0)

    sample_factor = min(1.0, len(df) / 1200.0)
    base_ai_weight = AI_BLEND_MAX * quality_r2 * sample_factor
    article_obs_factor = np.minimum(1.0, np.log1p(df["ArticleSalesObs"]) / np.log(8.0))
    shop_obs_factor = np.minimum(1.0, np.log1p(df["ShopSalesObs"]) / np.log(30.0))
    coverage_factor = article_obs_factor * shop_obs_factor
    df["DemandBlendWeight"] = base_ai_weight * coverage_factor
    if model_mode == "formula_only":
        df["DemandBlendWeight"] = 0.0

    df["DemandHybrid"] = (1.0 - df["DemandBlendWeight"]) * df["DemandRule"] + df["DemandBlendWeight"] * df["DemandAI"]
    df["DemandHybrid"] = np.maximum(df["DemandHybrid"], 0.0)

    # Ensure active rows (sales observed) are not muted.
    active_floor = np.where((df["Periodo_Qty"] + df["Venduto_Qty"]) > 0, 1.0, 0.0)
    df["DemandHybrid"] = np.maximum(df["DemandHybrid"], active_floor)

    demand = {(r.Article, r.Shop): float(r.DemandHybrid) for r in df.itertuples(index=False)}
    diagnostics = df[
        [
            "Article",
            "Shop",
            "Periodo_Qty",
            "Venduto_Qty",
            "StockNow",
            "SelloutUsed",
            "DemandRule",
            "DemandAI",
            "DemandBlendWeight",
            "DemandHybrid",
            "ScarcityBoost",
            "ServiceFactor",
        ]
    ].copy()
    diagnostics["DemandModelMode"] = model_mode
    diagnostics["DemandModelQualityR2"] = float(quality_r2)

    return demand, diagnostics
