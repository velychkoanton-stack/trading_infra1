from __future__ import annotations

import pandas as pd


def clean_numeric_series(series: pd.Series) -> pd.Series:
    clean = pd.to_numeric(series, errors="coerce").dropna()

    if clean.empty:
        raise ValueError("Series is empty after cleaning.")

    return clean.astype(float)


def build_spread_series(
    asset_1_close: pd.Series,
    asset_2_close: pd.Series,
    beta_norm: float | None = None,
) -> pd.Series:
    """
    Spread = close_1 - beta_norm * close_2

    If beta_norm is missing or invalid, fallback to 1.0.
    """
    s1 = clean_numeric_series(asset_1_close)
    s2 = clean_numeric_series(asset_2_close)

    if len(s1) != len(s2):
        raise ValueError("Close series lengths do not match.")

    beta = float(beta_norm) if beta_norm not in (None, 0) else 1.0
    return s1 - beta * s2


def calculate_zscore_series(spread: pd.Series) -> pd.Series:
    clean = clean_numeric_series(spread)

    mean_val = float(clean.mean())
    std_val = float(clean.std())

    if std_val == 0:
        raise ValueError("Spread standard deviation is zero; z-score cannot be calculated.")

    return (clean - mean_val) / std_val


def calculate_latest_zscore(spread: pd.Series) -> float:
    z = calculate_zscore_series(spread)
    return float(z.iloc[-1])


def calculate_zscore_summary(spread: pd.Series) -> dict[str, float]:
    z = calculate_zscore_series(spread)

    return {
        "last_z_score": float(z.iloc[-1]),
        "max_z_score": float(z.max()),
        "min_z_score": float(z.min()),
    }