from __future__ import annotations

from typing import Any

import pandas as pd

from Common.backtest.result_writer import write_pair_backtest_grid_csv
from Common.backtest.trade_simulator import (
    prepare_pair_backtest_df,
    simulate_backtest_on_pair_df,
)


def clamp_beta_to_band(
    beta_norm: float,
    band_min: float,
    band_max: float,
) -> float:
    """
    Clamp measured beta_norm to production-compatible band.

    Example:
    beta_norm = 1.34 -> 1.20
    beta_norm = 0.76 -> 0.80
    """
    beta_value = float(beta_norm)
    return max(float(band_min), min(float(band_max), beta_value))


def snap_beta_to_step(
    beta_value: float,
    step: float = 0.05,
    band_min: float = 0.8,
    band_max: float = 1.2,
) -> float:
    """
    Snap beta value to nearest production-style discrete step.

    Example:
    0.93 -> 0.95
    1.07 -> 1.05
    """
    if step <= 0:
        raise ValueError("step must be positive")

    clamped = clamp_beta_to_band(beta_value, band_min=band_min, band_max=band_max)
    snapped = round(round((clamped - band_min) / step) * step + band_min, 10)

    snapped = max(band_min, min(band_max, snapped))
    return round(snapped, 4)


def calculate_backtest_score(
    bt_final_equity: float,
    initial_balance: float,
    win_rate: float,
    risk_reward_ratio: float,
    num_trades: int,
    positive_grid_share: float,
) -> float:
    """
    Provisional backtest score.

    This is intentionally simple and can be refined later.
    """
    if initial_balance <= 0:
        raise ValueError("initial_balance must be positive")

    return_multiple = float(bt_final_equity) / float(initial_balance)
    rr_capped = min(float(risk_reward_ratio), 5.0)
    trades_capped = min(int(num_trades), 50)

    score = (
        max(0.0, (return_multiple - 1.0)) * 40.0
        + (float(win_rate) / 100.0) * 20.0
        + (rr_capped / 5.0) * 15.0
        + (trades_capped / 50.0) * 10.0
        + float(positive_grid_share) * 15.0
    )

    return round(max(0.0, min(score, 100.0)), 4)


def _prepare_grid_records(
    grid_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    required_cols = {
        "take_profit_percent",
        "stop_loss_percent",
        "zscore_sl_threshold",
        "open_threshold_multiplier",
    }

    if not required_cols.issubset(grid_df.columns):
        missing = required_cols - set(grid_df.columns)
        raise ValueError(f"Backtest grid is missing required columns: {missing}")

    records = grid_df.to_dict(orient="records")
    if not records:
        raise ValueError("Backtest grid is empty")

    return records


def _find_plateau_candidates(
    positive_df: pd.DataFrame,
    equity_col: str = "bt_final_equity",
) -> pd.DataFrame:
    """
    Simple robust plateau heuristic.

    Instead of taking only the absolute maximum, prefer rows that:
    - are profitable
    - have high equity
    - also sit near the central tendency of profitable region

    Current implementation:
    - keep rows above profitable median equity
    - then sort by equity, win rate, RR, trades
    """
    if positive_df.empty:
        return positive_df

    median_equity = positive_df[equity_col].median()
    plateau_df = positive_df[positive_df[equity_col] >= median_equity].copy()

    if plateau_df.empty:
        plateau_df = positive_df.copy()

    plateau_df = plateau_df.sort_values(
        by=["bt_final_equity", "win_rate", "risk_reward_ratio", "num_trades"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    return plateau_df


def run_pair_backtest(
    df_asset_1: pd.DataFrame,
    df_asset_2: pd.DataFrame,
    asset_1: str,
    asset_2: str,
    rolling_window: int,
    measured_beta_norm: float,
    grid_df: pd.DataFrame,
    rules: dict[str, str],
) -> dict[str, Any]:
    """
    Run full grid-search backtest for one pair and return a structured result.

    Expected rules keys:
    - BACKTEST_START_BALANCE
    - MIN_POSITIVE_GRID_SHARE
    - MIN_TRADES
    - BACKTEST_OUTPUT_DIR
    - BETA_NORM_MIN
    - BETA_NORM_MAX
    Optionally:
    - TRANSACTION_COST
    - ENTRY_Z_UP
    - ENTRY_Z_DN
    - EXIT_Z_MR
    - ADF_PVAL_THRESHOLD
    - ADF_STAT_MUST_BE_NEG
    - ADF_CHECK_EVERY_BARS
    - ADF_PERSIST_K
    - MIN_ADF_WINDOW_BARS
    - BETA_SNAP_STEP
    """
    initial_balance = float(rules["BACKTEST_START_BALANCE"])
    min_positive_grid_share = float(rules["MIN_POSITIVE_GRID_SHARE"])
    min_trades = int(rules["MIN_TRADES"])
    output_dir = rules["BACKTEST_OUTPUT_DIR"]

    transaction_cost = float(rules.get("TRANSACTION_COST", 0.005))
    entry_z_up = float(rules.get("ENTRY_Z_UP", 2.0))
    entry_z_dn = float(rules.get("ENTRY_Z_DN", -2.0))
    exit_z_mr = float(rules.get("EXIT_Z_MR", 0.0))
    adf_pval_threshold = float(rules.get("ADF_PVAL_THRESHOLD", 0.05))
    adf_stat_must_be_neg = str(rules.get("ADF_STAT_MUST_BE_NEG", "true")).lower() == "true"
    adf_check_every_bars = int(rules.get("ADF_CHECK_EVERY_BARS", 12))
    adf_persist_k = int(rules.get("ADF_PERSIST_K", 3))
    min_adf_window_bars = int(rules.get("MIN_ADF_WINDOW_BARS", 200))

    beta_norm_min = float(rules.get("BETA_NORM_MIN", 0.8))
    beta_norm_max = float(rules.get("BETA_NORM_MAX", 1.2))
    beta_snap_step = float(rules.get("BETA_SNAP_STEP", 0.05))

    test_beta = snap_beta_to_step(
        beta_value=float(measured_beta_norm),
        step=beta_snap_step,
        band_min=beta_norm_min,
        band_max=beta_norm_max,
    )

    pair_df = prepare_pair_backtest_df(
        df_asset_1=df_asset_1,
        df_asset_2=df_asset_2,
        rolling_window=int(rolling_window),
        beta_value=float(test_beta),
    )

    if pair_df is None or pair_df.empty:
        return {
            "success": False,
            "reason": "pair_df_empty",
            "grid_records": [],
            "selected_result": None,
            "positive_grid_share": 0.0,
            "best_beta": float(test_beta),
        }

    grid_records = _prepare_grid_records(grid_df)

    all_results: list[dict[str, Any]] = []

    for row in grid_records:
        tp = float(row["take_profit_percent"])
        sl = float(row["stop_loss_percent"])
        zsl = float(row["zscore_sl_threshold"])
        open_mult = float(row["open_threshold_multiplier"])

        sim_result = simulate_backtest_on_pair_df(
            df=pair_df,
            rolling_window=int(rolling_window),
            take_profit_percent=tp,
            stop_loss_percent=sl,
            zscore_sl_threshold=zsl,
            open_threshold_multiplier=open_mult,
            initial_balance=initial_balance,
            transaction_cost=transaction_cost,
            entry_z_up=entry_z_up,
            entry_z_dn=entry_z_dn,
            exit_z_mr=exit_z_mr,
            adf_pvalue_threshold=adf_pval_threshold,
            adf_stat_must_be_neg=adf_stat_must_be_neg,
            adf_check_every_bars=adf_check_every_bars,
            adf_persist_k=adf_persist_k,
            min_adf_window_bars=min_adf_window_bars,
        )

        if sim_result is None:
            continue

        all_results.append(
            {
                "asset_1": asset_1,
                "asset_2": asset_2,
                "rolling_window": int(rolling_window),
                "take_profit_percent": tp,
                "stop_loss_percent": sl,
                "zscore_sl_threshold": zsl,
                "open_threshold_multiplier": open_mult,
                "beta_used": float(test_beta),
                **sim_result,
            }
        )

    if not all_results:
        return {
            "success": False,
            "reason": "no_grid_results",
            "grid_records": [],
            "selected_result": None,
            "positive_grid_share": 0.0,
            "best_beta": float(test_beta),
        }

    results_df = pd.DataFrame(all_results)
    write_pair_backtest_grid_csv(
        records=all_results,
        asset_1=asset_1,
        asset_2=asset_2,
        relative_output_dir=output_dir,
    )

    positive_df = results_df[
        (results_df["bt_final_equity"] > initial_balance) &
        (results_df["num_trades"] >= min_trades)
    ].copy()

    positive_grid_share = len(positive_df) / len(results_df) if len(results_df) > 0 else 0.0

    if positive_df.empty or positive_grid_share < min_positive_grid_share:
        return {
            "success": False,
            "reason": "insufficient_positive_grid_share",
            "grid_records": all_results,
            "selected_result": None,
            "positive_grid_share": round(float(positive_grid_share), 4),
            "best_beta": float(test_beta),
        }

    plateau_df = _find_plateau_candidates(positive_df)
    selected = plateau_df.iloc[0].to_dict()

    backtest_score = calculate_backtest_score(
        bt_final_equity=float(selected["bt_final_equity"]),
        initial_balance=initial_balance,
        win_rate=float(selected["win_rate"]),
        risk_reward_ratio=float(selected["risk_reward_ratio"]),
        num_trades=int(selected["num_trades"]),
        positive_grid_share=float(positive_grid_share),
    )

    selected["backtest_score"] = float(backtest_score)

    return {
        "success": True,
        "reason": "ok",
        "grid_records": all_results,
        "selected_result": selected,
        "positive_grid_share": round(float(positive_grid_share), 4),
        "best_beta": float(test_beta),
    }