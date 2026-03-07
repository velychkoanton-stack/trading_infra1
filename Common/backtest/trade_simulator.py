from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller


def adf_cointegration_ok(
    spread_window: pd.Series,
    adf_pvalue_threshold: float = 0.05,
    adf_stat_must_be_neg: bool = True,
    min_window_bars: int = 200,
) -> bool:
    """
    In-trade ADF check on spread window.

    Conservative behavior:
    - if window is too short, return True
    - if ADF raises exception, return True
    """
    try:
        spread = pd.to_numeric(spread_window, errors="coerce").dropna().astype(float)

        if len(spread) < min_window_bars:
            return True

        stat, pvalue = adfuller(spread, autolag="AIC")[0:2]

        if pvalue >= adf_pvalue_threshold:
            return False

        if adf_stat_must_be_neg and stat >= 0:
            return False

        return True

    except Exception:
        return True


def prepare_pair_backtest_df(
    df_asset_1: pd.DataFrame,
    df_asset_2: pd.DataFrame,
    rolling_window: int,
    beta_value: float,
) -> pd.DataFrame | None:
    """
    Align two asset dataframes by timestamp and build spread/zscore columns.

    Expected source columns:
    - ts
    - close

    Output columns:
    - ts
    - close_1
    - close_2
    - spread
    - rolling_mean
    - rolling_std
    - zscore
    """
    required_cols = {"ts", "close"}

    if not required_cols.issubset(df_asset_1.columns):
        raise ValueError(f"df_asset_1 missing required columns: {required_cols}")

    if not required_cols.issubset(df_asset_2.columns):
        raise ValueError(f"df_asset_2 missing required columns: {required_cols}")

    left = df_asset_1[["ts", "close"]].copy()
    left.columns = ["ts", "close_1"]

    right = df_asset_2[["ts", "close"]].copy()
    right.columns = ["ts", "close_2"]

    merged = pd.merge(left, right, on="ts", how="inner")
    if merged.empty:
        return None

    merged["close_1"] = pd.to_numeric(merged["close_1"], errors="coerce")
    merged["close_2"] = pd.to_numeric(merged["close_2"], errors="coerce")
    merged = merged.dropna(subset=["close_1", "close_2"]).sort_values("ts").reset_index(drop=True)

    if len(merged) < rolling_window + 50:
        return None

    if (merged["close_1"] <= 0).any() or (merged["close_2"] <= 0).any():
        return None

    beta = float(beta_value)

    merged["spread"] = np.log(merged["close_1"]) - beta * np.log(merged["close_2"])
    merged["rolling_mean"] = merged["spread"].rolling(window=int(rolling_window)).mean()
    merged["rolling_std"] = merged["spread"].rolling(window=int(rolling_window)).std()

    merged["zscore"] = (merged["spread"] - merged["rolling_mean"]) / merged["rolling_std"]
    merged = merged.replace([np.inf, -np.inf], np.nan)
    merged = merged.dropna(subset=["zscore"]).reset_index(drop=True)

    if len(merged) < rolling_window + 10:
        return None

    return merged


def _update_trade_stats(
    num_wins: int,
    num_losses: int,
    total_win_profit: float,
    total_loss_abs: float,
    num_trades: int,
    net_pnl: float,
) -> tuple[int, int, float, float, int]:
    num_trades += 1

    if net_pnl > 0:
        num_wins += 1
        total_win_profit += net_pnl
    else:
        num_losses += 1
        total_loss_abs += abs(net_pnl)

    return num_wins, num_losses, total_win_profit, total_loss_abs, num_trades


def simulate_backtest_on_pair_df(
    df: pd.DataFrame,
    rolling_window: int,
    take_profit_percent: float,
    stop_loss_percent: float,
    zscore_sl_threshold: float,
    open_threshold_multiplier: float,
    initial_balance: float,
    transaction_cost: float,
    entry_z_up: float = 2.0,
    entry_z_dn: float = -2.0,
    exit_z_mr: float = 0.0,
    adf_pvalue_threshold: float = 0.05,
    adf_stat_must_be_neg: bool = True,
    adf_check_every_bars: int = 12,
    adf_persist_k: int = 3,
    min_adf_window_bars: int = 200,
) -> dict[str, Any] | None:
    """
    Run pair-trading simulation on prepared pair dataframe.

    Exit priority:
    1) TP / SL
    2) Mean reversion
    3) Half-life gate + lost cointegration

    Notes:
    - half-life gate uses rolling_window as practical proxy, matching your previous infra
    - zscore_sl_threshold is currently accepted as input and reported back for compatibility,
      but live stop logic is still based on monetary SL, same as your previous engine
    """
    if df is None or df.empty:
        return None

    upper_entry = float(entry_z_up) * float(open_threshold_multiplier)
    lower_entry = float(entry_z_dn) * float(open_threshold_multiplier)

    balance = float(initial_balance)

    open_position: str | None = None
    entry_p1: float | None = None
    entry_p2: float | None = None

    bars_in_trade = 0
    fail_count = 0
    cointegration_lost = False
    half_life_bars = int(rolling_window)

    num_wins = 0
    num_losses = 0
    total_win_profit = 0.0
    total_loss_abs = 0.0
    num_trades = 0

    exits = {
        "TP": 0,
        "SL": 0,
        "MEAN_REVERT": 0,
        "HL_COINTEGRATION_LOST": 0,
    }

    equity_curve: list[float] = [balance]
    max_balance = balance
    max_drawdown = 0.0

    for i in range(len(df)):
        row = df.iloc[i]

        z = float(row["zscore"])
        p1 = float(row["close_1"])
        p2 = float(row["close_2"])

        # ENTRY
        if open_position is None:
            if z > upper_entry:
                open_position = "SHORT"
                entry_p1 = p1
                entry_p2 = p2
                bars_in_trade = 0
                fail_count = 0
                cointegration_lost = False

            elif z < lower_entry:
                open_position = "LONG"
                entry_p1 = p1
                entry_p2 = p2
                bars_in_trade = 0
                fail_count = 0
                cointegration_lost = False

            equity_curve.append(balance)
            max_balance = max(max_balance, balance)
            if max_balance > 0:
                drawdown = (balance - max_balance) / max_balance
                max_drawdown = min(max_drawdown, drawdown)
            continue

        bars_in_trade += 1

        ch1 = (p1 - entry_p1) / entry_p1
        ch2 = (p2 - entry_p2) / entry_p2
        raw_pnl = (ch1 - ch2) if open_position == "LONG" else (ch2 - ch1)

        gross_pnl = (balance / 2.0) * raw_pnl
        fee = balance * float(transaction_cost)
        net_pnl_if_exit = gross_pnl - fee

        # Periodic ADF check while position is open
        if bars_in_trade % int(adf_check_every_bars) == 0:
            start_idx = max(0, i - int(rolling_window))
            ok = adf_cointegration_ok(
                spread_window=df["spread"].iloc[start_idx:i],
                adf_pvalue_threshold=adf_pvalue_threshold,
                adf_stat_must_be_neg=adf_stat_must_be_neg,
                min_window_bars=min_adf_window_bars,
            )
            fail_count = 0 if ok else (fail_count + 1)
            cointegration_lost = fail_count >= int(adf_persist_k)

        # 1) TP / SL any time
        if net_pnl_if_exit >= balance * float(take_profit_percent):
            balance += net_pnl_if_exit
            num_wins, num_losses, total_win_profit, total_loss_abs, num_trades = _update_trade_stats(
                num_wins=num_wins,
                num_losses=num_losses,
                total_win_profit=total_win_profit,
                total_loss_abs=total_loss_abs,
                num_trades=num_trades,
                net_pnl=net_pnl_if_exit,
            )
            exits["TP"] += 1
            open_position = None

        elif net_pnl_if_exit <= -balance * float(stop_loss_percent):
            balance += net_pnl_if_exit
            num_wins, num_losses, total_win_profit, total_loss_abs, num_trades = _update_trade_stats(
                num_wins=num_wins,
                num_losses=num_losses,
                total_win_profit=total_win_profit,
                total_loss_abs=total_loss_abs,
                num_trades=num_trades,
                net_pnl=net_pnl_if_exit,
            )
            exits["SL"] += 1
            open_position = None

        # Optional hard z-score stop, separate from monetary SL
        elif abs(z) >= float(zscore_sl_threshold):
            balance += net_pnl_if_exit
            num_wins, num_losses, total_win_profit, total_loss_abs, num_trades = _update_trade_stats(
                num_wins=num_wins,
                num_losses=num_losses,
                total_win_profit=total_win_profit,
                total_loss_abs=total_loss_abs,
                num_trades=num_trades,
                net_pnl=net_pnl_if_exit,
            )
            exits["SL"] += 1
            open_position = None

        # 2) Mean reversion any time
        elif (open_position == "LONG" and z >= float(exit_z_mr)) or (
            open_position == "SHORT" and z <= float(exit_z_mr)
        ):
            balance += net_pnl_if_exit
            num_wins, num_losses, total_win_profit, total_loss_abs, num_trades = _update_trade_stats(
                num_wins=num_wins,
                num_losses=num_losses,
                total_win_profit=total_win_profit,
                total_loss_abs=total_loss_abs,
                num_trades=num_trades,
                net_pnl=net_pnl_if_exit,
            )
            exits["MEAN_REVERT"] += 1
            open_position = None

        # 3) Half-life gate + lost cointegration
        elif bars_in_trade >= half_life_bars and cointegration_lost:
            balance += net_pnl_if_exit
            num_wins, num_losses, total_win_profit, total_loss_abs, num_trades = _update_trade_stats(
                num_wins=num_wins,
                num_losses=num_losses,
                total_win_profit=total_win_profit,
                total_loss_abs=total_loss_abs,
                num_trades=num_trades,
                net_pnl=net_pnl_if_exit,
            )
            exits["HL_COINTEGRATION_LOST"] += 1
            open_position = None

        equity_curve.append(balance)
        max_balance = max(max_balance, balance)
        if max_balance > 0:
            drawdown = (balance - max_balance) / max_balance
            max_drawdown = min(max_drawdown, drawdown)

    if num_trades == 0:
        win_rate = 0.0
        risk_reward_ratio = 0.0
        avg_win = 0.0
        avg_loss = 0.0
    else:
        win_rate = (num_wins / num_trades) * 100.0
        avg_win = total_win_profit / num_wins if num_wins > 0 else 0.0
        avg_loss = total_loss_abs / num_losses if num_losses > 0 else 0.0
        risk_reward_ratio = (avg_win / avg_loss) if avg_loss > 0 else 999999.0

    result = {
        "bt_final_equity": float(balance),
        "win_rate": float(win_rate),
        "risk_reward_ratio": float(risk_reward_ratio),
        "num_trades": int(num_trades),
        "exit_mean_reverted": int(exits["MEAN_REVERT"]),
        "exit_hl_coint_lst": int(exits["HL_COINTEGRATION_LOST"]),
        "exit_tp": int(exits["TP"]),
        "exit_sl": int(exits["SL"]),
        "avg_win": float(avg_win),
        "avg_loss": float(avg_loss),
        "max_drawdown": float(max_drawdown),
        "positive_result": bool(balance > float(initial_balance)),
    }

    return result