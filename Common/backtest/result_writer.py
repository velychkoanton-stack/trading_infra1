from __future__ import annotations

from pathlib import Path

import pandas as pd

from Common.config.path_config import get_project_root


def ensure_backtest_output_dir(relative_dir: str) -> Path:
    """
    Resolve and create the backtest output directory under project root.
    """
    output_dir = get_project_root() / relative_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def pair_to_backtest_filename(asset_1: str, asset_2: str) -> str:
    """
    Convert pair symbols to a filesystem-safe CSV filename.

    Example:
    BTC/USDT:USDT + ETH/USDT:USDT
    -> BTC_USDT_USDT__ETH_USDT_USDT.csv
    """
    def sanitize(symbol: str) -> str:
        return symbol.replace("/", "_").replace(":", "_").replace("\\", "_").replace(" ", "_")

    return f"{sanitize(asset_1)}__{sanitize(asset_2)}.csv"


def write_pair_backtest_grid_csv(
    records: list[dict],
    asset_1: str,
    asset_2: str,
    relative_output_dir: str,
) -> Path:
    """
    Write all grid-search results for one pair to its own CSV file.

    Existing file is replaced fully on each run.
    """
    output_dir = ensure_backtest_output_dir(relative_output_dir)
    file_path = output_dir / pair_to_backtest_filename(asset_1, asset_2)

    df = pd.DataFrame(records)
    df.to_csv(file_path, index=False)

    return file_path