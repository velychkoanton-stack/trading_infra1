from pathlib import Path

import pandas as pd

from Common.parquet.symbol_to_path import get_symbol_parquet_path


def parquet_exists(symbol: str) -> bool:
    path = get_symbol_parquet_path(symbol)
    return path.exists()


def read_symbol_ohlcv_parquet(symbol: str) -> pd.DataFrame:
    """
    Read one symbol parquet file into DataFrame.

    Raises:
    - FileNotFoundError if parquet does not exist
    """
    path: Path = get_symbol_parquet_path(symbol)

    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found for symbol={symbol}: {path}")

    return pd.read_parquet(path, engine="pyarrow")