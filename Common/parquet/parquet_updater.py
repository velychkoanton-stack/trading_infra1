import pandas as pd

from Common.parquet.parquet_reader import parquet_exists, read_symbol_ohlcv_parquet
from Common.parquet.parquet_writer import write_symbol_ohlcv_parquet, ensure_ohlcv_columns


def build_ohlcv_dataframe(rows: list[list | tuple]) -> pd.DataFrame:
    """
    Convert CCXT OHLCV rows to normalized DataFrame.

    Expected row format:
    [timestamp_ms, open, high, low, close, volume]
    """
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    ensure_ohlcv_columns(df)
    return df


def replace_symbol_ohlcv_parquet(symbol: str, rows: list[list | tuple]) -> None:
    """
    Replace parquet content fully with fresh OHLCV rows.
    """
    df = build_ohlcv_dataframe(rows)
    write_symbol_ohlcv_parquet(symbol, df)


def merge_symbol_ohlcv_parquet(symbol: str, rows: list[list | tuple]) -> None:
    """
    Merge new OHLCV rows into existing parquet file by timestamp.

    Rules:
    - concatenate old + new
    - deduplicate by ts
    - keep latest occurrence
    - sort by ts ascending
    """
    new_df = build_ohlcv_dataframe(rows)

    if parquet_exists(symbol):
        old_df = read_symbol_ohlcv_parquet(symbol)
        combined = pd.concat([old_df, new_df], ignore_index=True)
    else:
        combined = new_df

    combined = combined.drop_duplicates(subset=["ts"], keep="last")
    combined = combined.sort_values("ts").reset_index(drop=True)

    write_symbol_ohlcv_parquet(symbol, combined)