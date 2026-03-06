from pathlib import Path

from Common.config.path_config import get_project_root


def sanitize_symbol(symbol: str) -> str:
    """
    Convert exchange symbol to filesystem-safe parquet filename stem.

    Examples:
    - BTC/USDT:USDT -> BTC_USDT_USDT
    - ETH/USDC:USDC -> ETH_USDC_USDC
    """
    sanitized = symbol.strip()
    sanitized = sanitized.replace("/", "_")
    sanitized = sanitized.replace(":", "_")
    sanitized = sanitized.replace("\\", "_")
    sanitized = sanitized.replace(" ", "_")
    return sanitized


def get_bybit_linear_5m_dir() -> Path:
    """
    Returns:
    Trading_infra/data/parquet_db/bybit_linear_5m/
    """
    return get_project_root() / "data" / "parquet_db" / "bybit_linear_5m"


def get_symbol_parquet_path(symbol: str) -> Path:
    """
    Build full parquet path for one symbol.
    """
    file_name = f"{sanitize_symbol(symbol)}.parquet"
    return get_bybit_linear_5m_dir() / file_name