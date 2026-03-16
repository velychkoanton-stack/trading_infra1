from __future__ import annotations


def ccxt_symbol_to_asset(symbol: str) -> str:
    ...


def pybit_symbol_to_asset(symbol: str) -> str:
    ...


def ccxt_symbol_to_pybit_symbol(symbol: str) -> str:
    ...


def assets_to_uuid(asset_1: str, asset_2: str) -> str:
    ...


def uuid_to_assets(uuid: str) -> tuple[str, str]:
    ...