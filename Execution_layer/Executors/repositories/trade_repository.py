from __future__ import annotations

from pathlib import Path

from Common.db.db_execute import execute, fetch_one
from Common.utils.sql_file_loader import load_sql_file


class TradeRepository:
    def __init__(self, api_file_name: str, sql_dir: str | Path) -> None:
        self.api_file_name = api_file_name
        self.sql_dir = Path(sql_dir)

    def insert_open_trade(
        self,
        uuid: str,
        bot_id: str,
        pos_val: float,
        open_cond: str | None,
    ) -> int:
        ...

    def update_close_trade(
        self,
        trade_id: int,
        pnl: float,
        pnl_pers: float,
        closed_by: str,
        close_cond: str | None,
    ) -> int:
        ...

    def get_open_trade_for_uuid_bot(self, uuid: str, bot_id: str) -> dict | None:
        ...