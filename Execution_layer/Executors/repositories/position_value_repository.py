from __future__ import annotations

from datetime import datetime
from pathlib import Path

from Common.db.db_execute import execute
from Common.utils.sql_file_loader import load_sql_file


class PositionValueRepository:
    def __init__(self, api_file_name: str, sql_dir: str | Path) -> None:
        self.api_file_name = api_file_name
        self.sql_dir = Path(sql_dir)

    def upsert_open_pair_value(
        self,
        bot_id: str,
        uuid: str,
        pos_value: float,
        unrealized_pnl: float,
        updated_at: datetime,
    ) -> int:
        ...

    def delete_open_pair_value(self, bot_id: str, uuid: str) -> int:
        ...