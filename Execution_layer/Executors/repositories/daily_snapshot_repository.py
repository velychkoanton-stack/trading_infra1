from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from Common.db.db_execute import execute, fetch_one
from Common.utils.sql_file_loader import load_sql_file


class DailySnapshotRepository:
    def __init__(self, api_file_name: str, sql_dir: str | Path) -> None:
        self.api_file_name = api_file_name
        self.sql_dir = Path(sql_dir)

    def ensure_today_snapshot(
        self,
        bot_id: str,
        snapshot_date: date,
        start_equity: float,
        start_balance: float,
        current_equity: float,
        start_ts: datetime,
    ) -> int:
        ...

    def update_current_equity(
        self,
        bot_id: str,
        snapshot_date: date,
        current_equity: float,
    ) -> int:
        ...

    def get_today_snapshot(self, bot_id: str, snapshot_date: date) -> dict | None:
        ...