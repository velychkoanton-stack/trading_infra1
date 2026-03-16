from __future__ import annotations

from pathlib import Path

from Common.db.db_execute import fetch_one
from Common.utils.sql_file_loader import load_sql_file


class SchedulerRepository:
    def __init__(self, api_file_name: str, sql_dir: str | Path) -> None:
        self.api_file_name = api_file_name
        self.sql_dir = Path(sql_dir)

    def get_worker_status(self, worker_id: str) -> str:
        ...