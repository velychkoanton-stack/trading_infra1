from __future__ import annotations

from pathlib import Path

from Common.db.db_execute import execute
from Common.db.db_transaction import run_in_transaction
from Common.utils.sql_file_loader import load_sql_file


class LockRepository:
    def __init__(self, api_file_name: str, sql_dir: str | Path) -> None:
        self.api_file_name = api_file_name
        self.sql_dir = Path(sql_dir)

    def try_lock_pair_assets(
        self,
        bot_id: str,
        uuid: str,
        asset_1: str,
        asset_2: str,
    ) -> bool:
        ...

    def release_pair_assets(self, bot_id: str, uuid: str) -> int:
        ...