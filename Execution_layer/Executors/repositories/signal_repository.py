from __future__ import annotations

from pathlib import Path

from Common.db.db_execute import fetch_all, fetch_one
from Common.utils.sql_file_loader import load_sql_file
from Execution_layer.Executors.models import CandidatePair


class SignalRepository:
    def __init__(self, api_file_name: str, sql_dir: str | Path) -> None:
        self.api_file_name = api_file_name
        self.sql_dir = Path(sql_dir)

    def fetch_candidate_pool(
        self,
        level_180: str,
        excluded_assets: list[str] | None = None,
        extra_filters_sql: str = "",
        extra_params: tuple = (),
    ) -> list[CandidatePair]:
        ...

    def fetch_candidate_by_uuid(self, uuid: str) -> CandidatePair | None:
        ...