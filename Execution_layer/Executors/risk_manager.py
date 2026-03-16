from __future__ import annotations

from datetime import datetime

from Execution_layer.Executors.models import CandidatePair, CloseDecision, OpenPairRecord, RiskDecision


class RiskManager:
    def __init__(self, logger) -> None:
        self.logger = logger

    def can_open(
        self,
        scheduler_status: str,
        candidate: CandidatePair,
        support_health: dict,
        account_snapshot: dict,
        signal_stale_sec: int,
        pair_state_stale_sec: int,
    ) -> RiskDecision:
        ...

    def should_force_close(
        self,
        scheduler_status: str,
        support_health: dict,
        candidate_refresh: CandidatePair | None,
        pair_unrealized_pnl: float | None,
        signal_stale_sec: int,
        pair_state_stale_sec: int,
        ws_stale_close_sec: int,
    ) -> CloseDecision:
        ...

    @staticmethod
    def is_fresh(ts: datetime, max_age_sec: int) -> bool:
        ...