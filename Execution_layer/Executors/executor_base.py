from __future__ import annotations

import time
from datetime import datetime

from Execution_layer.Executors.models import CandidatePair, CloseDecision, OpenPairRecord


class ExecutorBase:
    def __init__(
        self,
        bot_config,
        worker_id: str,
        rules: dict[str, str],
        support_bridge,
        shared_state,
        signal_repo,
        lock_repo,
        trade_repo,
        scheduler_repo,
        position_value_repo,
        order_manager,
        risk_manager,
        position_sizer,
        notification_service,
        logger,
    ) -> None:
        self.bot_config = bot_config
        self.worker_id = worker_id
        self.rules = rules
        self.support_bridge = support_bridge
        self.shared_state = shared_state
        self.signal_repo = signal_repo
        self.lock_repo = lock_repo
        self.trade_repo = trade_repo
        self.scheduler_repo = scheduler_repo
        self.position_value_repo = position_value_repo
        self.order_manager = order_manager
        self.risk_manager = risk_manager
        self.position_sizer = position_sizer
        self.notification_service = notification_service
        self.logger = logger

    def run_cycle(self) -> None:
        ...

    def load_candidates(self) -> list[CandidatePair]:
        ...

    def select_candidate(self, candidates: list[CandidatePair]) -> CandidatePair | None:
        ...

    def try_open_candidate(self, candidate: CandidatePair) -> OpenPairRecord | None:
        ...

    def monitor_open_trade(self, record: OpenPairRecord) -> None:
        ...

    def close_trade(self, record: OpenPairRecord, close_reason: str) -> None:
        ...

    def build_open_cond(self, candidate: CandidatePair) -> str:
        ...

    def build_close_cond(self, candidate: CandidatePair | None) -> str:
        ...

    def get_entry_sides(self, z_score: float) -> tuple[str, str]:
        ...

    def should_close_by_trade_logic(
        self,
        record: OpenPairRecord,
        candidate_refresh: CandidatePair | None,
        pair_unrealized_pnl: float | None,
    ) -> CloseDecision:
        ...