from __future__ import annotations

from Execution_layer.Executors.models import CandidatePair, SizingResult


class PositionSizer:
    def __init__(self, rules: dict[str, str]) -> None:
        self.rules = rules

    def calculate_pair_size(
        self,
        candidate: CandidatePair,
        account_snapshot: dict,
        order_manager: "OrderManager",
        throttle_mode: bool = False,
    ) -> SizingResult:
        ...