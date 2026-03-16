from __future__ import annotations

import threading
from datetime import datetime
from typing import Optional

from Execution_layer.Executors.models import OpenPairRecord, PairLiveMetrics


class SharedExecutorState:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._open_pairs: dict[str, OpenPairRecord] = {}
        self._pair_metrics: dict[str, PairLiveMetrics] = {}
        self._ws_critical: bool = False
        self._ws_critical_since: Optional[datetime] = None
        self._ws_comment: Optional[str] = None

    def register_open_pair(self, record: OpenPairRecord) -> None:
        ...

    def remove_open_pair(self, uuid: str) -> None:
        ...

    def get_open_pair(self, uuid: str) -> Optional[OpenPairRecord]:
        ...

    def get_all_open_pairs(self) -> list[OpenPairRecord]:
        ...

    def get_open_pairs_for_bot(self, bot_id: str) -> list[OpenPairRecord]:
        ...

    def update_pair_metrics(self, metrics: PairLiveMetrics) -> None:
        ...

    def get_pair_metrics(self, uuid: str) -> Optional[PairLiveMetrics]:
        ...

    def remove_pair_metrics(self, uuid: str) -> None:
        ...

    def set_ws_critical(self, is_critical: bool, comment: str | None = None) -> None:
        ...

    def is_ws_critical(self) -> bool:
        ...

    def get_ws_critical_state(self) -> dict[str, object]:
        ...