from __future__ import annotations

import threading
import time
from datetime import date, datetime

from Common.db.heartbeat_writer import write_heartbeat
from Execution_layer.Executors.models import PairLiveMetrics


class ControlWorker:
    def __init__(
        self,
        bot_config,
        support_bridge,
        shared_state,
        daily_snapshot_repo,
        position_value_repo,
        logger,
    ) -> None:
        self.bot_config = bot_config
        self.support_bridge = support_bridge
        self.shared_state = shared_state
        self.daily_snapshot_repo = daily_snapshot_repo
        self.position_value_repo = position_value_repo
        self.logger = logger
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        ...

    def stop(self) -> None:
        ...

    def run_loop(self) -> None:
        ...

    def run_cycle(self) -> None:
        ...

    def write_heartbeat(self, runtime_status: str, comment: str | None = None) -> None:
        ...

    def ensure_daily_snapshot(self) -> None:
        ...

    def update_pair_metrics_and_position_values(self) -> None:
        ...

    def update_ws_critical_flag(self) -> None:
        ...