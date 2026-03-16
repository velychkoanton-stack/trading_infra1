from __future__ import annotations

import threading
from typing import Any, Optional

from Execution_layer.Support_layer.support_runner import SupportRunner


class SupportBridge:
    def __init__(
        self,
        environment: str,
        api_file_name: str,
        monitor: bool = False,
        monitor_interval_sec: int = 10,
    ) -> None:
        self.environment = environment
        self.api_file_name = api_file_name
        self.monitor = monitor
        self.monitor_interval_sec = monitor_interval_sec

        self.runner = SupportRunner(
            environment=self.environment,
            api_file_name=self.api_file_name,
            monitor=self.monitor,
            monitor_interval_sec=self.monitor_interval_sec,
        )

        self._thread: threading.Thread | None = None

    def start(self) -> None:
        ...

    def stop(self) -> None:
        ...

    def is_running(self) -> bool:
        ...

    def get_account_snapshot(self) -> dict[str, Any]:
        ...

    def get_positions_snapshot(self) -> dict[str, dict[str, Any]]:
        ...

    def get_open_orders_snapshot(self) -> list[dict[str, Any]]:
        ...

    def get_recent_executions_snapshot(self) -> list[dict[str, Any]]:
        ...

    def get_subscribed_symbols(self) -> set[str]:
        ...

    def get_health_snapshot(
        self,
        private_stale_sec: int = 120,
        public_stale_sec: int = 120,
    ) -> dict[str, Any]:
        ...

    def get_total_live_pnl(self) -> float:
        ...

    def get_position(self, pybit_symbol: str) -> Optional[dict[str, Any]]:
        ...

    def get_live_price(self, pybit_symbol: str) -> Optional[float]:
        ...

    def get_live_unrealized_pnl(self, pybit_symbol: str) -> Optional[float]:
        ...

    def is_ws_healthy(self, max_stale_sec: int = 120) -> bool:
        ...