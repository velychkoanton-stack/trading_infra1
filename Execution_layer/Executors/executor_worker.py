from __future__ import annotations

import random
import threading
import time


class ExecutorWorker:
    def __init__(
        self,
        executor_base,
        scheduler_repo,
        bot_id: str,
        worker_id: str,
        startup_delay_sec: float,
        logger,
    ) -> None:
        self.executor_base = executor_base
        self.scheduler_repo = scheduler_repo
        self.bot_id = bot_id
        self.worker_id = worker_id
        self.startup_delay_sec = startup_delay_sec
        self.logger = logger
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        ...

    def stop(self) -> None:
        ...

    def run_loop(self) -> None:
        ...

    def handle_scheduler_status(self, status: str) -> None:
        ...

    def close_all_bot_open_pairs(self) -> None:
        ...