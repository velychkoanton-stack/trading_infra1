from __future__ import annotations

import random
import threading
import time


class ExecutorWorker:
    """
    Thread wrapper around ExecutorBase.

    Responsibilities:
    - startup delay / jitter
    - read scheduler status
    - obey RUNNING / SLEEP / SL_BLOCK / STOP
    - call executor cycle
    """

    def __init__(
        self,
        executor_base,
        repositories,
        bot_id: str,
        worker_id: str,
        startup_delay_sec: float,
        logger,
    ) -> None:
        self.executor_base = executor_base
        self.repositories = repositories
        self.bot_id = bot_id
        self.worker_id = worker_id
        self.startup_delay_sec = startup_delay_sec
        self.logger = logger

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    # -------------------------------------------------------
    # LIFECYCLE
    # -------------------------------------------------------

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._thread = threading.Thread(
            target=self.run_loop,
            name=self.worker_id,
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    # -------------------------------------------------------
    # MAIN LOOP
    # -------------------------------------------------------

    def run_loop(self) -> None:
        delay = self.startup_delay_sec + random.uniform(0.0, 1.5)

        self.logger.info(
            "ExecutorWorker starting worker_id=%s startup_delay=%.2f",
            self.worker_id,
            delay,
        )

        time.sleep(delay)

        while not self._stop_event.is_set():
            try:
                status = self.repositories.get_scheduler_status(self.bot_id)
                self.handle_scheduler_status(status)

            except Exception:
                self.logger.exception("ExecutorWorker failed worker_id=%s", self.worker_id)

            time.sleep(self.executor_base.bot_config.worker_loop_sec)

        self.logger.info("ExecutorWorker stopped worker_id=%s", self.worker_id)

    # -------------------------------------------------------
    # STATUS HANDLING
    # -------------------------------------------------------

    def handle_scheduler_status(self, status: str) -> None:
        status = str(status).strip().upper()

        if status == "RUNNING":
            self.executor_base.run_cycle()
            return

        if status == "SLEEP":
            self.logger.info("worker_id=%s sleeping by scheduler", self.worker_id)
            return

        if status == "SL_BLOCK":
            self.logger.warning("worker_id=%s received SL_BLOCK", self.worker_id)
            self.close_all_bot_open_pairs()
            return

        if status == "STOP":
            self.logger.warning("worker_id=%s received STOP", self.worker_id)
            self.close_all_bot_open_pairs()
            return

        self.logger.warning(
            "worker_id=%s unknown scheduler status=%s -> treating as SLEEP",
            self.worker_id,
            status,
        )

    # -------------------------------------------------------
    # FORCE CLOSE
    # -------------------------------------------------------

    def close_all_bot_open_pairs(self) -> None:
        open_pairs = self.executor_base.shared_state.get_open_pairs_for_bot(self.bot_id)

        for record in open_pairs:
            try:
                self.executor_base.close_trade(record, "scheduler_force_close")
            except Exception:
                self.logger.exception(
                    "force close failed worker_id=%s uuid=%s",
                    self.worker_id,
                    record.uuid,
                )