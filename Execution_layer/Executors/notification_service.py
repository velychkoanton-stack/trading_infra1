from __future__ import annotations

from Common.utils.telegram_sender import send_tg_message
from Execution_layer.Executors.models import CandidatePair, OpenPairRecord


class NotificationService:
    def __init__(self, telegram_api_file: str, bot_id: str, logger) -> None:
        self.telegram_api_file = telegram_api_file
        self.bot_id = bot_id
        self.logger = logger

    def send_open(
        self,
        candidate: CandidatePair,
        side_1: str,
        side_2: str,
        total_exposure: float,
    ) -> bool:
        ...

    def send_close(
        self,
        record: OpenPairRecord,
        close_reason: str,
        total_exposure: float,
        realized_pnl: float | None = None,
    ) -> bool:
        ...

    def send_alert(self, message: str) -> bool:
        ...