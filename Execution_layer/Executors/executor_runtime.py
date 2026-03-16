from __future__ import annotations

import signal
import threading

from Common.config.rules_loader import load_rules_file
from Common.utils.logger import setup_logger
from Execution_layer.Executors.control_worker import ControlWorker
from Execution_layer.Executors.executor_base import ExecutorBase
from Execution_layer.Executors.executor_worker import ExecutorWorker
from Execution_layer.Executors.notification_service import NotificationService
from Execution_layer.Executors.order_manager import OrderManager
from Execution_layer.Executors.position_sizer import PositionSizer
from Execution_layer.Executors.risk_manager import RiskManager
from Execution_layer.Executors.shared_state import SharedExecutorState
from Execution_layer.Support_layer.support_bridge import SupportBridge


class ExecutorRuntime:
    def __init__(self, bot_config) -> None:
        self.bot_config = bot_config

    def build(self) -> None:
        ...

    def start(self) -> None:
        ...

    def stop(self) -> None:
        ...