import logging
import time
from typing import Callable, TypeVar

import mysql.connector

logger = logging.getLogger(__name__)

T = TypeVar("T")

# MySQL retryable lock errors
DEADLOCK_ERROR_CODE = 1213
LOCK_WAIT_TIMEOUT_ERROR_CODE = 1205

# Exponential backoff sequence in seconds
BACKOFF_SCHEDULE = (0.2, 0.5, 1.0, 2.0, 3.0)


def is_retryable_mysql_error(exc: Exception) -> bool:
    """
    Return True only for retryable MySQL lock-related errors.

    Retryable:
    - 1213: Deadlock found when trying to get lock
    - 1205: Lock wait timeout exceeded
    """
    if not isinstance(exc, mysql.connector.Error):
        return False

    return exc.errno in (DEADLOCK_ERROR_CODE, LOCK_WAIT_TIMEOUT_ERROR_CODE)


def run_with_deadlock_retry(func: Callable[[], T], max_retries: int = 5) -> T:
    """
    Execute a DB operation with retry on retryable MySQL lock errors.

    Notes:
    - max_retries=5 means: initial try + up to 5 retries
    - backoff schedule is capped by BACKOFF_SCHEDULE length
    """
    attempt = 0

    while True:
        try:
            return func()

        except Exception as exc:
            if not is_retryable_mysql_error(exc) or attempt >= max_retries:
                raise

            sleep_seconds = BACKOFF_SCHEDULE[min(attempt, len(BACKOFF_SCHEDULE) - 1)]

            logger.error(
                "Retryable MySQL lock error detected. "
                "attempt=%s/%s errno=%s sleep=%.1fs message=%s",
                attempt + 1,
                max_retries,
                getattr(exc, "errno", None),
                sleep_seconds,
                str(exc),
            )

            time.sleep(sleep_seconds)
            attempt += 1