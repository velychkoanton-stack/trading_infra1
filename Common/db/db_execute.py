import logging
from contextlib import closing
from typing import Any, Iterable, Optional

from mysql.connector.cursor import MySQLCursorDict

from Common.db.db_connect import create_connection
from Common.db.deadlock_retry import run_with_deadlock_retry

logger = logging.getLogger(__name__)


def fetch_all(
    sql: str,
    api_file_name: str,
    params: Optional[tuple | dict] = None,
) -> list[dict[str, Any]]:
    """
    Execute a SELECT and return all rows as list[dict].
    """

    def _operation() -> list[dict[str, Any]]:
        with closing(create_connection(api_file_name)) as conn:
            with closing(conn.cursor(dictionary=True)) as cursor:  # type: MySQLCursorDict
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                return rows if rows is not None else []

    try:
        return run_with_deadlock_retry(_operation)
    except Exception:
        logger.exception("DB fetch_all failed")
        raise


def fetch_one(
    sql: str,
    api_file_name: str,
    params: Optional[tuple | dict] = None,
) -> Optional[dict[str, Any]]:
    """
    Execute a SELECT and return one row as dict or None.
    """

    def _operation() -> Optional[dict[str, Any]]:
        with closing(create_connection(api_file_name)) as conn:
            with closing(conn.cursor(dictionary=True)) as cursor:  # type: MySQLCursorDict
                cursor.execute(sql, params)
                row = cursor.fetchone()
                return row

    try:
        return run_with_deadlock_retry(_operation)
    except Exception:
        logger.exception("DB fetch_one failed")
        raise


def execute(
    sql: str,
    api_file_name: str,
    params: Optional[tuple | dict] = None,
) -> int:
    """
    Execute INSERT/UPDATE/DELETE and return affected row count.
    """

    def _operation() -> int:
        with closing(create_connection(api_file_name)) as conn:
            with closing(conn.cursor(dictionary=True)) as cursor:  # type: MySQLCursorDict
                cursor.execute(sql, params)
                return cursor.rowcount

    try:
        return run_with_deadlock_retry(_operation)
    except Exception:
        logger.exception("DB execute failed")
        raise


def execute_many(
    sql: str,
    api_file_name: str,
    params_seq: Iterable[tuple | dict],
) -> int:
    """
    Execute many INSERT/UPDATE statements and return affected row count.

    Notes:
    - params_seq is materialized once to avoid partial iterator issues on retry
    - intended for short batches
    """
    params_list = list(params_seq)

    def _operation() -> int:
        with closing(create_connection(api_file_name)) as conn:
            with closing(conn.cursor(dictionary=True)) as cursor:  # type: MySQLCursorDict
                cursor.executemany(sql, params_list)
                return cursor.rowcount

    try:
        return run_with_deadlock_retry(_operation)
    except Exception:
        logger.exception("DB execute_many failed")
        raise