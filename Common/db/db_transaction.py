from __future__ import annotations

from typing import Callable, TypeVar

from mysql.connector import MySQLConnection

from Common.db.db_connect import create_connection
from Common.db.deadlock_retry import run_with_deadlock_retry

T = TypeVar("T")


def run_in_transaction(
    api_file_name: str,
    operation: Callable[[MySQLConnection], T],
) -> T:
    """
    Execute DB operation inside explicit transaction.

    Usage example:

        def op(conn):
            cursor = conn.cursor(dictionary=True)
            cursor.execute(...)
            cursor.execute(...)
            return result

        run_in_transaction("api_mysql_main.txt", op)

    Behavior:
    - opens new connection
    - disables autocommit
    - commits on success
    - rollbacks on error
    - supports deadlock retry
    """

    def _wrapped() -> T:
        conn = create_connection(api_file_name)

        try:
            conn.autocommit = False
            result = operation(conn)
            conn.commit()
            return result

        except Exception:
            conn.rollback()
            raise

        finally:
            conn.close()

    return run_with_deadlock_retry(_wrapped)