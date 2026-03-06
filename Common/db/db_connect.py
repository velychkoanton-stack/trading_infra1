import logging
from pathlib import Path

import mysql.connector
from mysql.connector import MySQLConnection

from Common.config.api_loader import load_api_file
from Common.config.path_config import get_api_file_path

logger = logging.getLogger(__name__)


def get_mysql_config(api_file_name: str) -> dict:
    """
    Load MySQL credentials from:
    Trading_infra/API/<api_file_name>

    Expected file format:
    DB_HOST=localhost
    DB_USER=root
    DB_PASS=your_password
    DB_NAME=trading_infra
    """
    api_path: Path = get_api_file_path(api_file_name)
    config = load_api_file(api_path)

    required_keys = ("DB_HOST", "DB_USER", "DB_PASS", "DB_NAME")
    missing = [key for key in required_keys if key not in config or not str(config[key]).strip()]

    if missing:
        raise ValueError(
            f"MySQL API file is missing required keys: {missing}. File: {api_path}"
        )

    return {
        "host": config["DB_HOST"],
        "user": config["DB_USER"],
        "password": config["DB_PASS"],
        "database": config["DB_NAME"],
        "autocommit": True,
    }


def create_connection(api_file_name: str) -> MySQLConnection:
    """
    Create a new MySQL connection.

    Connection strategy:
    - one connection per DB operation
    - autocommit enabled
    - dictionary cursor will be created in db_execute.py
    """
    conn_config = get_mysql_config(api_file_name)

    try:
        return mysql.connector.connect(**conn_config)
    except mysql.connector.Error:
        logger.exception("Failed to create MySQL connection using api_file_name=%s", api_file_name)
        raise