from pathlib import Path


def get_project_root() -> Path:
    """
    Resolve project root dynamically.

    Assumes structure:
    Trading_infra/Common/config/path_config.py
    """

    return Path(__file__).resolve().parents[2]


def get_api_dir() -> Path:
    """
    Returns path to API directory.
    """
    return get_project_root() / "API"


def get_api_file_path(file_name: str) -> Path:
    """
    Resolve path to specific API file.

    Example:
    api_mysql_main.txt
    """

    return get_api_dir() / file_name