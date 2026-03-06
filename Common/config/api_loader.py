from pathlib import Path


def load_api_file(file_path: Path) -> dict:
    """
    Load API configuration file in key=value format.

    Example file:

    DB_HOST=localhost
    DB_USER=root
    DB_PASS=secret
    DB_NAME=trading_infra
    """

    config = {}

    if not file_path.exists():
        raise FileNotFoundError(f"API config file not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:

            line = line.strip()

            # skip comments and empty lines
            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                continue

            key, value = line.split("=", 1)

            config[key.strip()] = value.strip()

    return config