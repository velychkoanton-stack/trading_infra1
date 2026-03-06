from pathlib import Path


def load_rules_file(file_path: str | Path) -> dict[str, str]:
    """
    Load a rules/config text file in simple key=value format.

    Example:
        BATCH_SIZE=20
        TIMEFRAME=5m
        ENABLE_REFRESH=true

    Notes:
    - Empty lines are ignored
    - Lines starting with '#' are ignored
    - Values are returned as strings
    - Type casting should be done explicitly in the worker
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Rules file not found: {path}")

    rules: dict[str, str] = {}

    with path.open("r", encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.strip()

            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()

            if key:
                rules[key] = value

    return rules