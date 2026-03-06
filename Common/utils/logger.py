import logging
from pathlib import Path


def setup_logger(
    logger_name: str,
    log_file_path: str | Path,
    level: int = logging.INFO,
) -> logging.Logger:
    """
    Create and configure a logger with:
    - file handler
    - stream handler
    - consistent formatting

    Notes:
    - Avoids duplicate handlers if called multiple times
    - Creates parent log directory automatically
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.propagate = False

    if logger.handlers:
        return logger

    path = Path(log_file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger