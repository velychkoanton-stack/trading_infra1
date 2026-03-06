import gc
from typing import Any


def cleanup_objects(*objects: Any) -> None:
    """
    Drop references to large in-memory objects and force garbage collection.

    Notes:
    - This helps long-running workers reduce retained Python objects.
    - It does not guarantee memory is returned to the OS immediately.
    - Main benefit comes from removing references in worker code before calling this.
    """
    for obj in objects:
        try:
            del obj
        except Exception:
            pass

    gc.collect()


def force_gc() -> None:
    """
    Force Python garbage collection.
    """
    gc.collect()