"""Logging setup utilities for the connector CLI.

Env vars:
- LOG_FILE: if set, also log to this file path
- LOG_FILE_MODE: "a" (append, default) or "w" (overwrite)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path


def setup_logging() -> None:
    """Configure logging to console and (optionally) a file.

    This is intended for CLI usage (e.g., `python connector.py`). Library code should
    generally not configure global logging handlers.
    """
    log_file = os.environ.get("LOG_FILE")
    mode = (os.environ.get("LOG_FILE_MODE") or "a").strip().lower()
    if mode not in {"a", "w"}:
        mode = "a"

    handlers: list[logging.Handler] = [logging.StreamHandler()]

    if log_file:
        path = Path(log_file.strip())
        path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(path, mode=mode, encoding="utf-8"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True,  # CLI: make behavior predictable even if something configured logging earlier.
    )
