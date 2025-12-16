"""Optional dotenv loading for local development.

Important: this is intentionally not run at import-time by the library code, to avoid
surprising side effects during tests/CI. Call `load_env()` from the CLI script.
"""

from __future__ import annotations

from contextlib import suppress

from dotenv import find_dotenv, load_dotenv


def load_env() -> None:
    dotenv_path = find_dotenv(usecwd=True)
    if dotenv_path:
        with suppress(PermissionError, OSError):
            load_dotenv(dotenv_path)
