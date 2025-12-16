"""Configuration (reads from environment variables)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env_str(name: str) -> str | None:
    """Read an env var and strip whitespace; returns None when unset/empty."""
    v = os.environ.get(name)
    return v.strip() if v else None


def _env_path(name: str, default: str) -> Path:
    """Read an env var as a filesystem path; returns default when unset."""
    v = os.environ.get(name)
    return Path(v.strip()) if v else Path(default)


@dataclass(frozen=True)
class Settings:
    raw_path: Path = field(
        default_factory=lambda: _env_path("RAW_PATH", "raw_data.parquet")
    )
    modeled_dir: Path = field(
        default_factory=lambda: _env_path("MODELED_DIR", "modeled")
    )
    api_token: str | None = field(default_factory=lambda: _env_str("API_TOKEN"))
