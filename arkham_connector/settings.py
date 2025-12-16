"""Connector configuration (reads from environment variables)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    return int(v.strip()) if v else default


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return bool(int(v.strip()))


def _env_path(name: str, default: str) -> Path:
    v = os.environ.get(name)
    return Path(v.strip()) if v else Path(default)


@dataclass(frozen=True)
class ConnectorSettings:
    # API
    base_url: str = "https://api.eia.gov/v2/"
    outages_route: str = "nuclear-outages/generator-nuclear-outages/data"

    # Paging + retries
    max_limit: int = field(default_factory=lambda: _env_int("MAX_LIMIT", 5000))
    max_records: int = field(default_factory=lambda: _env_int("MAX_RECORDS", 10000))
    max_retries: int = field(default_factory=lambda: _env_int("MAX_RETRIES", 3))
    retry_delay: int = field(default_factory=lambda: _env_int("RETRY_DELAY", 5))

    # Output
    output_file: Path = field(
        default_factory=lambda: _env_path("OUTPUT_FILE", "raw_data.parquet")
    )

    # Incremental mode
    incremental: bool = field(default_factory=lambda: _env_bool("INCREMENTAL", False))
    early_stop_on_old_period: bool = field(
        default_factory=lambda: _env_bool("EARLY_STOP_ON_OLD_PERIOD", True)
    )

    # Validation + dedup keys
    required_fields: tuple[str, ...] = ("period", "facility", "generator")
    key_fields: tuple[str, ...] = ("period", "facility", "generator")

    @property
    def api_endpoint(self) -> str:
        return f"{self.base_url}{self.outages_route}"
