"""Incremental extraction helpers (dedup + early-stop state)."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd


def safe_period_to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.normalize()


def row_keys(df: pd.DataFrame) -> list[tuple[str, str, str]]:
    return list(
        zip(
            df["period"].astype(str),
            df["facility"].astype(str),
            df["generator"].astype(str),
            strict=True,
        )
    )


def load_existing_state(
    output_path: Path, *, key_fields: tuple[str, ...]
) -> tuple[pd.DataFrame | None, set[tuple[str, str, str]], pd.Timestamp | None]:
    """Load existing parquet (if present) and compute dedup keys + max period."""
    if not output_path.exists():
        return None, set(), None

    try:
        existing = pd.read_parquet(output_path)
    except Exception as e:  # noqa: BLE001
        logging.warning(
            f"Failed to read existing parquet at {output_path}; falling back to full extract. Error: {e}"
        )
        return None, set(), None

    if not set(key_fields).issubset(set(existing.columns)):
        logging.warning(
            f"Existing parquet at {output_path} missing {list(key_fields)}; falling back to full extract."
        )
        return None, set(), None

    existing = existing.copy()
    existing["period"] = safe_period_to_datetime(existing["period"])
    existing["facility"] = existing["facility"].astype(str)
    existing["generator"] = existing["generator"].astype(str)

    keys = set(row_keys(existing.dropna(subset=["period"])))
    max_period = existing["period"].max() if existing["period"].notna().any() else None
    return existing, keys, max_period
