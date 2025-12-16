"""Refresh pipeline: fetch raw data, rebuild modeled tables, and swap atomically."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

import data_model
from arkham_connector.runner import run_connector
from arkham_connector.settings import ConnectorSettings


def _run_connector_to(raw_path: Path) -> None:
    """Run the extraction step, forcing output to raw_path."""
    # Note: connector API key is still read from env (EIA_API_KEY).
    settings = ConnectorSettings(output_file=raw_path)
    try:
        run_connector(settings)
    except SystemExit as e:
        raise RuntimeError("EIA_API_KEY missing or invalid") from e


def _preview_modeled(*, modeled_dir: Path, head: int) -> dict[str, Any]:
    """Return small previews of the modeled parquet tables (for debugging)."""
    dim_plant = pd.read_parquet(modeled_dir / "dim_plant.parquet").head(head)
    dim_date = pd.read_parquet(modeled_dir / "dim_date.parquet").head(head)
    fact_outage = pd.read_parquet(modeled_dir / "fact_outage.parquet").head(head)

    return {
        "head": head,
        "dim_plant": dim_plant.to_dict(orient="records"),
        "dim_date": dim_date.to_dict(orient="records"),
        "fact_outage": fact_outage.to_dict(orient="records"),
    }


def build_modeled_tables(*, raw_path: Path, modeled_dir: Path) -> dict[str, int]:
    """Rebuild modeled parquet tables from the raw parquet."""
    df_raw = pd.read_parquet(raw_path)

    expected = {"period", "facility", "facilityName", "generator"}
    missing = expected - set(df_raw.columns)
    if missing:
        raise ValueError(f"raw_data is missing required columns: {sorted(missing)}")

    df_raw = df_raw.copy()
    df_raw["period"] = pd.to_datetime(df_raw["period"], errors="coerce").dt.normalize()
    df_raw["facility"] = df_raw["facility"].astype(str)
    df_raw["generator"] = df_raw["generator"].astype(str)

    dim_plant = data_model.build_dim_plant(df_raw)
    dim_date = data_model.build_dim_date(df_raw)
    fact_outage = data_model.build_fact_outage(df_raw, dim_plant=dim_plant)

    modeled_dir.mkdir(parents=True, exist_ok=True)
    dim_plant.to_parquet(modeled_dir / "dim_plant.parquet", index=False)
    dim_date.to_parquet(modeled_dir / "dim_date.parquet", index=False)
    fact_outage.to_parquet(modeled_dir / "fact_outage.parquet", index=False)

    return {
        "dim_plant": int(len(dim_plant)),
        "dim_date": int(len(dim_date)),
        "fact_outage": int(len(fact_outage)),
    }


def atomic_replace_modeled_dir(*, src_dir: Path, dst_dir: Path) -> None:
    """Atomically replace dst_dir with src_dir (same filesystem) and delete old."""
    src_dir = src_dir.resolve()
    dst_dir = dst_dir.resolve()
    parent = dst_dir.parent
    backup_dir = parent / f".{dst_dir.name}.bak-{uuid4().hex}"

    if src_dir.parent != parent:
        raise ValueError("src_dir must be in the same parent directory as dst_dir")

    try:
        if dst_dir.exists():
            dst_dir.rename(backup_dir)
        src_dir.rename(dst_dir)
    except Exception:  # noqa: BLE001
        if backup_dir.exists() and not dst_dir.exists():
            backup_dir.rename(dst_dir)
        raise
    finally:
        if backup_dir.exists():
            shutil.rmtree(backup_dir, ignore_errors=True)


def run_refresh_build_tmp(
    *, raw_path: Path, modeled_dir: Path, preview: bool = False, head: int = 5
) -> tuple[Path, dict[str, Any]]:
    """Build a fresh modeled dataset in a temp directory.

    The caller is responsible for calling `atomic_replace_modeled_dir()` under a lock to
    avoid races with concurrent reads.
    """
    _run_connector_to(raw_path)

    tmp_dir = modeled_dir.parent / f".{modeled_dir.name}.tmp-{uuid4().hex}"
    try:
        counts = build_modeled_tables(raw_path=raw_path, modeled_dir=tmp_dir)
    except Exception:  # noqa: BLE001
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    payload = {
        "status": "ok",
        "raw_path": str(raw_path),
        "modeled_dir": str(modeled_dir),
        **counts,
    }
    if preview:
        payload["preview"] = _preview_modeled(modeled_dir=tmp_dir, head=head)
    return tmp_dir, payload
