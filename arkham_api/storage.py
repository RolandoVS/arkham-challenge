"""Parquet IO + derived "outage view" dataframe + simple in-process cache types."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from fastapi import HTTPException

MODELED_FILES: dict[str, str] = {
    "dim_plant": "dim_plant.parquet",
    "dim_date": "dim_date.parquet",
    "fact_outage": "fact_outage.parquet",
}

OUTAGE_VIEW_COLUMNS: list[str] = [
    "OutageKey",
    "EIA_OutageID",
    "PlantKey",
    "EIA_FacilityID",
    "PlantName",
    "Generator",
    "UnitName",
    "DateKey",
    "Date",
    "OutageStartTimestamp",
    "OutageEndTimestamp",
    "OutageDurationHours",
]


def read_modeled_tables(
    modeled_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dim_plant = pd.read_parquet(modeled_dir / MODELED_FILES["dim_plant"])
    dim_date = pd.read_parquet(modeled_dir / MODELED_FILES["dim_date"])
    fact_outage = pd.read_parquet(modeled_dir / MODELED_FILES["fact_outage"])
    return dim_plant, dim_date, fact_outage


def modeled_mtimes(modeled_dir: Path) -> dict[str, float]:
    return {
        name: (modeled_dir / filename).stat().st_mtime
        for name, filename in MODELED_FILES.items()
    }


def build_outage_view(
    dim_plant: pd.DataFrame, dim_date: pd.DataFrame, fact_outage: pd.DataFrame
) -> pd.DataFrame:
    """Join fact with dimensions to return a useful API payload."""
    df = fact_outage.merge(dim_plant, on="PlantKey", how="left", validate="many_to_one")
    df = df.merge(
        dim_date[["DateKey", "Date"]], on="DateKey", how="left", validate="many_to_one"
    )

    existing = [c for c in OUTAGE_VIEW_COLUMNS if c in df.columns]
    return df[existing]


@dataclass
class Cache:
    mtimes: dict[str, float]
    outage_view: pd.DataFrame


def load_outage_view(modeled_dir: Path) -> tuple[pd.DataFrame, dict[str, float]]:
    dim_plant, dim_date, fact_outage = read_modeled_tables(modeled_dir)
    view = build_outage_view(dim_plant, dim_date, fact_outage)
    mtimes = modeled_mtimes(modeled_dir)
    return view, mtimes


def modeled_not_found() -> HTTPException:
    return HTTPException(
        status_code=404,
        detail="Modeled data not found. Run POST /refresh or run data_model.py first.",
    )
