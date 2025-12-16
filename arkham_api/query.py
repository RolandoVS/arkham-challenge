"""Filtering + pagination helpers for the /data endpoint."""

from __future__ import annotations

import pandas as pd


def apply_filters(
    df: pd.DataFrame,
    *,
    facility_id: str | None,
    generator: str | None,
    plant_key: int | None,
    plant_name: str | None,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    if facility_id:
        df = df[df["EIA_FacilityID"].astype(str) == str(facility_id)]
    if generator:
        df = df[df["Generator"].astype(str) == str(generator)]
    if plant_key is not None:
        df = df[df["PlantKey"] == plant_key]
    if plant_name:
        df = df[
            df["PlantName"].astype(str).str.contains(plant_name, case=False, na=False)
        ]

    if start_date:
        start = pd.to_datetime(start_date, errors="raise").normalize()
        df = df[pd.to_datetime(df["OutageStartTimestamp"]).dt.normalize() >= start]
    if end_date:
        end = pd.to_datetime(end_date, errors="raise").normalize()
        df = df[pd.to_datetime(df["OutageStartTimestamp"]).dt.normalize() <= end]

    return df


def paginate(df: pd.DataFrame, *, page: int, limit: int) -> tuple[pd.DataFrame, int]:
    total = int(len(df))
    start = (page - 1) * limit
    end = start + limit
    return df.iloc[start:end], total
