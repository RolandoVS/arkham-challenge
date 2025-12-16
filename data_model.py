"""
Data model builder

This project currently extracts *daily* generator-level outage observations from EIA.
The raw file (`raw_data.parquet`) has these columns:
- period (YYYY-MM-DD)
- facility (EIA facility id)
- facilityName (plant name)
- generator (unit number)

Star schema (2-4 tables max):

- DimPlant (one row per facility+generator)
- DimDate  (one row per calendar date)
- FactOutage (one row per *outage event* per generator)

Because the raw feed is daily, we derive "outage events" by collapsing consecutive
`period` dates for the same generator into a single event:
- OutageStartTimestamp = first date in the run (00:00:00)
- OutageEndTimestamp   = day *after* the last date in the run (00:00:00)
  (i.e., an exclusive end timestamp)
- OutageDurationHours  = (OutageEndTimestamp - OutageStartTimestamp) in hours

Columns like capacity affected, reason, reactor type, etc. are not present in the
current raw extract, so this script only outputs columns it can actually derive from
the raw feed.

ER diagram (Mermaid):

```mermaid
erDiagram
    DIM_PLANT ||--o{ FACT_OUTAGE : has
    DIM_DATE  ||--o{ FACT_OUTAGE : starts_on

    DIM_PLANT {
        int PlantKey PK
        string EIA_FacilityID
        string PlantName
        string UnitName
        string State
        int UnitCapacityMW
        string ReactorType
    }

    DIM_DATE {
        int DateKey PK
        date Date
        int Year
        int Month
        string DayOfWeek
        boolean IsWeekend
    }

    FACT_OUTAGE {
        int OutageKey PK
        int PlantKey FK
        int DateKey FK
        datetime OutageStartTimestamp
        datetime OutageEndTimestamp
        float OutageDurationHours
        float CapacityAffectedMW
        string OutageReasonCode
        string EIA_OutageID
    }
```

Relationships:
- DimPlant.PlantKey (PK) -> FactOutage.PlantKey (FK): 1-to-many
- DimDate.DateKey (PK)   -> FactOutage.DateKey (FK): 1-to-many (DateKey is the outage *start* date)
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _make_date_key(dt: pd.Series) -> pd.Series:
    """Convert a datetime-like series to YYYYMMDD integer keys."""
    return dt.dt.strftime("%Y%m%d").astype("int64")


def build_dim_plant(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Build DimPlant with a surrogate PlantKey."""
    plants = (
        df_raw[["facility", "facilityName", "generator"]]
        .drop_duplicates()
        .rename(
            columns={
                "facility": "EIA_FacilityID",
                "facilityName": "PlantName",
                "generator": "Generator",
            }
        )
        .sort_values(["EIA_FacilityID", "Generator"], kind="stable")
        .reset_index(drop=True)
    )

    plants.insert(0, "PlantKey", plants.index.astype("int64") + 1)

    plants["UnitName"] = "Unit " + plants["Generator"].astype(str)

    # Keep the raw generator id as string-ish for joins/traceability.
    plants["Generator"] = plants["Generator"].astype(str)
    plants["EIA_FacilityID"] = plants["EIA_FacilityID"].astype(str)

    return plants[["PlantKey", "EIA_FacilityID", "PlantName", "UnitName", "Generator"]]


def build_dim_date(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Build DimDate from the raw `period` column."""
    dates = (
        pd.DataFrame({"Date": df_raw["period"].dropna().drop_duplicates()})
        .sort_values("Date", kind="stable")
        .reset_index(drop=True)
    )

    dates["DateKey"] = _make_date_key(dates["Date"])
    dates["Year"] = dates["Date"].dt.year.astype("int64")
    dates["Month"] = dates["Date"].dt.month.astype("int64")
    dates["DayOfWeek"] = dates["Date"].dt.day_name()
    dates["IsWeekend"] = dates["Date"].dt.dayofweek >= 5

    return dates[["DateKey", "Date", "Year", "Month", "DayOfWeek", "IsWeekend"]]


def build_fact_outage(df_raw: pd.DataFrame, *, dim_plant: pd.DataFrame) -> pd.DataFrame:
    """Build FactOutage by collapsing consecutive daily outages into events."""
    # Join surrogate PlantKey onto raw rows.
    raw_with_keys = df_raw.merge(
        dim_plant[["PlantKey", "EIA_FacilityID", "Generator"]],
        left_on=["facility", "generator"],
        right_on=["EIA_FacilityID", "Generator"],
        how="inner",
        validate="many_to_one",
    ).copy()

    raw_with_keys = raw_with_keys.sort_values(["PlantKey", "period"], kind="stable")

    # Identify breaks between consecutive days within each PlantKey.
    prev = raw_with_keys.groupby("PlantKey")["period"].shift(1)
    gap_days = (raw_with_keys["period"] - prev).dt.days
    is_new_event = prev.isna() | (gap_days != 1)
    raw_with_keys["_event_id"] = is_new_event.groupby(
        raw_with_keys["PlantKey"]
    ).cumsum()

    # Collapse to one row per (PlantKey, event).
    events = (
        raw_with_keys.groupby(["PlantKey", "_event_id"], as_index=False)
        .agg(
            OutageStartDate=("period", "min"),
            OutageLastDate=("period", "max"),
        )
        .sort_values(["PlantKey", "OutageStartDate"], kind="stable")
        .reset_index(drop=True)
    )

    # Exclusive end timestamp = next day at midnight.
    events["OutageStartTimestamp"] = events["OutageStartDate"]
    events["OutageEndTimestamp"] = events["OutageLastDate"] + pd.Timedelta(days=1)
    events["OutageDurationHours"] = (
        events["OutageEndTimestamp"] - events["OutageStartTimestamp"]
    ).dt.total_seconds() / 3600.0

    # DateKey is the outage start date.
    events["DateKey"] = _make_date_key(events["OutageStartTimestamp"])

    # Attach natural identifiers for traceability / building EIA_OutageID.
    events = events.merge(
        dim_plant[["PlantKey", "EIA_FacilityID", "Generator"]],
        on="PlantKey",
        how="left",
        validate="many_to_one",
    )

    events["EIA_OutageID"] = (
        events["EIA_FacilityID"].astype(str)
        + "-"
        + events["Generator"].astype(str)
        + "-"
        + events["DateKey"].astype(str)
    )

    events.insert(0, "OutageKey", events.index.astype("int64") + 1)

    return events[
        [
            "OutageKey",
            "PlantKey",
            "DateKey",
            "OutageStartTimestamp",
            "OutageEndTimestamp",
            "OutageDurationHours",
            "EIA_OutageID",
        ]
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Build star-schema model tables.")
    parser.add_argument(
        "--input",
        default="raw_data.parquet",
        help="Path to raw parquet produced by connector (default: raw_data.parquet)",
    )
    parser.add_argument(
        "--output-dir",
        default="modeled",
        help="Directory to write modeled parquet files (default: modeled/)",
    )
    parser.add_argument(
        "--print-raw",
        action="store_true",
        help="Print a preview of the raw input dataframe (for debugging).",
    )
    parser.add_argument(
        "--print-modeled",
        action="store_true",
        help="Print a preview of the modeled tables (for debugging).",
    )
    parser.add_argument(
        "--head",
        type=int,
        default=20,
        help="Number of rows to print when using --print-raw/--print-modeled (default: 20).",
    )
    parser.add_argument(
        "--print-max-cols",
        type=int,
        default=50,
        help="Max columns to display when printing (default: 50).",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df_raw = pd.read_parquet(input_path)

    if args.print_raw:
        with pd.option_context(
            "display.max_rows",
            args.head,
            "display.max_columns",
            args.print_max_cols,
            "display.width",
            120,
            "display.max_colwidth",
            80,
        ):
            print("\n=== RAW DATA (preview) ===")
            print(f"rows={len(df_raw)} cols={len(df_raw.columns)}")
            print(df_raw.head(args.head))

    expected = {"period", "facility", "facilityName", "generator"}
    missing = expected - set(df_raw.columns)
    if missing:
        raise ValueError(f"raw_data is missing required columns: {sorted(missing)}")

    # Normalize types.
    df_raw = df_raw.copy()
    df_raw["period"] = pd.to_datetime(df_raw["period"], errors="coerce").dt.normalize()
    df_raw["facility"] = df_raw["facility"].astype(str)
    df_raw["generator"] = df_raw["generator"].astype(str)

    dim_plant = build_dim_plant(df_raw)
    dim_date = build_dim_date(df_raw)
    fact_outage = build_fact_outage(df_raw, dim_plant=dim_plant)

    dim_plant.to_parquet(output_dir / "dim_plant.parquet", index=False)
    dim_date.to_parquet(output_dir / "dim_date.parquet", index=False)
    fact_outage.to_parquet(output_dir / "fact_outage.parquet", index=False)

    if args.print_modeled:
        with pd.option_context(
            "display.max_rows",
            args.head,
            "display.max_columns",
            args.print_max_cols,
            "display.width",
            120,
            "display.max_colwidth",
            80,
        ):
            print("\n=== DIM_PLANT (preview) ===")
            print(f"rows={len(dim_plant)} cols={len(dim_plant.columns)}")
            print(dim_plant.head(args.head))

            print("\n=== DIM_DATE (preview) ===")
            print(f"rows={len(dim_date)} cols={len(dim_date.columns)}")
            print(dim_date.head(args.head))

            print("\n=== FACT_OUTAGE (preview) ===")
            print(f"rows={len(fact_outage)} cols={len(fact_outage.columns)}")
            print(fact_outage.head(args.head))

    print(
        "Wrote modeled tables:",
        f"dim_plant={len(dim_plant)} rows,",
        f"dim_date={len(dim_date)} rows,",
        f"fact_outage={len(fact_outage)} rows",
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
