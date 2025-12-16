"""
Unit tests for `data_model.py`.

We validate:
- Dimension key uniqueness + stable/expected columns.
- Fact event collapsing (consecutive days collapse to one event; gaps create new events).
- Parquet write flow via `data_model.main()` (integration-style, small in-memory dataset).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import data_model


def _raw_df(rows: list[dict]) -> pd.DataFrame:
    """Helper to create a raw dataframe in the normalized shape expected by builders."""
    df = pd.DataFrame(rows)
    df["period"] = pd.to_datetime(df["period"], errors="raise").dt.normalize()
    df["facility"] = df["facility"].astype(str)
    df["generator"] = df["generator"].astype(str)
    return df


class TestBuildDimPlant:
    def test_columns_and_keys(self) -> None:
        df_raw = _raw_df(
            [
                {
                    "period": "2025-01-01",
                    "facility": "200",
                    "facilityName": "Plant B",
                    "generator": "2",
                },
                {
                    "period": "2025-01-02",
                    "facility": "100",
                    "facilityName": "Plant A",
                    "generator": "1",
                },
            ]
        )

        dim = data_model.build_dim_plant(df_raw)

        assert list(dim.columns) == [
            "PlantKey",
            "EIA_FacilityID",
            "PlantName",
            "UnitName",
            "Generator",
        ]
        assert dim["PlantKey"].is_unique
        assert dim[["EIA_FacilityID", "Generator"]].duplicated().sum() == 0

        # Sorted by facility then generator (string sort), with sequential surrogate keys.
        assert dim.iloc[0]["EIA_FacilityID"] == "100"
        assert dim.iloc[0]["Generator"] == "1"
        assert dim.iloc[0]["PlantKey"] == 1
        assert dim.iloc[0]["UnitName"] == "Unit 1"


class TestBuildDimDate:
    def test_date_keys_and_weekends(self) -> None:
        df_raw = _raw_df(
            [
                {
                    "period": "2025-03-28",  # Friday
                    "facility": "100",
                    "facilityName": "Plant A",
                    "generator": "1",
                },
                {
                    "period": "2025-03-29",  # Saturday
                    "facility": "100",
                    "facilityName": "Plant A",
                    "generator": "1",
                },
            ]
        )

        dim = data_model.build_dim_date(df_raw)
        assert dim["DateKey"].is_unique
        assert dim["Date"].is_unique

        row_fri = dim.loc[dim["Date"] == pd.Timestamp("2025-03-28")].iloc[0]
        assert row_fri["DateKey"] == 20250328
        assert row_fri["DayOfWeek"] == "Friday"
        assert bool(row_fri["IsWeekend"]) is False

        row_sat = dim.loc[dim["Date"] == pd.Timestamp("2025-03-29")].iloc[0]
        assert row_sat["DateKey"] == 20250329
        assert row_sat["DayOfWeek"] == "Saturday"
        assert bool(row_sat["IsWeekend"]) is True


class TestBuildFactOutage:
    def test_collapses_consecutive_days_and_splits_on_gaps(self) -> None:
        # For one generator, create two runs: 1-3 and 5-5 (gap at day 4).
        df_raw = _raw_df(
            [
                {
                    "period": "2025-01-01",
                    "facility": "100",
                    "facilityName": "Plant A",
                    "generator": "1",
                },
                {
                    "period": "2025-01-02",
                    "facility": "100",
                    "facilityName": "Plant A",
                    "generator": "1",
                },
                {
                    "period": "2025-01-03",
                    "facility": "100",
                    "facilityName": "Plant A",
                    "generator": "1",
                },
                {
                    "period": "2025-01-05",
                    "facility": "100",
                    "facilityName": "Plant A",
                    "generator": "1",
                },
                # Another generator with a single day.
                {
                    "period": "2025-01-02",
                    "facility": "200",
                    "facilityName": "Plant B",
                    "generator": "2",
                },
            ]
        )

        dim_plant = data_model.build_dim_plant(df_raw)
        fact = data_model.build_fact_outage(df_raw, dim_plant=dim_plant)

        assert list(fact.columns) == [
            "OutageKey",
            "PlantKey",
            "DateKey",
            "OutageStartTimestamp",
            "OutageEndTimestamp",
            "OutageDurationHours",
            "EIA_OutageID",
        ]

        # We expect 3 outage events total:
        # - Plant A gen 1: two events
        # - Plant B gen 2: one event
        assert len(fact) == 3
        assert fact["OutageKey"].is_unique
        assert fact["EIA_OutageID"].is_unique

        # Find Plant A gen 1's PlantKey.
        plant_a_key = int(
            dim_plant.loc[
                (dim_plant["EIA_FacilityID"] == "100")
                & (dim_plant["Generator"] == "1"),
                "PlantKey",
            ].iloc[0]
        )

        a_events = fact.loc[fact["PlantKey"] == plant_a_key].sort_values("DateKey")
        assert len(a_events) == 2

        # Event 1: 2025-01-01..2025-01-04 exclusive => 72 hours
        e1 = a_events.iloc[0]
        assert e1["DateKey"] == 20250101
        assert e1["OutageStartTimestamp"] == pd.Timestamp("2025-01-01")
        assert e1["OutageEndTimestamp"] == pd.Timestamp("2025-01-04")
        assert e1["OutageDurationHours"] == pytest.approx(72.0)
        assert e1["EIA_OutageID"] == "100-1-20250101"

        # Event 2: 2025-01-05..2025-01-06 exclusive => 24 hours
        e2 = a_events.iloc[1]
        assert e2["DateKey"] == 20250105
        assert e2["OutageStartTimestamp"] == pd.Timestamp("2025-01-05")
        assert e2["OutageEndTimestamp"] == pd.Timestamp("2025-01-06")
        assert e2["OutageDurationHours"] == pytest.approx(24.0)
        assert e2["EIA_OutageID"] == "100-1-20250105"


class TestMainIntegration:
    def test_main_writes_modeled_parquets_minimal_schema(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        capsys: pytest.CaptureFixture,
    ) -> None:
        df_raw = _raw_df(
            [
                {
                    "period": "2025-01-01",
                    "facility": "100",
                    "facilityName": "Plant A",
                    "generator": "1",
                },
                {
                    "period": "2025-01-02",
                    "facility": "100",
                    "facilityName": "Plant A",
                    "generator": "1",
                },
            ]
        )
        inp = tmp_path / "raw.parquet"
        out_dir = tmp_path / "modeled"
        df_raw.to_parquet(inp, index=False)

        monkeypatch.setattr(
            "sys.argv",
            [
                "data_model.py",
                "--input",
                str(inp),
                "--output-dir",
                str(out_dir),
                "--print-raw",
                "--print-modeled",
                "--head",
                "5",
            ],
        )

        rc = data_model.main()
        assert rc == 0

        dim_plant_p = out_dir / "dim_plant.parquet"
        dim_date_p = out_dir / "dim_date.parquet"
        fact_outage_p = out_dir / "fact_outage.parquet"
        assert dim_plant_p.exists()
        assert dim_date_p.exists()
        assert fact_outage_p.exists()

        dim_plant = pd.read_parquet(dim_plant_p)
        fact_outage = pd.read_parquet(fact_outage_p)

        # Minimal schema: no placeholder columns.
        assert set(dim_plant.columns) == {
            "PlantKey",
            "EIA_FacilityID",
            "PlantName",
            "UnitName",
            "Generator",
        }
        assert set(fact_outage.columns) == {
            "OutageKey",
            "PlantKey",
            "DateKey",
            "OutageStartTimestamp",
            "OutageEndTimestamp",
            "OutageDurationHours",
            "EIA_OutageID",
        }

        out = capsys.readouterr().out
        assert "=== RAW DATA (preview) ===" in out
        assert "=== DIM_PLANT (preview) ===" in out
        assert "=== FACT_OUTAGE (preview) ===" in out
