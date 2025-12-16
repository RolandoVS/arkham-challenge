from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import api
from arkham_api import refresh as refresh_mod
from arkham_api import storage as storage_mod


def _write_modeled(modeled_dir: Path) -> None:
    modeled_dir.mkdir(parents=True, exist_ok=True)

    dim_plant = pd.DataFrame(
        [
            {
                "PlantKey": 1,
                "EIA_FacilityID": "100",
                "PlantName": "Plant A",
                "UnitName": "Unit 1",
                "Generator": "1",
            },
            {
                "PlantKey": 2,
                "EIA_FacilityID": "200",
                "PlantName": "Plant B",
                "UnitName": "Unit 2",
                "Generator": "2",
            },
        ]
    )
    dim_date = pd.DataFrame(
        [
            {
                "DateKey": 20250101,
                "Date": pd.Timestamp("2025-01-01"),
                "Year": 2025,
                "Month": 1,
                "DayOfWeek": "Wednesday",
                "IsWeekend": False,
            },
            {
                "DateKey": 20250105,
                "Date": pd.Timestamp("2025-01-05"),
                "Year": 2025,
                "Month": 1,
                "DayOfWeek": "Sunday",
                "IsWeekend": True,
            },
        ]
    )
    fact_outage = pd.DataFrame(
        [
            {
                "OutageKey": 1,
                "PlantKey": 1,
                "DateKey": 20250101,
                "OutageStartTimestamp": pd.Timestamp("2025-01-01"),
                "OutageEndTimestamp": pd.Timestamp("2025-01-02"),
                "OutageDurationHours": 24.0,
                "EIA_OutageID": "100-1-20250101",
            },
            {
                "OutageKey": 2,
                "PlantKey": 2,
                "DateKey": 20250105,
                "OutageStartTimestamp": pd.Timestamp("2025-01-05"),
                "OutageEndTimestamp": pd.Timestamp("2025-01-06"),
                "OutageDurationHours": 24.0,
                "EIA_OutageID": "200-2-20250105",
            },
        ]
    )

    dim_plant.to_parquet(modeled_dir / "dim_plant.parquet", index=False)
    dim_date.to_parquet(modeled_dir / "dim_date.parquet", index=False)
    fact_outage.to_parquet(modeled_dir / "fact_outage.parquet", index=False)


def test_data_endpoint_filters_and_paginates(tmp_path: Path) -> None:
    modeled_dir = tmp_path / "modeled"
    _write_modeled(modeled_dir)
    app = api.create_app(
        api.Settings(
            raw_path=tmp_path / "raw.parquet",
            modeled_dir=modeled_dir,
            api_token="test-token",
        )
    )
    client = TestClient(app)
    headers = {"Authorization": "Bearer test-token"}

    # No filters: should return both.
    resp = client.get("/data", params={"page": 1, "limit": 10}, headers=headers)
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["total"] == 2
    assert len(payload["data"]) == 2

    # Filter by facility_id.
    resp = client.get("/data", params={"facility_id": "100"}, headers=headers)
    payload = resp.json()
    assert payload["total"] == 1
    assert payload["data"][0]["EIA_FacilityID"] == "100"

    # Date filter (start_date/end_date apply to outage start).
    resp = client.get(
        "/data",
        params={"start_date": "2025-01-05", "end_date": "2025-01-05"},
        headers=headers,
    )
    payload = resp.json()
    assert payload["total"] == 1
    assert payload["data"][0]["DateKey"] == 20250105

    # Pagination.
    resp = client.get("/data", params={"page": 1, "limit": 1}, headers=headers)
    payload = resp.json()
    assert payload["total"] == 2
    assert len(payload["data"]) == 1


def test_refresh_endpoint_calls_pipeline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    modeled_dir = tmp_path / "modeled"
    raw_path = tmp_path / "raw.parquet"
    app = api.create_app(
        api.Settings(raw_path=raw_path, modeled_dir=modeled_dir, api_token="t")
    )
    client = TestClient(app)
    headers = {"Authorization": "Bearer t"}

    called = {"n": 0}

    def fake_refresh_build_tmp(*, raw_path: Path, modeled_dir: Path, **_kwargs):  # noqa: ANN001
        called["n"] += 1
        tmp_dir = modeled_dir.parent / ".modeled.tmp-test"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "status": "ok",
            "raw_path": str(raw_path),
            "modeled_dir": str(modeled_dir),
        }
        return tmp_dir, payload

    def fake_atomic_replace(*, src_dir: Path, dst_dir: Path) -> None:  # noqa: ARG001
        return None

    monkeypatch.setattr(refresh_mod, "run_refresh_build_tmp", fake_refresh_build_tmp)
    monkeypatch.setattr(refresh_mod, "atomic_replace_modeled_dir", fake_atomic_replace)

    resp = client.post("/refresh", headers=headers)
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert called["n"] == 1


def test_auth_required_when_token_configured(tmp_path: Path) -> None:
    modeled_dir = tmp_path / "modeled"
    _write_modeled(modeled_dir)
    app = api.create_app(
        api.Settings(
            raw_path=tmp_path / "raw.parquet", modeled_dir=modeled_dir, api_token="t"
        )
    )
    client = TestClient(app)

    resp = client.get("/data")
    assert resp.status_code == 401

    resp = client.get("/data", headers={"Authorization": "Bearer wrong"})
    assert resp.status_code == 403

    resp = client.post("/refresh")
    assert resp.status_code == 401


def test_data_uses_cache_and_invalidates_on_file_change(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import os
    import time

    modeled_dir = tmp_path / "modeled"
    _write_modeled(modeled_dir)

    app = api.create_app(
        api.Settings(
            raw_path=tmp_path / "raw.parquet", modeled_dir=modeled_dir, api_token="t"
        )
    )
    client = TestClient(app)
    headers = {"Authorization": "Bearer t"}

    calls = {"n": 0}
    real_read = storage_mod.pd.read_parquet

    def counted_read(*args, **kwargs):  # noqa: ANN001
        calls["n"] += 1
        return real_read(*args, **kwargs)

    monkeypatch.setattr(storage_mod.pd, "read_parquet", counted_read)

    # First request loads 3 parquets (dim_plant, dim_date, fact_outage).
    resp = client.get("/data", headers=headers)
    assert resp.status_code == 200
    assert calls["n"] == 3

    # Second request should hit cache (no extra parquet reads).
    resp = client.get("/data", params={"facility_id": "100"}, headers=headers)
    assert resp.status_code == 200
    assert calls["n"] == 3

    # Touch a modeled file (rewrite) to bump mtime; cache should invalidate and reload.
    p = modeled_dir / "fact_outage.parquet"
    now = time.time() + 5
    os.utime(p, (now, now))

    resp = client.get("/data", headers=headers)
    assert resp.status_code == 200
    assert calls["n"] == 6
