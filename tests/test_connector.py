"""
Unit tests for `connector.py`.

These tests focus on:
- Authentication handling (`EIA_API_KEY` required, auth failures are fatal).
- Request construction (API key + pagination params are sent to the API).
- Retry behavior (transient failures retry; auth failures should not).
- End-to-end connector flow at a small scale (pagination + validation + write).

All HTTP I/O is mocked; tests never hit the real EIA API.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import requests

from arkham_connector import client
from arkham_connector.runner import run_connector
from arkham_connector.settings import ConnectorSettings


class TestGetApiKey:
    def test_exits_when_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Missing credentials is a hard stop: the connector should exit immediately.
        monkeypatch.delenv("EIA_API_KEY", raising=False)
        with pytest.raises(SystemExit):
            client.get_api_key()

    def test_returns_value_when_present(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # When present, the API key should be returned unchanged.
        monkeypatch.setenv("EIA_API_KEY", "test-key")
        assert client.get_api_key() == "test-key"


class TestFetchPageWithRetry:
    def test_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Happy-path: a 200 response returns parsed JSON.
        class FakeResponse:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {
                    "response": {
                        "data": [{"period": "2025-01", "facility": "X", "generator": 1}]
                    }
                }

        settings = ConnectorSettings(max_retries=1)
        monkeypatch.setattr(client.requests, "get", lambda *a, **k: FakeResponse())
        data = client.fetch_page_with_retry(settings, "k", offset=0, limit=5)
        assert data["response"]["data"][0]["facility"] == "X"

    def test_sends_api_key_as_query_param(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The EIA API key is sent as a query param (`api_key`), not in headers.
        captured: dict = {}

        class FakeResponse:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"response": {"data": []}}

        def fake_get(url: str, *, params: dict, timeout: int):  # noqa: ARG001
            captured["params"] = params
            captured["timeout"] = timeout
            return FakeResponse()

        monkeypatch.setattr(client.requests, "get", fake_get)
        settings = ConnectorSettings()
        client.fetch_page_with_retry(settings, "the-key", offset=10, limit=25)

        assert captured["params"]["api_key"] == "the-key"
        assert captured["params"]["offset"] == 10
        assert captured["params"]["length"] == 25
        assert captured["timeout"] == 30

    def test_unauthorized_raises_permission_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Auth failures should be treated as fatal (retrying won't help).
        class FakeResponse:
            status_code = 401

            def raise_for_status(self) -> None:  # should not be called for 401/403
                raise AssertionError(
                    "raise_for_status() should not be called for 401/403"
                )

            def json(self) -> dict:
                return {}

        monkeypatch.setattr(client.requests, "get", lambda *a, **k: FakeResponse())
        settings = ConnectorSettings(max_retries=1)
        with pytest.raises(PermissionError):
            client.fetch_page_with_retry(settings, "bad", offset=0, limit=5)

    @pytest.mark.parametrize("status_code", [401, 403])
    def test_auth_errors_do_not_retry(
        self, monkeypatch: pytest.MonkeyPatch, status_code: int
    ) -> None:
        # Even if MAX_RETRIES is high, 401/403 should raise immediately after 1 request.
        calls = {"n": 0}

        class FakeResponse:
            def __init__(self, code: int) -> None:
                self.status_code = code

            def raise_for_status(self) -> None:
                raise AssertionError(
                    "raise_for_status() should not be called for 401/403"
                )

            def json(self) -> dict:
                return {}

        def fake_get(*_a, **_k):
            calls["n"] += 1
            return FakeResponse(status_code)

        def sleep_should_not_be_called(*_a, **_k) -> None:
            raise AssertionError("sleep should not be called for auth errors")

        settings = ConnectorSettings(max_retries=5, retry_delay=0)
        monkeypatch.setattr(client.time, "sleep", sleep_should_not_be_called)
        monkeypatch.setattr(client.requests, "get", fake_get)

        with pytest.raises(PermissionError):
            client.fetch_page_with_retry(settings, "bad", offset=0, limit=5)

        assert calls["n"] == 1

    def test_retries_then_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Transient network failures should retry up to MAX_RETRIES, then skip the page.
        calls = {"n": 0}

        def flaky_get(*_a, **_k):
            calls["n"] += 1
            raise requests.exceptions.RequestException("network down")

        settings = ConnectorSettings(max_retries=2, retry_delay=0)
        monkeypatch.setattr(client.time, "sleep", lambda *_a, **_k: None)
        monkeypatch.setattr(client.requests, "get", flaky_get)

        assert client.fetch_page_with_retry(settings, "k", offset=0, limit=5) is None
        assert calls["n"] == 2

    def test_http_error_retries_then_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Non-auth HTTP failures (e.g., 500) should be retried up to MAX_RETRIES,
        # and then the page should be skipped (return None).
        calls = {"n": 0}

        class FakeResponse:
            status_code = 500

            def raise_for_status(self) -> None:
                raise requests.exceptions.HTTPError("server error")

            def json(self) -> dict:  # pragma: no cover - not used on HTTPError path
                return {}

        def always_500(*_a, **_k):
            calls["n"] += 1
            return FakeResponse()

        settings = ConnectorSettings(max_retries=3)
        monkeypatch.setattr(client.requests, "get", always_500)

        assert client.fetch_page_with_retry(settings, "k", offset=0, limit=5) is None
        assert calls["n"] == 3


class TestRunConnector:
    def test_paginates_validates_and_writes(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        # Lightweight "integration-style" test: run the connector loop with a mocked
        # fetcher and a mocked parquet writer to verify control flow and validation.
        # Ensure the "required credentials" gate is satisfied.
        monkeypatch.setenv("EIA_API_KEY", "k")

        # Keep the run small/fast.
        # Direct output to a temp path.
        out = tmp_path / "out.parquet"
        settings = ConnectorSettings(
            output_file=out, max_limit=2, max_records=10, max_retries=1, retry_delay=0
        )

        # Simulate two pages then end-of-data.
        pages = [
            {
                "response": {
                    "data": [
                        {"period": "2025-01", "facility": "A", "generator": 1},
                        {
                            "period": "2025-01",
                            "facility": None,
                            "generator": 2,
                        },  # dropped
                    ]
                }
            },
            {
                "response": {
                    "data": [{"period": "2025-02", "facility": "B", "generator": 3}]
                }
            },
        ]

        def fake_fetch(_api_key: str, offset: int, limit: int):
            # offset is advanced by MAX_LIMIT in the connector; we don't need it here.
            return pages.pop(0) if pages else {"response": {"data": []}}

        monkeypatch.setattr(
            client, "fetch_page_with_retry", lambda *_a, **_k: fake_fetch("", 0, 0)
        )

        # Avoid depending on parquet engine behavior; just assert we wrote "something"
        # and capture how many rows made it through validation.
        seen = {"rows": None}

        def fake_to_parquet(self, path, index: bool = False):  # noqa: ANN001
            # `self` is a pandas DataFrame; write a tiny sentinel payload.
            seen["rows"] = len(self)
            Path(path).write_bytes(b"PAR1")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet, raising=True)

        run_connector(settings)

        assert out.exists()
        # One record should be dropped due to facility=None
        assert seen["rows"] == 2

    def test_incremental_appends_only_new_rows(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        # Seed an existing parquet with one row.
        out = tmp_path / "raw.parquet"
        pd.DataFrame(
            [{"period": "2025-01-01", "facility": "A", "generator": "1"}]
        ).to_parquet(out, index=False)

        monkeypatch.setenv("EIA_API_KEY", "k")
        settings = ConnectorSettings(
            output_file=out,
            max_limit=2,
            max_records=0,
            incremental=True,
            early_stop_on_old_period=True,
            max_retries=1,
            retry_delay=0,
        )

        # Page 1 includes a duplicate of existing + a new row (newer period).
        # Page 2 includes only duplicates (older/equal periods) -> should early-stop.
        pages = [
            {
                "response": {
                    "data": [
                        {"period": "2025-02-01", "facility": "B", "generator": "2"},
                        {"period": "2025-01-01", "facility": "A", "generator": "1"},
                    ]
                }
            },
            {
                "response": {
                    "data": [
                        {"period": "2025-01-01", "facility": "A", "generator": "1"},
                    ]
                }
            },
        ]

        def fake_fetch(_api_key: str, offset: int, limit: int):  # noqa: ARG001
            return pages.pop(0) if pages else {"response": {"data": []}}

        monkeypatch.setattr(
            client,
            "fetch_page_with_retry",
            lambda *_a, **_k: fake_fetch("", 0, 0),
        )

        run_connector(settings)

        df = pd.read_parquet(out)
        assert len(df) == 2
        # Ensure both rows exist (and no duplicates).
        keys = set(
            zip(
                df["period"].astype(str),
                df["facility"].astype(str),
                df["generator"].astype(str),
                strict=True,
            )
        )
        assert keys == {("2025-01-01", "A", "1"), ("2025-02-01", "B", "2")}
