"""HTTP client helpers for talking to the EIA API."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from arkham_connector.settings import ConnectorSettings


def get_api_key() -> str:
    """Return the EIA API key from the environment.

    Exits the process with a clear error if the key is missing (matches challenge spec).
    """
    import os
    import sys

    api_key = os.environ.get("EIA_API_KEY")
    if not api_key:
        logging.error(
            "EIA_API_KEY environment variable not set. Cannot authenticate. Exiting."
        )
        sys.exit(1)
    return api_key


def fetch_page_with_retry(
    settings: ConnectorSettings, api_key: str, *, offset: int, limit: int
) -> dict[str, Any] | None:
    """Fetch a single page from the EIA API with retries.

    Returns parsed JSON dict on success, or None if this page is skipped after retries.
    Raises PermissionError for invalid credentials (fatal).
    """
    params = {"api_key": api_key, "offset": offset, "length": limit}

    for attempt in range(1, settings.max_retries + 1):
        try:
            response = requests.get(settings.api_endpoint, params=params, timeout=30)

            if response.status_code in (401, 403):
                logging.error(
                    f"Attempt {attempt}: Authentication failed. Check API key."
                )
                raise PermissionError("Invalid API credentials detected.")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            logging.error(
                f"Attempt {attempt}: HTTP Error fetching data (Offset {offset}): {e}"
            )
            if attempt == settings.max_retries:
                logging.critical(
                    f"Max HTTP retries failed for offset {offset}. Skipping this batch."
                )
                return None

        except requests.exceptions.RequestException as e:
            logging.warning(
                f"Attempt {attempt}: Network failure (Offset {offset}): {e}"
            )
            if attempt == settings.max_retries:
                logging.critical(
                    f"Max network retries failed for offset {offset}. Skipping this batch."
                )
                return None
            delay = settings.retry_delay * (2 ** (attempt - 1))
            time.sleep(delay)

    return None
