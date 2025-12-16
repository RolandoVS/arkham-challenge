"""
EIA Nuclear Outages connector.

This script pages through the EIA v2 API endpoint for generator nuclear outages and
writes the raw results to a Parquet file.

Configuration is controlled via environment variables (optionally from a `.env`):
- EIA_API_KEY (required): API key used for authentication.
- MAX_LIMIT (default 5000): page size per request (EIA v2 max is typically 5000).
- MAX_RECORDS (default 10000): total cap across all pages (set to 0 to disable cap).
- MAX_RETRIES (default 3): retries per page on transient failures.
- RETRY_DELAY (default 5): base delay (seconds) used for exponential backoff.
- OUTPUT_FILE (default raw_data.parquet): output path for Parquet file.
"""

import logging
import os
import sys
import time
from contextlib import suppress
from pathlib import Path

import pandas as pd

# import json for debugging
# import json
import requests
from dotenv import find_dotenv, load_dotenv

# Load environment variables from a local `.env` file (if present).
# Note: In some environments (e.g., sandboxed CI/restricted filesystems), `.env` may be
# present but unreadable, which would otherwise raise at import time and break tests.
_dotenv_path = find_dotenv(usecwd=True)
if _dotenv_path:
    with suppress(PermissionError, OSError):
        load_dotenv(_dotenv_path)

# --- CONFIGURATION ---
BASE_URL = "https://api.eia.gov/v2/"
OUTAGES_ROUTE = "nuclear-outages/generator-nuclear-outages/data"
API_ENDPOINT = f"{BASE_URL}{OUTAGES_ROUTE}"
MAX_LIMIT = int(os.environ.get("MAX_LIMIT", 5000))  # EIA's maximum rows per request
MAX_RECORDS = int(
    os.environ.get("MAX_RECORDS", 10000)
)  # Maximum total records to fetch
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
RETRY_DELAY = int(os.environ.get("RETRY_DELAY", 5))  # seconds
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "raw_data.parquet")
# Minimal schema expectations used for post-fetch validation.
# These fields are present in the current API response; if the API evolves, validation
# is skipped rather than failing the run.
REQUIRED_FIELDS = [
    "period",
    "facility",
    "generator",
]  # period (date), facility (ID), generator (number)

# Configure basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# --- HELPER FUNCTIONS ---
def _get_api_key():
    """Return the EIA API key from the environment.

    Exits the process with a clear error if the key is missing.
    """
    api_key = os.environ.get("EIA_API_KEY")
    if not api_key:
        # Missing credentials is treated as a hard stop: no request can succeed.
        logging.error(
            "EIA_API_KEY environment variable not set. Cannot authenticate. Exiting."
        )
        sys.exit(1)
    return api_key


# --- MAIN FUNCTIONS ---
def _fetch_page_with_retry(api_key, offset, limit):
    """Fetch a single page from the EIA API with retries.

    Args:
        api_key: EIA API key string.
        offset: Number of rows to skip (pagination cursor).
        limit: Number of rows to request for this page.

    Returns:
        Parsed JSON dict on success, or None if this page is skipped after retries.

    Raises:
        PermissionError: If the API indicates invalid/unauthorized credentials.
    """
    params = {
        "api_key": api_key,
        "offset": offset,
        "length": limit,
        # EIA v2 supports specifying returned columns via `data=[...]`; we fetch the
        # default set to keep this connector flexible with evolving schemas.
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Send the request. A short timeout prevents hanging on slow networks.
            response = requests.get(API_ENDPOINT, params=params, timeout=30)

            # Auth failures should stop the pipeline entirely (retrying won't help).
            # Note: must happen BEFORE raise_for_status(), otherwise it would be unreachable.
            if response.status_code in (401, 403):
                logging.error(
                    f"Attempt {attempt}: Authentication failed. Check API key."
                )
                raise PermissionError("Invalid API credentials detected.")

            # Convert HTTP status errors (4xx/5xx) into exceptions handled below.
            response.raise_for_status()

            # Return JSON data if successful
            return response.json()

        except requests.exceptions.HTTPError as e:
            # Handles 4xx/5xx errors (other than 401/403 caught above)
            logging.error(
                f"Attempt {attempt}: HTTP Error fetching data (Offset {offset}): {e}"
            )
            if attempt == MAX_RETRIES:
                # Non-auth HTTP failures are treated as "skip this page" to keep the
                # pipeline moving; downstream consumers can decide how to handle gaps.
                logging.critical(
                    f"Max HTTP retries failed for offset {offset}. Skipping this batch."
                )
                return None

        except requests.exceptions.RequestException as e:
            # Network failures: DNS, connection resets, timeouts, etc.
            logging.warning(
                f"Attempt {attempt}: Network failure (Offset {offset}): {e}"
            )
            if attempt == MAX_RETRIES:
                # After max retries, skip this page (do not crash the entire run).
                logging.critical(
                    f"Max network retries failed for offset {offset}. Skipping this batch."
                )
                return None
            # Exponential backoff: wait longer on each retry
            delay = RETRY_DELAY * (2 ** (attempt - 1))
            time.sleep(delay)

        except PermissionError as e:
            # Credential errors are not recoverable; stop the pipeline immediately.
            raise e

    return (
        None  # Should only be reached if retries fail and it doesn't raise an exception
    )


def run_connector():
    """Run the full extraction pipeline: paginate, validate, and write Parquet."""
    api_key = _get_api_key()  # Will exit if key is missing

    all_records = []
    current_offset = 0

    while True:
        # Progress tracking is approximate: offset increments by MAX_LIMIT, but the API
        # can return fewer than requested near the end.
        if MAX_RECORDS:
            progress = min(100, int((len(all_records) / MAX_RECORDS) * 100))
            logging.info(
                f"Progress: {progress}% | Fetching page starting at offset: {current_offset} with limit: {MAX_LIMIT}"
            )
        else:
            logging.info(
                f"Fetched {len(all_records)} records so far | Fetching page starting at offset: {current_offset} with limit: {MAX_LIMIT}"
            )

        # Call the robust fetcher
        page_data = _fetch_page_with_retry(api_key, current_offset, MAX_LIMIT)

        if not page_data or "response" not in page_data:
            # If the fetcher skipped the page (None) or the response structure is
            # unexpected, stop paginating to avoid writing partial/ambiguous data.
            logging.warning(
                f"No response or empty response for offset {current_offset}. Halting pagination."
            )
            break

        # EIA v2 data is typically under response['data']
        records = page_data.get("response", {}).get("data", [])

        # Print first element of API response for verification (only on first page)
        # if current_offset == 0 and records:
        #     print("\n" + "="*80)
        #     print("First Element from API Response:")
        #     print("="*80)
        #     print(json.dumps(records[0], indent=2))
        #     print("="*80 + "\n")

        if not records:
            # End condition: API indicates there are no more records.
            logging.info("Pagination complete: API returned an empty list of records.")
            break

        all_records.extend(records)
        current_offset += MAX_LIMIT

        # End condition: MAX_RECORDS cap reached (if enabled).
        if len(all_records) >= MAX_RECORDS:
            logging.info(
                f"Reached maximum records limit ({MAX_RECORDS}). Stopping pagination."
            )
            # Trim to exact limit if we exceeded it
            if len(all_records) > MAX_RECORDS:
                all_records = all_records[:MAX_RECORDS]
            break

        # End condition: short page implies we've reached the end of the dataset.
        if len(records) < MAX_LIMIT:
            logging.info(
                f"Received {len(records)} records, less than limit. Pagination complete."
            )
            break

    # --- Final Validation and Storage ---
    if not all_records:
        logging.critical(
            "Pipeline finished, but zero records were collected. Nothing to save."
        )
        return

    # 1. Load into DataFrame
    df = pd.DataFrame(all_records)

    # Log DataFrame info
    logging.info(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns")
    logging.info(f"Columns: {list(df.columns)}")

    # 2. Data validation (best-effort): drop rows missing required identifier fields.
    existing_required_fields = [
        field for field in REQUIRED_FIELDS if field in df.columns
    ]

    if existing_required_fields:
        initial_count = len(df)
        df.dropna(subset=existing_required_fields, inplace=True)
        dropped_count = initial_count - len(df)
        logging.info(
            f"Dropped {dropped_count} records with null values in: {existing_required_fields}"
        )
    else:
        logging.warning(
            f"Expected fields {REQUIRED_FIELDS} not found in data. Skipping validation."
        )
        logging.info("Available fields for validation: " + ", ".join(df.columns))

    # 3. Final Logging
    logging.info(f"Total records collected: {len(all_records)}")
    logging.info(f"Total valid records after validation: {len(df)}")

    # 4. Critical Requirement: Save data to Parquet
    output_path = Path(OUTPUT_FILE)
    try:
        # Ensure output directory exists (supports nested paths like data/raw/output.parquet).
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        file_size = output_path.stat().st_size / (1024 * 1024)  # Size in MB
        logging.info(
            f"Successfully saved raw data to '{output_path}' ({file_size:.2f} MB)."
        )
        logging.info(
            f"Pipeline completed successfully: {len(df)} records extracted and saved."
        )
    except Exception as e:
        logging.error(f"Failed to write Parquet file: {e}")
        raise  # Re-raise to indicate failure

    # Print the DataFrame (only for debugging)
    # print("\n" + "="*80)
    # print("DataFrame Summary:")
    # print("="*80)
    # print(df)
    # print("="*80 + "\n")


if __name__ == "__main__":
    logging.info("--- Starting EIA Data Connector Pipeline ---")
    run_connector()
    logging.info("--- Pipeline Finished ---")
