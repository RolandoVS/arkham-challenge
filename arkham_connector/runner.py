"""High-level connector runner: paginate, validate, (optionally) merge incrementally, write parquet."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from arkham_connector import client, incremental
from arkham_connector.settings import ConnectorSettings


def run_connector(settings: ConnectorSettings | None = None) -> None:
    settings = settings or ConnectorSettings()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    api_key = client.get_api_key()

    output_path = settings.output_file
    existing_df: pd.DataFrame | None = None
    existing_keys: set[tuple[str, str, str]] = set()
    existing_max_period: pd.Timestamp | None = None
    incremental_active = False

    if settings.incremental:
        existing_df, existing_keys, existing_max_period = (
            incremental.load_existing_state(output_path, key_fields=settings.key_fields)
        )
        incremental_active = existing_df is not None
        if incremental_active:
            logging.info(
                f"Incremental mode enabled. Existing rows={len(existing_df)} max_period={existing_max_period}"
            )

    all_records: list[dict[str, Any]] = []
    new_records: list[dict[str, Any]] = []
    current_offset = 0

    while True:
        if settings.max_records:
            progress = min(100, int((len(all_records) / settings.max_records) * 100))
            logging.info(
                f"Progress: {progress}% | Fetching page starting at offset: {current_offset} with limit: {settings.max_limit}"
            )
        else:
            logging.info(
                f"Fetched {len(all_records)} records so far | Fetching page starting at offset: {current_offset} with limit: {settings.max_limit}"
            )

        page_data = client.fetch_page_with_retry(
            settings, api_key, offset=current_offset, limit=settings.max_limit
        )

        if not page_data or "response" not in page_data:
            logging.warning(
                f"No response or empty response for offset {current_offset}. Halting pagination."
            )
            break

        records = page_data.get("response", {}).get("data", [])
        if not records:
            logging.info("Pagination complete: API returned an empty list of records.")
            break

        if incremental_active:
            page_df = pd.DataFrame(records)
            if set(settings.key_fields).issubset(set(page_df.columns)):
                page_df = page_df.copy()
                page_df["period"] = incremental.safe_period_to_datetime(
                    page_df["period"]
                )
                page_df["facility"] = page_df["facility"].astype(str)
                page_df["generator"] = page_df["generator"].astype(str)
                page_df = page_df.dropna(subset=["period", "facility", "generator"])

                keys = incremental.row_keys(page_df)
                is_new = [k not in existing_keys for k in keys]
                page_new_df = page_df.loc[is_new]

                if not page_new_df.empty:
                    new_records.extend(page_new_df.to_dict(orient="records"))
                    existing_keys.update(incremental.row_keys(page_new_df))

                if (
                    settings.early_stop_on_old_period
                    and existing_max_period is not None
                    and page_df["period"].notna().any()
                ):
                    page_max = page_df["period"].max()
                    if (
                        page_max is not None
                        and page_max <= existing_max_period
                        and page_new_df.empty
                    ):
                        logging.info(
                            "Incremental early-stop: reached periods <= existing max_period with no new rows."
                        )
                        break
            else:
                logging.warning(
                    f"Incremental mode could not run; page missing key fields {list(settings.key_fields)}. Falling back to full extract."
                )
                incremental_active = False
                all_records.extend(records)
        else:
            all_records.extend(records)

        current_offset += settings.max_limit

        total_seen = len(all_records) + len(new_records)
        if settings.max_records and total_seen >= settings.max_records:
            logging.info(
                f"Reached maximum records limit ({settings.max_records}). Stopping pagination."
            )
            if total_seen > settings.max_records:
                overflow = total_seen - settings.max_records
                if new_records:
                    new_records = (
                        new_records[:-overflow] if overflow < len(new_records) else []
                    )
                else:
                    all_records = all_records[:-overflow]
            break

        if len(records) < settings.max_limit:
            logging.info(
                f"Received {len(records)} records, less than limit. Pagination complete."
            )
            break

    # --- Final Validation and Storage ---
    if incremental_active:
        if not new_records:
            logging.info("Incremental run found 0 new records. Output unchanged.")
            return
        df = pd.DataFrame(new_records)
    else:
        df = pd.DataFrame(all_records)

    if df.empty:
        logging.critical(
            "Pipeline finished, but zero records were collected. Nothing to save."
        )
        return

    logging.info(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns")
    logging.info(f"Columns: {list(df.columns)}")

    existing_required_fields = [f for f in settings.required_fields if f in df.columns]
    if existing_required_fields:
        initial_count = len(df)
        df.dropna(subset=existing_required_fields, inplace=True)
        dropped_count = initial_count - len(df)
        logging.info(
            f"Dropped {dropped_count} records with null values in: {existing_required_fields}"
        )
    else:
        logging.warning(
            f"Expected fields {list(settings.required_fields)} not found in data. Skipping validation."
        )
        logging.info("Available fields for validation: " + ", ".join(df.columns))

    if incremental_active:
        logging.info(
            f"Total records collected: {len(all_records)} (new={len(new_records)})"
        )
    else:
        logging.info(f"Total records collected: {len(all_records)}")
    logging.info(f"Total valid records after validation: {len(df)}")

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if incremental_active and existing_df is not None:
            merged = pd.concat([existing_df, df], ignore_index=True)
            if set(settings.key_fields).issubset(set(merged.columns)):
                merged = merged.drop_duplicates(
                    subset=list(settings.key_fields), keep="last"
                )
            merged.to_parquet(output_path, index=False)
            df_written = merged
        else:
            df.to_parquet(output_path, index=False)
            df_written = df

        file_size = output_path.stat().st_size / (1024 * 1024)
        logging.info(
            f"Successfully saved raw data to '{output_path}' ({file_size:.2f} MB)."
        )
        logging.info(
            f"Pipeline completed successfully: {len(df_written)} records extracted and saved."
        )
    except Exception as e:  # noqa: BLE001
        logging.error(f"Failed to write Parquet file: {e}")
        raise
