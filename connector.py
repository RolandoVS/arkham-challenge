"""
EIA Nuclear Outages connector (script entrypoint).

This repository keeps `connector.py` as the runnable script (deliverable), but the
implementation lives in `arkham_connector/` for readability/testability.

Run:
  python connector.py

Incremental mode (dedup + early-stop):
  INCREMENTAL=1 python connector.py
"""

from __future__ import annotations

import logging

from arkham_connector import env
from arkham_connector.runner import run_connector

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    env.load_env()
    logging.info("--- Starting EIA Data Connector Pipeline ---")
    run_connector()
    logging.info("--- Pipeline Finished ---")
