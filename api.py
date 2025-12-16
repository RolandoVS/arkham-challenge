"""Compatibility shim.

We keep `api.py` so `uvicorn api:app --reload` continues to work, but the implementation
now lives in the `arkham_api/` package for readability.
"""

from arkham_api.main import app, create_app  # noqa: F401
from arkham_api.settings import Settings  # noqa: F401
