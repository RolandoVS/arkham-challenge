"""
Pytest configuration and shared test helpers.

Why this file exists:
- This project is a single-file script/module (`connector.py`) rather than an installed
  package. Depending on how pytest is invoked, the repository root may not be on
  `sys.path`, which can make `import connector` fail during test collection.

What we do here:
- Add the repo root to `sys.path` so tests can import `connector` reliably without
  requiring packaging/installation steps.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Repository root directory (parent of `tests/`).
ROOT = Path(__file__).resolve().parents[1]

# Ensure the project root is importable for tests.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
