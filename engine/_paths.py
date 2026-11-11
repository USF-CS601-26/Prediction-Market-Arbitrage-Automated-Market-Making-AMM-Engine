"""
engine/_paths.py

Puts both venue engines on the import path.

kalshi_engine/ and Polymarket_engine/ are sibling top-level folders, not
installed packages, and both use flat intra-package imports (`from models
import ...`). run_pair.py already bootstraps sys.path the same way; this
module exists so anything under engine/ can do it with one import instead
of repeating the path arithmetic.

Importing this module is idempotent, so `import engine.anything` works from
the repo root, from a test, or from `python -m engine.pipeline` without the
caller knowing the layout.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
KALSHI_DIR = ROOT / "kalshi_engine"
POLY_DIR = ROOT / "Polymarket_engine"

for _d in (ROOT, KALSHI_DIR, POLY_DIR):
    _s = str(_d)
    if _s not in sys.path:
        sys.path.insert(0, _s)
