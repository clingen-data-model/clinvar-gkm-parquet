"""The bundle fixture the suite reads: `tests/fixtures/real-1.1.0/clinvar-gkm_excerpt.json.gz`, a real 1.1.0
bundle excerpt made by scripts/extract-fixtures.py (design plan §8.1).

The environment variable CLINVAR_GKM_FIXTURE, set to the path of a folder the extractor wrote, runs the suite on
that excerpt instead (to try a new excerpt before it is committed).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"
REAL = FIXTURES / "real-1.1.0" / "clinvar-gkm_excerpt.json.gz"
_ENV = "CLINVAR_GKM_FIXTURE"


def _choose() -> Path:
    folder = os.environ.get(_ENV)
    if folder is None:
        return REAL
    path = Path(folder) / REAL.name
    if not path.is_file():
        raise RuntimeError(f"{_ENV}={folder}: no fixture at {path} (run scripts/extract-fixtures.py)")
    return path


FIXTURE = _choose()
PROVENANCE_PATH = FIXTURE.parent / "PROVENANCE.json"
PROVENANCE = json.loads(PROVENANCE_PATH.read_text(encoding="utf-8"))
COUNTS: dict[str, int] = PROVENANCE["counts"]
# The release the fixture's records come from, as the extractor recorded it.
SOURCE_RELEASE: str = PROVENANCE["source"]["release"]
STAGING_NAME = FIXTURE.name.removesuffix(".json.gz")
