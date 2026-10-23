"""Which bundle fixture the suite reads: the one switch between the provisional and the real fixture.

- `tests/fixtures/real-1.1.0/clinvar-gkm_excerpt.json.gz`, a real 1.1.0 bundle excerpt made by
  scripts/extract-fixtures.py, once it is committed (design plan §8.1);
- until then, the provisional `tests/fixtures/examples-1.1.0/clinvar-gkm_examples.json.gz`.

The environment variable CLINVAR_GKM_FIXTURE overrides the choice: `provisional`, `real`, or the path of a folder
the extractor wrote (to try the suite on an excerpt before it is committed).

The provisional fixture predates parts of the 1.1.0 code (its README): bare condition CURIEs where the current
code writes `#/condition/` and `#/conditionSet/` pointers. IS_PROVISIONAL marks the tests that are strict xfails
on that shape only; on a real excerpt they are expected to pass.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"
PROVISIONAL = FIXTURES / "examples-1.1.0" / "clinvar-gkm_examples.json.gz"
REAL = FIXTURES / "real-1.1.0" / "clinvar-gkm_excerpt.json.gz"
_ENV = "CLINVAR_GKM_FIXTURE"


def _choose() -> Path:
    choice = os.environ.get(_ENV, "auto")
    if choice == "auto":
        return REAL if REAL.is_file() else PROVISIONAL
    if choice == "provisional":
        return PROVISIONAL
    path = REAL if choice == "real" else Path(choice) / REAL.name
    if not path.is_file():
        raise RuntimeError(f"{_ENV}={choice}: no fixture at {path} (run scripts/extract-fixtures.py)")
    return path


FIXTURE = _choose()
PROVENANCE_PATH = FIXTURE.parent / "PROVENANCE.json"
PROVENANCE = json.loads(PROVENANCE_PATH.read_text(encoding="utf-8"))
IS_PROVISIONAL = "provisional" in PROVENANCE
COUNTS: dict[str, int] = PROVENANCE["counts"]
# The release the fixture's records come from: the provisional examples' dataset is clinvar_2026_10_04_v2_6_0
# (its PROVENANCE.json); a real excerpt records the release it was cut from.
SOURCE_RELEASE: str = "2026-10-04" if IS_PROVISIONAL else PROVENANCE["source"]["release"]
STAGING_NAME = FIXTURE.name.removesuffix(".json.gz")
