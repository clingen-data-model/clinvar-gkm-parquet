"""The compatibility data: what this tool release reads and writes. The source of `version --json`.

Keep in sync with the matrix in README.md (ADR 0002; the release-tool skill updates both).
"""

from __future__ import annotations

import duckdb

from . import __version__
from .spec import load_spec

TABLE_SPEC_VERSION = "1.0"
ACCEPTED_SCHEMA_VERSIONS: tuple[str, ...] = ("1.1.0",)


def version_info() -> dict:
    spec = load_spec(TABLE_SPEC_VERSION)
    return {
        "tool_version": __version__,
        "table_spec_version": TABLE_SPEC_VERSION,
        "table_spec_status": spec.status,
        "accepted_schema_versions": list(ACCEPTED_SCHEMA_VERSIONS),
        "duckdb_version": duckdb.__version__,
        "tables": sorted(t.name for t in spec.tables),
    }
