"""The compatibility data: what this tool release reads and writes. The source of `version --json`.

Keep in sync with the matrix in README.md (ADR 0002, amended by ADR 0003 and ADR 0006; the release-tool skill
updates both). A bundle schema_version has no fixed list here: `sections` compiles any bundle with its schema and
column set (ADR 0006), `baseline`, `clinvar-gkm` and a custom spec read the versions their tables.yaml lists. This
module holds data only; it imports the spec loader lazily, because spec.py reads these constants.
"""

from __future__ import annotations

import duckdb

from . import __version__
from .compiler import RULES_VERSION, SPEC_NAME

# The built-in table specs (ADR 0003): name -> the one version of it this release writes.
BUILTIN_SPECS: dict[str, str] = {
    SPEC_NAME: RULES_VERSION,
    "baseline": "1.0",
    "clinvar-gkm": "1.0",
}
COMPILED_SPEC = SPEC_NAME  # compiled from a schema and its column set, not read from a tables.yaml (ADR 0006)
DEFAULT_SPEC = "sections"  # what a build writes when no --spec is given (ADR 0003; baseline until 2026-10-29)
KV_PREFIX = (
    "clinvar_gkm_parquet."  # of every key-value entry a build writes; a rename here changes the files' bytes
)
PUBLISHED_SPEC = "clinvar-gkm"  # the spec of ClinVar-GKM's published set; its release and the oracle name it
# The version of the clinvar-gkm spec. Kept under this name for build.py, verify.py and version --json, which
# read it as "the" table-spec version from before specs had names.
TABLE_SPEC_VERSION = BUILTIN_SPECS[PUBLISHED_SPEC]


SCHEMA_VERSION_RULE = (
    "sections compiles any bundle from its schema and column set (ADR 0006). With neither given, it reads the "
    "default bundle's: the versions in snapshotted_schema_versions work offline, and any other whose schema and "
    "column set are published is fetched once and cached. Every other spec reads the bundle_schema_versions it "
    "lists."
)


def validate_engines() -> dict:
    """`validate`'s engines: the library each runs on and its installed version (None when not installed)."""
    from importlib.metadata import PackageNotFoundError, version

    def installed(dist: str) -> str | None:
        try:
            return version(dist)
        except PackageNotFoundError:
            return None

    return {
        "rs": {"library": "jsonschema-rs", "version": installed("jsonschema-rs"), "default": True},
        "python": {
            "library": "ga4gh.gkm",
            "version": installed("ga4gh.gkm"),
            "default": False,
            "extra": "reference",
        },
    }


def version_info() -> dict:
    """`version --json`. The keys table_spec_version, table_spec_status and tables describe the clinvar-gkm spec,
    as before specs had names; `table_specs` lists every built-in spec and marks the default. A compiled spec has
    no one table set, so its entry has `tables_by_schema_version` (the snapshotted versions) instead of
    `tables`."""
    from .compiled import compiled_spec
    from .default_pairing import snapshot_versions
    from .spec import load_builtin

    published = load_builtin(PUBLISHED_SPEC)
    snapshotted = snapshot_versions()
    specs = []
    for name, version in BUILTIN_SPECS.items():
        entry = {
            "name": name,
            "version": version,
            "default": name == DEFAULT_SPEC,
            "compiled": name == COMPILED_SPEC,
        }
        if name == COMPILED_SPEC:
            by_version = {v: compiled_spec(v)[0] for v in snapshotted}
            entry["status"] = next(iter(by_version.values())).status if by_version else "draft"
            entry["bundle_schema_versions"] = snapshotted
            entry["tables_by_schema_version"] = {
                v: sorted(t.name for t in s.tables) for v, s in by_version.items()
            }
        else:
            spec = load_builtin(name, version)
            entry["status"] = spec.status
            entry["bundle_schema_versions"] = list(spec.bundle_schema_versions)
            entry["tables"] = sorted(t.name for t in spec.tables)
        specs.append(entry)
    return {
        "tool_version": __version__,
        "table_spec_version": TABLE_SPEC_VERSION,
        "table_spec_status": published.status,
        "schema_version_rule": SCHEMA_VERSION_RULE,
        "snapshotted_schema_versions": snapshotted,
        "duckdb_version": duckdb.__version__,
        "tables": sorted(t.name for t in published.tables),
        "default_table_spec": DEFAULT_SPEC,
        "table_specs": specs,
        "validate_engines": validate_engines(),
    }
