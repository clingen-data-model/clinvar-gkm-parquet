"""The compatibility data: what this tool release reads and writes. The source of `version --json`.

Keep in sync with the matrix in README.md (ADR 0002, amended by ADR 0003; the release-tool skill updates both).
This module holds data only; it imports the spec loader lazily, because spec.py reads these constants.
"""

from __future__ import annotations

import duckdb

from . import __version__

ACCEPTED_SCHEMA_VERSIONS: tuple[str, ...] = ("1.1.0",)

# The sections each accepted bundle schema_version has (schema/clinvar-gkm/clinvar-gkm-bundle.schema.json @
# origin/1.0), in the assembler's order. A table spec may read only these (spec.check_rules). bundle.SECTIONS
# holds the same 1.1.0 list (a test keeps them equal).
SCHEMA_SECTIONS: dict[str, tuple[str, ...]] = {
    "1.1.0": (
        "sequenceReference",
        "location",
        "allele",
        "copyNumberCount",
        "copyNumberChange",
        "gene",
        "variation",
        "condition",
        "conditionSet",
        "therapy",
        "therapyGroup",
        "submitter",
        "propositionPathogenicity",
        "propositionOncogenicity",
        "propositionSomaticClinicalImpact",
        "propositionClinvarCustom",
        "evidenceLine",
        "scv",
        "vcv",
        "rcv",
    ),
}

# The built-in table specs (ADR 0003): name -> the one version of it this release writes.
BUILTIN_SPECS: dict[str, str] = {
    "baseline": "1.0",
    "clinvar-gkm": "1.0",
}
DEFAULT_SPEC = "baseline"  # what a build writes when no --spec is given
PUBLISHED_SPEC = "clinvar-gkm"  # the spec of ClinVar-GKM's published set; its release and the oracle name it
# The version of the clinvar-gkm spec. Kept under this name for build.py, verify.py and version --json, which
# read it as "the" table-spec version from before specs had names.
TABLE_SPEC_VERSION = BUILTIN_SPECS[PUBLISHED_SPEC]


def schema_sections(schema_version: str) -> tuple[str, ...]:
    return SCHEMA_SECTIONS[schema_version]


def version_info() -> dict:
    """`version --json`. The keys table_spec_version, table_spec_status and tables describe the clinvar-gkm
    spec, as before specs had names; `table_specs` lists every built-in spec and marks the default."""
    from .spec import load_builtin

    published = load_builtin(PUBLISHED_SPEC)
    specs = []
    for name, version in BUILTIN_SPECS.items():
        spec = load_builtin(name, version)
        specs.append(
            {
                "name": name,
                "version": version,
                "status": spec.status,
                "default": name == DEFAULT_SPEC,
                "bundle_schema_versions": list(spec.bundle_schema_versions),
                "tables": sorted(t.name for t in spec.tables),
            }
        )
    return {
        "tool_version": __version__,
        "table_spec_version": TABLE_SPEC_VERSION,
        "table_spec_status": published.status,
        "accepted_schema_versions": list(ACCEPTED_SCHEMA_VERSIONS),
        "duckdb_version": duckdb.__version__,
        "tables": sorted(t.name for t in published.tables),
        "default_table_spec": DEFAULT_SPEC,
        "table_specs": specs,
    }
