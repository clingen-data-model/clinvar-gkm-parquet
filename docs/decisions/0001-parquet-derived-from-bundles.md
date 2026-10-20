# 0001 — Parquet is derived from the bundles; the bundles are the record

- **Status:** Proposed
- **Date:** 2026-10-20
- **Plan:** [`docs/plans/2026-10-20-design.md`](../plans/2026-10-20-design.md)

## Context

ClinVar-GKM publishes its data as JSON bundles (monthly fulls and weekly deltas) and as Parquet tables. Its
maintainer has decided (2026-10-18) that:

- the JSON bundles are the public record: schema-validated, schema-versioned, and kept under
  `archive/<version>/` with sha256s when a generation is superseded
  ([ClinVar-GKM ADR 0008](https://github.com/clingen-data-model/clinvar-gkm/pull/168), proposed);
- Parquet is "simply a method for making the json version of the data easily available, a kind of reference
  tool for quick access to the data using current database tools";
- the bundle is the one source of truth, and every other published artifact must be recomputable from it
  ([ClinVar-GKM ADR 0007](https://github.com/clingen-data-model/clinvar-gkm/blob/main/docs/decisions/0007-pipeline-principles-and-simplification.md),
  principle 2).

Today the Parquet tables are not built from the bundle. ClinVar-GKM's BigQuery SQL
(`src/scripts/parquet-schemas/*.sql`, 19 files on the `1.0` branch) re-derives them from the `gkm_dict_*`
tables in parallel to the bundle, and has to copy the assembler's record cleanup to match it. That copy drifted
in 1.0.1 (all-null placeholder elements in typed `extensions`, ClinVar-GKM #143) and is the defect class ADR
0007 Workstream 1 sets out to remove.

## Decision

1. **Input is the published bundle only.** The tool reads published bundle files: a monthly full, optionally
   followed by the weekly deltas and their manifests, from a local path or a URL (including `archive/` URLs).
   It never reads BigQuery, the assembler's intermediate files, or any other source.
2. **Every value is recomputable from the bundle.** Each column in the table spec cites the bundle JSON path it
   comes from and the transform applied. Allowed: casts, bare ids stripped from `#/` pointers, the section a
   pointer names, range bounds, extension values picked by extension name, typed copies of record fields,
   join and lookup tables that combine sections, and `data` columns holding the bundle record itself.
   Not allowed: a row or value with no source in the bundle, such as a gene id looked up elsewhere, or a record
   the bundle doesn't contain.
3. **A missing fact goes into the bundle first.** If a table needs something the bundle doesn't carry, the
   change is made in ClinVar-GKM's bundle (a schema or data change there), and the tool follows.
4. **One implementation.** ClinVar-GKM builds its published latest Parquet set with a pinned release of this
   tool, so the published Parquet and a user's rebuild come from the same code.

## Consequences

- A user can rebuild the Parquet of any release, including archived generations, from the bundles and the
  matching tool version, and check it against the published sha256s.
- The tool does the record-to-column work itself. Some typed columns need knowledge that is a property of the
  schema, not of a record: which typed slot (`value_string`, `value_submitted_condition_set`, …) an extension
  value occupies, or which nested struct fields a column has. That knowledge lives in the table spec, keyed by
  extension name and bundle path, and is still derivable from the bundle. An extension name the spec doesn't
  know stops the build.
- The 1.0.x and 1.1.0 Parquet sets were built by BigQuery, not by this tool, so they can be rebuilt in content
  but not byte for byte (ClinVar-GKM ADR 0008 (e)). The oracle (design plan §8) measures how close the content is.
- ClinVar-GKM ADR 0007 Workstream 1 recommended building Parquet from cleaned NDJSON the assembler would write
  (its option A). This decision builds from the bundle itself (its option B), because users only have the
  bundle. That ADR and its plan need a matching amendment in ClinVar-GKM; that is the maintainer's call.
- The bundle's line layout (one record per line) becomes something the tool relies on. The design plan §4.2
  verifies it and §14 asks whether ClinVar-GKM should declare it a contract.
