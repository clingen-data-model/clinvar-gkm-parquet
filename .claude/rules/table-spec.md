---
description: Table-spec conventions (derivation rule, version bump rules, what moves with a spec change)
paths:
  - "spec/**"
  - "src/clinvar_gkm_parquet/sql/**"
---

# Table-spec conventions

The table spec defines every Parquet table the tool writes: file name, source bundle section(s), row filter,
sort key, and per column its name, exact DuckDB type, empty-versus-NULL rule and derivation. The proposed
format and location (`spec/<MAJOR>.<MINOR>/tables.yaml`, one directory per spec version) are in the
[design plan](../../docs/plans/2026-10-20-design.md) §3. The spec is the contract; the SQL under
`src/clinvar_gkm_parquet/sql/` implements it, and a test checks that each built table matches the spec exactly.

## Every column cites the bundle

Each column's `derivation` names the bundle JSON path(s) it comes from and the transform (cast, pointer strip,
range bound, extension pick by name, ...). A column whose value can't be traced to the published bundle is not
allowed (see [ADR 0001](../../docs/decisions/0001-parquet-derived-from-bundles.md)). Lookup tables may combine
several sections, but each value still cites its source path.

## Version bump rules

| Change | Spec bump |
| --- | --- |
| New table | MINOR |
| New column appended after the existing columns | MINOR |
| New accepted value in an existing column that follows from new bundle content (e.g. a new enum value) | none (record it in the changelog) |
| Rename, remove or reorder a column; change a type; change empty-versus-NULL behaviour | MAJOR |
| Change what a column means, or which rows a table holds | MAJOR |
| Remove or rename a table or file | MAJOR |
| Writer settings only (compression, row-group size, sort order) | none, but call it out in the tool changelog: it changes remote-query cost |

A published spec version is **immutable**. Fix a mistake with a new version, not an edit. A tool bug fix that
makes the output conform to an existing spec is a tool PATCH, not a spec change.

## What moves with a spec change (same PR)

- The new `spec/<version>/` directory (copy the previous one; never edit a released one).
- The table SQL and the spec-conformance test.
- The compatibility matrix: `README.md` and the package's `version --json` data.
- The changelog entry, listing every added, changed or removed column.
- A note for the ClinVar-GKM docs (the column reference `docs/parquet/tables.md` and the Ecosystem add-on
  page). This repo doesn't edit ClinVar-GKM; open or request a paired PR there.
- An oracle or bundle-recompute run on a real release for the changed tables, with the result in the PR body.
