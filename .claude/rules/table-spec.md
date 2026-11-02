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

## Spec files and the compiled spec

`spec/<MAJOR>.<MINOR>/` (`clinvar-gkm`) and `spec/baseline/<MAJOR>.<MINOR>/` hold spec files. `sections` has no file: it
is compiled at run time from a bundle schema and its column set
([ADR 0006](../../docs/decisions/0006-sections-spec-compiled-from-schemas.md), format in
[`docs/column-set-format.md`](../../docs/column-set-format.md)). `spec/sections/column-sets/<version>.columns.yaml` holds
the column set of a published ClinVar-GKM version, and `spec/sections/default-bundle.yaml` says where others are.

What identifies a compiled table set is `table_spec_sha256`, of what affects the tables; `schema_set_sha256` and
`column_set_sha256` say what it came from. All three, and the compiled spec itself (`table-spec.json`), are in every set.

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

The table applies to spec files. For `sections`:

- **A pairing change needs no bump.** The columns follow the column set, so the producer names them and announces changes
  (ADR 0006).
- **A change to the rules bumps the rules version** (`compiler.RULES_VERSION`), by the table: what an existing pairing
  compiles to is what the table judges.
- **A built set doesn't depend on the rules:** it carries its compiled spec, so `verify` and `rebuild` read an older set
  with no recompile. They do need a tool that understands that spec's format.
- Rewording changes no hash: `table_spec_sha256` leaves out `description`, `transform`, `sources` and `status`.
- The golden test (`tests/test_compiler.py` against `tests/fixtures/sections-golden/`) lists every accepted difference
  on 1.1.0, and the lossless round trip must still pass on the real excerpt.

## What moves with a spec change (same PR)

- The new `spec/<version>/` directory (copy the previous one; never edit a released one), or for `sections` the rules
  version, or a column set under `spec/sections/column-sets/`.
- The table SQL and the spec-conformance test.
- The compatibility matrix: `README.md` and the package's `version --json` data.
- The changelog entry, listing every added, changed or removed column.
- A note for the ClinVar-GKM docs (the column reference `docs/parquet/tables.md` and the Ecosystem add-on
  page). This repo doesn't edit ClinVar-GKM; open or request a paired PR there.
- An oracle or bundle-recompute run on a real release for the changed tables, with the result in the PR body.
