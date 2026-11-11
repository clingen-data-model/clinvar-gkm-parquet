# CLAUDE.md

Project-wide invariants for clinvar-gkm-parquet (to be renamed gkm-bundle-parquet, ADR 0009). Keep this file short (under about 120 lines) and limited to
things that are true across the whole repo. Path-specific conventions live in `.claude/rules/` (loaded only when
working on matching files); one-off decisions live in `docs/decisions/` (ADRs). Change this file via PR review.

## General rules

- Only modify files the user asked for. Ask before touching tests, specs or docs beyond the request.
- Don't assume. Don't hide confusion. Surface tradeoffs.
- Minimum code that solves the problem. Nothing speculative.
- Touch only what you must. Clean up only your own mess.
- Define success criteria. Loop until verified.

## Purpose

This repo builds a downloadable, versioned tool that turns **GKM JSON bundles** into Parquet and back, and validates
them: any bundle built on the GA4GH GKM classes (VRS, Cat-VRS, VA-Spec), with that bundle's JSON Schema. The end
state is in [ADR 0008](docs/decisions/0008-end-state-reversible-default-custom-projections-filters.md) (accepted) and
[ADR 0009](docs/decisions/0009-generic-gkm-bundle-tool-renamed.md) (proposed):

- **No producer is built in.** The tool names no producer and assumes none; each run is given the bundle's schema.
  The first producer is [ClinVar-GKM](https://github.com/clingen-data-model/clinvar-gkm) (local clone:
  `~/Development/gks/clinvar-gks`); real CIViC bundles (`tests/fixtures/civic/`) are the second. Until 2.0.0, ClinVar-GKM
  is still the default and its release model (line index, deltas, replay) is still in the tool; both leave in 2.0.0.
  Don't add producer-specific logic, names or URLs to the code; a producer's facts are data (its schema, column set).
- **The JSON bundles are the public record. Parquet is a derived convenience**, a quick way to query the bundle
  content with current database tools.
- **Invariant: Parquet derives from the bundle.** Every Parquet row and value must be recomputable from the bundle
  alone. Derived forms are fine: bare ids from `#/` pointers, casts, range bounds, flattened extensions, join and
  lookup tables that combine sections. Facts the bundle lacks are not: they go into the producer's bundle first (a
  schema change there), never into this tool. See [ADR 0001](docs/decisions/0001-parquet-derived-from-bundles.md).
- **The default set** is generated from the bundle schema and rebuilds the bundle **record-identical** (every record by
  key, JSON-equal; key order, array order, whitespace and record order don't count). Custom column sets are labelled
  projections. ClinVar-GKM plans to publish the default set as its Parquet dataset.
- The generic core is meant to move to the GKM Toolkit (`ga4gh/gkm-starter-kit`, Python `ga4gh.gkm`) when the
  maintainer asks ([ADR 0006](docs/decisions/0006-sections-spec-compiled-from-schemas.md)).

## Versioning

See [ADR 0002](docs/decisions/0002-independent-tool-versioning.md). In short:

- **Tool version:** semver, independent of the bundle schema version (they release on different cadences).
- **Table-spec version:** `MAJOR.MINOR`, versioned separately from the tool. It defines the tables, columns,
  types and derivations. MINOR = additive (new table, new trailing column); MAJOR = anything else.
- **Compatibility matrix:** every tool release declares the table-spec versions it writes and what it reads.
  `sections` is compiled from a bundle schema and its column set, and refuses a pairing that doesn't load and compile
  (ADR 0006). In 1.x, with neither given it reads ClinVar-GKM's schema at the bundle's `schema_version`, and `baseline`,
  `clinvar-gkm` and a custom spec refuse any version not in their list; from 2.0.0 the matrix lists no producer's
  versions (ADR 0009). The matrix lives in `README.md` and in `version --json`; keep both in sync.
- Each Parquet file records the tool version, the table-spec version and the source bundle sha256(s) in its
  key-value metadata; a compiled `sections` file also the sha256 of its schema set, column set and compiled spec.

## Testing and verification

- **Real data, not hand-built samples.** Unit-test fixtures are small excerpts of real published bundles (ClinVar-GKM,
  CIViC), with the source URL, release and sha256 recorded. A hand-written record can hide a real-data shape.
- **The round trip is the check:** a build rebuilds every record and compares it with its source; `verify
  --bundle-recompute` and `verify --roundtrip` check a built set against its bundle. The BigQuery oracle no longer
  gates anything (ADR 0008).
- **`validate`'s engines must agree:** a change to validation reruns the equivalence evidence (docs/validate.md).
- A bundle the schema can't read, an unknown bundle section, or an unknown extension name: fail loudly. Never drop
  rows or values silently.

## Reading ClinVar-GKM's published data

For fixtures and evidence runs (ClinVar-GKM's data access rules; they leave the tool's code in 2.0.0):

- Base URL `https://data.clinvar-gkm.org`. Release index: `/index.json` (root manifest, lists the lines) and
  one index per line (`/v1/1.1/index.json`, provisional path), which gives each monthly's `sha256` and
  `source_release`. Paths move at month and year rollover: resolve files through the index, by release.
- Send a descriptive `User-Agent` (curl and scripts need one). The CDN rate limit is **250 requests per 10 s**
  per client; a burst over it gets HTTP 429. Back off and retry; never parallelize past it.
- Only `/v1/`, `/archive/`, `/.well-known/`, `/` and `/index.json` are served; other paths are blocked.
  `/archive/` goes once `archive/1.0.1/` has moved to `v1/1.0/`.
- This repo never writes to R2, BigQuery or a producer's repo. Publishing Parquet is the producer's job.

## Git conventions

- Default branch is `main`. Work on a branch; land changes via PR review. Don't merge without the user's OK.
- **No Claude attribution:** no "Co-Authored-By: Claude" and no "Generated with Claude Code" in commit
  messages (the user's rule). Keep PR bodies free of them too, since a squash merge can copy the body into
  the commit.
- Keep commit messages clean and focused on the change.
- Docs move with every PR: a change to behaviour, the table spec or the compatibility matrix updates the
  matching docs in the same PR.

## Where things live

- `docs/plans/` — design plans. [2026-10-20-design.md](docs/plans/2026-10-20-design.md) is the history of the
  ClinVar-GKM-specific design; ADRs 0008 and 0009 replace parts of it.
- `docs/decisions/` — ADRs, one per decision; `README.md` is the index.
- `.claude/rules/` — path-scoped conventions:
  - [`python.md`](.claude/rules/python.md) — package code and tests (`src/**`, `tests/**`, `pyproject.toml`)
  - [`table-spec.md`](.claude/rules/table-spec.md) — the table spec and its bump rules (`spec/**`, table SQL)
  - [`docs.md`](.claude/rules/docs.md) — design records and the README (`docs/**`, `README.md`)
- `.claude/skills/` — `pr` (open a PR), `release-tool` (cut a tool release, update the matrix).
- `/catchup` (`.claude/commands/catchup.md`) — what changed here and in ClinVar-GKM since you last looked.
- User-facing docs: the README and `docs/` here. ClinVar-GKM's site (`https://dataexchange.clinicalgenome.org/clinvar-gkm/`)
  plans a page under **Ecosystem → Add-ons** that summarises and links here; it doesn't duplicate.
- ClinVar-GKM, the first producer, for its fixtures and evidence: the assembler (`src/scripts/assemble-gkm-dicts.py` +
  `gkm_json_cleanup.py`), the bundle schema (`schema/clinvar-gkm/clinvar-gkm-bundle.schema.json`), the release
  model docs (`docs/data-access/release-model.md`, `weekly-deltas.md`) and the Parquet column reference
  (`docs/parquet/tables.md`). Read them on the right branch: `1.0` is the release line, `main` is dev.
