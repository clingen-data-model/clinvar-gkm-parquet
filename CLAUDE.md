# CLAUDE.md

Project-wide invariants for clinvar-gkm-parquet. Keep this file short (under about 120 lines) and limited to
things that are true across the whole repo. Path-specific conventions live in `.claude/rules/` (loaded only when
working on matching files); one-off decisions live in `docs/decisions/` (ADRs). Change this file via PR review.

## General rules

- Only modify files the user asked for. Ask before touching tests, specs or docs beyond the request.
- Don't assume. Don't hide confusion. Surface tradeoffs.
- Minimum code that solves the problem. Nothing speculative.
- Touch only what you must. Clean up only your own mess.
- Define success criteria. Loop until verified.

## Purpose

This repo builds a downloadable, versioned **bundle → Parquet** tool for
[ClinVar-GKM](https://github.com/clingen-data-model/clinvar-gkm) (local clone: `~/Development/gks/clinvar-gks`).

- **The JSON bundles are the public record.** ClinVar-GKM publishes monthly full bundles and weekly delta
  bundles, schema-versioned, at `https://data.clinvar-gkm.org/v1/`. Superseded generations are kept under
  `archive/<version>/` with a `MANIFEST.json` of sha256s.
- **Parquet is a derived convenience**: a quick way to query the bundle content with current database tools.
  It is not a record. Only the latest Parquet set is published online; this tool lets anyone rebuild Parquet
  for any release, including archived ones.
- **Invariant: Parquet derives from the bundle.** Every Parquet row and value must be recomputable from the
  published bundle(s) alone. Derived forms are fine: bare ids from `#/` pointers, casts, range bounds,
  flattened extensions, join and lookup tables that combine sections. Facts the bundle lacks are not. If a
  table needs a fact the bundle doesn't carry, it goes into the ClinVar-GKM bundle first (a schema change
  there), never into this tool. See [ADR 0001](docs/decisions/0001-parquet-derived-from-bundles.md).

Until the tool ships, ClinVar-GKM still builds its latest Parquet set with BigQuery SQL
(`src/scripts/parquet-schemas/*.sql` on its `1.0` branch). This tool replaces that path once it matches it
(ClinVar-GKM ADR 0007, Workstream 1). The plan is in [the design plan](docs/plans/2026-10-20-design.md).

## Versioning

See [ADR 0002](docs/decisions/0002-independent-tool-versioning.md). In short:

- **Tool version:** semver, independent of the bundle schema version (they release on different cadences).
- **Table-spec version:** `MAJOR.MINOR`, versioned separately from the tool. It defines the tables, columns,
  types and derivations. MINOR = additive (new table, new trailing column); MAJOR = anything else.
- **Compatibility matrix:** every tool release declares the bundle `schema_version`s it accepts and the
  table-spec version it writes. The tool reads each bundle's self-declared `schema_version` and **refuses**
  any version not in its list. The matrix lives in `README.md` and in `version --json`; keep both in sync.
- Each Parquet file records the tool version, the table-spec version and the source bundle sha256(s) in its
  key-value metadata.

## Testing and verification

- **Real data, not hand-built samples.** Unit-test fixtures are small excerpts of real published bundles,
  with the source URL, release and sha256 recorded. A hand-written record can hide a real-data shape.
- **The oracle** gates the switch-over: on the same release, the tool's output must match ClinVar-GKM's
  BigQuery-exported latest Parquet table by table (schema, row counts, id sets, every typed column, `data`).
  Every difference is either fixed or listed, explained and accepted by the maintainer before rollout.
- **Bundle recompute:** id sets equal the bundle sections, and `data` equals the bundle record.
- Unknown `schema_version`, unknown bundle section, or unknown extension name: fail loudly. Never drop rows
  or values silently.

## Published data access

- Base URL `https://data.clinvar-gkm.org`. Release index: `/index.json` (root manifest) and `/v1/index.json`.
- Send a descriptive `User-Agent` (curl and scripts need one). The CDN rate limit is **250 requests per 10 s**
  per client; a burst over it gets HTTP 429. Back off and retry; never parallelize past it.
- Only `/v1/`, `/archive/`, `/.well-known/`, `/` and `/index.json` are served; other paths are blocked.
- This repo never writes to R2, BigQuery or the ClinVar-GKM repo. Publishing Parquet is ClinVar-GKM's job.

## Git conventions

- Default branch is `main`. Work on a branch; land changes via PR review. Don't merge without the user's OK.
- **No Claude attribution:** no "Co-Authored-By: Claude" and no "Generated with Claude Code" in commit
  messages or PR bodies.
- Keep commit messages clean and focused on the change.
- Docs move with every PR: a change to behaviour, the table spec or the compatibility matrix updates the
  matching docs in the same PR.

## Where things live

- `docs/plans/` — design plans (start with [2026-10-20-design.md](docs/plans/2026-10-20-design.md)).
- `docs/decisions/` — ADRs, one per decision; `README.md` is the index.
- `.claude/rules/` — path-scoped conventions:
  - [`python.md`](.claude/rules/python.md) — package code and tests (`src/**`, `tests/**`, `pyproject.toml`)
  - [`table-spec.md`](.claude/rules/table-spec.md) — the table spec and its bump rules (`spec/**`, table SQL)
  - [`docs.md`](.claude/rules/docs.md) — design records and the README (`docs/**`, `README.md`)
- `.claude/skills/` — `pr` (open a PR), `release-tool` (cut a tool release, update the matrix).
- `/catchup` (`.claude/commands/catchup.md`) — what changed here and in ClinVar-GKM since you last looked.
- User-facing docs live on the ClinVar-GKM site (`https://dataexchange.clinicalgenome.org/clinvar-gkm/`),
  planned under **Ecosystem → Add-ons**. That page summarises and links here; it doesn't duplicate.
- ClinVar-GKM facts this tool depends on: the assembler (`src/scripts/assemble-gkm-dicts.py` +
  `gkm_json_cleanup.py`), the bundle schema (`schema/clinvar-gkm/clinvar-gkm-bundle.schema.json`), the release
  model docs (`docs/data-access/release-model.md`, `weekly-deltas.md`) and the Parquet column reference
  (`docs/parquet/tables.md`). Read them on the right branch: `1.0` is the release line, `main` is dev.
