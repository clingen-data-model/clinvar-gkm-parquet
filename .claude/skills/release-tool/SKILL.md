---
name: release-tool
description: Cut a clinvar-gkm-parquet tool release and update the compatibility matrix. Use when the user asks to release, tag or publish a new version of the tool, or to add support for a new bundle schema_version.
---

# Cut a tool release

The release process below is the one proposed in the design plan (`docs/plans/2026-10-20-design.md`, §9 and
§10). Where the implementation differs, follow the implementation and fix this skill in the same PR.
Every step that publishes (tag push, PyPI, GitHub Release) needs the user's explicit OK.

## 1. Decide the version

- Read `version --json` on `main` and the changelog since the last tag.
- Tool version (semver):
  - **MAJOR**: a breaking CLI change, a table-spec MAJOR, or dropping an accepted `schema_version`;
  - **MINOR**: a new accepted `schema_version`, a table-spec MINOR, or a new feature;
  - **PATCH**: a fix that doesn't change the table spec (including a fix that makes output conform to it).
- Table-spec version: changes only if the release ships a new `spec/<version>/` (see
  `.claude/rules/table-spec.md`).

## 2. Verify before tagging

- `uv run ruff check` and `uv run pytest` pass.
- For every `schema_version` the release accepts, run `build` then `verify` on a **real published bundle**
  of that version (a monthly full, plus a delta chain if replay changed), and record the commands and results.
- If the table spec or the build logic changed: rerun the oracle against ClinVar-GKM's published latest Parquet
  for the same release (design plan §8), or the bundle recompute if no BigQuery export exists for that release.
  Every difference is fixed or listed and accepted by the user.
- Run the build twice and compare file sha256s: the same inputs must give byte-identical files.

## 3. Update the compatibility matrix (one PR)

- The package's compatibility data (the source of `version --json`).
- The table in `README.md`: one row per tool release, with accepted `schema_version`s and the table-spec
  version it writes.
- `CHANGELOG.md`: the version, date, accepted `schema_version`s, table-spec version, and every change.
- Merge only with the user's OK.

## 4. Tag and publish (after the user's OK)

- Tag the merge commit on `main` (tag format: see design plan §9.3) and push the tag.
- The release workflow builds the sdist and wheel and publishes them (PyPI via trusted publishing, plus a
  GitHub Release with the changelog entry). Check that the published version installs in a clean environment
  and that `clinvar-gkm-parquet version --json` reports the expected matrix.

## 5. Tell ClinVar-GKM

- If ClinVar-GKM should move to the new version, open a request (issue or paired PR) in
  `clingen-data-model/clinvar-gkm` to bump the pinned version in its Parquet release script and to update the
  Ecosystem add-on page and, for a spec change, `docs/parquet/tables.md`. This repo doesn't edit ClinVar-GKM.
- Release announcements go to `clinvar-gkm@broadinstitute.org` and GitHub Discussions, through the maintainer.
