# 0002 — Tool versioning independent of the bundle schema

- **Status:** Proposed
- **Date:** 2026-10-20
- **Plan:** [`docs/plans/2026-10-20-design.md`](../plans/2026-10-20-design.md) §3 and §9

## Context

The ClinVar-GKM bundle schema has its own semver (ClinVar-GKM
[ADR 0004](https://github.com/clingen-data-model/clinvar-gkm/blob/main/docs/decisions/0004-schema-versioning-and-release-management.md)),
and each bundle names its version in a top-level `schema_version` field (1.0.1 today on the live site; 1.1.0 is
being released). The Parquet tables change on a different schedule: a new lookup table or column needn't wait
for a bundle schema change, and a bundle schema change may not touch the Parquet layout at all. Users must also
be able to rebuild Parquet for an archived release with the tool version that matches it.

Three things therefore need versions, and they move independently:

1. the bundle schema (owned by ClinVar-GKM);
2. the Parquet **table spec**: the tables, columns, types and derivations;
3. the **tool** that reads (1) and writes (2).

## Decision

1. **The tool uses semver**, independent of the bundle schema version.
   - MAJOR: a breaking CLI change, a table-spec MAJOR, or dropping an accepted `schema_version`.
   - MINOR: a newly accepted `schema_version`, a table-spec MINOR, or a new feature.
   - PATCH: a fix that doesn't change the table spec.
2. **The table spec has its own version, `MAJOR.MINOR`.** MINOR is additive only (a new table, or a new column
   appended after the existing ones). Any rename, removal, reorder, type change or change of meaning is MAJOR.
   A published spec version is immutable. The spec lives in this repo, one directory per version.
3. **Every tool release declares a compatibility row:** the exact list of bundle `schema_version`s it accepts,
   and the one table-spec version it writes. The list is explicit (no ranges), because a bundle MINOR can add
   sections and content the tool must handle.
4. **The tool refuses an unsupported bundle.** It reads `schema_version` from the bundle and stops with an error
   naming the supported versions and pointing at the compatibility matrix.
5. **Each Parquet file is self-describing.** Its key-value metadata records the tool version, the table-spec
   version, the bundle `schema_version`, and the sha256 of every source bundle file.
6. **The compatibility matrix** is kept in `README.md` (for people) and in the package's `version --json`
   output (for scripts), updated together in the release PR.

## Consequences

- A user holding any bundle can find the tool version that reads it, and any Parquet file says which tool,
  spec and bundle bytes produced it.
- ClinVar-GKM pins one tool version per release, and its docs can say which spec version the published latest
  set follows.
- Accepting a new bundle `schema_version`, even a PATCH, needs a tool release (at least a MINOR). That is
  deliberate: the tool only claims to read what it has been run on.
- Two version numbers can confuse: "tool 1.0.0", "table spec 1.0" and "bundle schema 1.1.0". Docs always say
  which one they mean; the design plan §9.3 recommends a `v` prefix on tool tags to tell them apart.
