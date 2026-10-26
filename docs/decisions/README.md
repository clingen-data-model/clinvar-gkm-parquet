# Architecture Decision Records (ADRs)

One short, dated file per significant decision: why we chose an approach, not just what the code does. ADRs are
the PR-reviewed record of the project's moving picture. `CLAUDE.md` points here rather than importing them.

Decisions that belong to ClinVar-GKM (the bundle schema, the release model, what is published) live in
[ClinVar-GKM's ADRs](https://github.com/clingen-data-model/clinvar-gkm/tree/main/docs/decisions). The ADRs here
cover the tool only, and link to those rather than restating them.

## Conventions

- Filename: `NNNN-kebab-title.md` (zero-padded, increasing).
- Copy [`0000-template.md`](0000-template.md) to start.
- Keep it short. Link to the fuller design plan in `docs/plans/` when there is one.
- Status: `Proposed` → `Accepted` → (later) `Superseded by NNNN`. Don't rewrite history; supersede.
- Land ADRs via PR review, like code.

## Index

- [0001 — Parquet is derived from the bundles; the bundles are the record](0001-parquet-derived-from-bundles.md)
- [0002 — Tool versioning independent of the bundle schema](0002-independent-tool-versioning.md)
- [0004 — Selective builds: a driver, a pointer walk and a verbatim mini-bundle](0004-driver-based-selection.md)
