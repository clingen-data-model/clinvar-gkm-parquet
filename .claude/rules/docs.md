---
description: Documentation conventions (plain Markdown design records here; user-facing pages on the ClinVar-GKM site)
paths:
  - "docs/**"
  - "README.md"
---

# Documentation conventions

## What lives where

- **This repo** holds contributor-facing design records as plain Markdown: ADRs in `docs/decisions/`, design
  plans in `docs/plans/`, and later the rendered table spec. There is no docs site here, so there is no build
  step; GitHub renders the Markdown. Check relative links by hand (or with a link checker) before committing.
- **User-facing docs** (how to install and run the tool, the compatibility matrix, the column-set format, `validate`)
  live here, in the README and `docs/`, because the tool serves any GKM producer (ADR 0009). A producer's own site, such
  as ClinVar-GKM's planned **Ecosystem → Add-ons** page, summarises its use of the tool and links here.
- **Summarise and link; don't duplicate.** A producer's pages summarise and link here; this repo links to the producer
  for its bundle, schema and release-model facts instead of restating them. When a fact must appear in
  both places, one is the source and the other links to it at a pinned ref (a tag or commit), not `main`.

## Docs move with every PR

A PR that changes CLI behaviour, the table spec, accepted `schema_version`s or the compatibility matrix updates
the README, the changelog and any affected plan or ADR in the same PR, and says in its body which producer page
(ClinVar-GKM's today) needs a paired change.

## Style

- Plain, direct, present tense. No emoji.
- Name things as ClinVar-GKM does: **GKM** (Genomic Knowledge Model) is the schema set (VRS, Cat-VRS,
  VA-Spec); **GKS** (Genomic Knowledge Standards) is the GA4GH workstream. Say "bundle", not "dictionary".
- Back claims with evidence: a file path at a branch or commit, a published URL, or a measurement with its
  date. Mark estimates as estimates.
- Don't describe features as available until they are released; say "planned" until then.

## ADRs

- One file per decision, `NNNN-kebab-title.md`, copied from `docs/decisions/0000-template.md`.
- Status `Proposed` → `Accepted` → `Superseded by NNNN`. Supersede; don't rewrite history.
- Add each new ADR to the index in `docs/decisions/README.md`.
