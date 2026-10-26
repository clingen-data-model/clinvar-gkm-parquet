# 0004 — Selective builds: a driver, a pointer walk and a verbatim mini-bundle

- **Status:** Proposed
- **Date:** 2026-10-26
- **Plan:** [`docs/plans/2026-10-20-design.md`](../plans/2026-10-20-design.md) §4.5

## Context

A full build reads the whole bundle (about 91.5 M records in the 2026-10-18 weekly full) and writes every table.
Many users want Parquet for a slice instead: a list of VCVs, the SCVs of one submitter, the records of one gene,
with the records those point to. The bundle already links its records with `#/<section>/<key>` pointers (design
plan §4.2), so a slice can be found by walking them. The slice must still obey
[ADR 0001](0001-parquet-derived-from-bundles.md): every row and value is recomputable from the published bundle.

The maintainer decided the user-facing shape on 2026-10-26 (below). This ADR records it, with the reasons and
the engine design (`src/clinvar_gkm_parquet/select.py`). The CLI wiring and the `--where` evaluation are
wired (`src/clinvar_gkm_parquet/driver.py`: `build --driver ...` and the `select` command; design plan §4.5,
§10).

## Decision

1. **The driver.** A selective run names a driver table of the chosen spec; its bundle section holds the
   starting records. The starting keys come from a SQL `--where` over that table, from explicit `--ids` or
   `--ids-file`, or from both (their union). The engine takes the starting keys as input
   (`{section: keys}`); `driver.py` evaluates `--where` (see Consequences).
2. **`--follow out|in|both`, default `out`.** `out` adds every record that a selected record points to (a
   `#/<section>/<key>` string anywhere in its text). `in` adds every record that holds a pointer to a selected
   record. `both` does both from every reached record.
3. **`--max-depth N`** counts pointer hops from a driver record; driver records are depth 0. The default is
   unlimited. Each selected record's depth is its shortest hop count, as a breadth-first search gives it.
4. **`--sections a,b,c`** lists the sections that may be traversed and written. A pointer into a section not
   listed is not followed and is not reported as dangling; the record that holds it keeps the pointer in its
   bytes. When a list is given, the driver's section must be in it.
5. **No driver means the whole bundle**, as today, and the engine is not used. With the `clinvar-gkm` spec and
   no driver, the output stays identical to ClinVar-GKM's BigQuery export layout: the engine is never in the
   path of a full build, so `--spec clinvar-gkm-1.0` with no driver remains the BigQuery-parity build.
6. **The output is a mini-bundle**: the selected records verbatim, in the exact bundle line layout (design plan
   §4.2), every record's bytes copied exactly, in source section order and source key order. The normal build
   then runs on it, with any spec.
7. **Validity.** The engine checks the source's sha256 when an expected one is known, checks `schema_version`
   against `compat.ACCEPTED_SCHEMA_VERSIONS`, checks every re-opened pass against pass 1, and fails on driver
   keys the driver section doesn't hold (naming up to ten). A pointer to a key its section doesn't hold is
   recorded as dangling in the result, never a failure (a full bundle should have none). The mini-bundle is
   re-read with `bundle.stage_bundle` before it is moved into place.
8. **Labelling.** A build from a selection carries the key-value entry `clinvar_gkm_parquet.selection` in every
   Parquet file, and a `selection` object in the manifest, holding the same canonical JSON (no timestamps):
   `{driver, where, ids_sha256, follow, max_depth, sections, seeds: {section: {count, sha256}},
   selected_records, selected_sha256}`. The entry's presence is what marks a subset; a full build has none.
   `source_bundle_sha256` and `source_deltas` still name the published inputs; `selected_sha256` names the
   mini-bundle built from them. `ids_sha256` and each seed `sha256` hash the sorted, distinct keys, one per line.

## Why a mini-bundle and then the normal build

- **The output stays derived from the bundle (ADR 0001).** Every record in the mini-bundle is a published record
  byte for byte, so every Parquet row and value is still recomputable from the published bundle. Nothing is
  re-serialized, filtered inside a record or looked up elsewhere.
- **The self-checks apply unchanged.** The mini-bundle goes through the same reader (`stage_bundle`), the same
  table SQL and the same spec checks as a full. `verify --bundle-recompute` checks a subset build against its
  mini-bundle exactly as it checks a full build against its bundle: id sets equal the mini-bundle's sections,
  and `data` equals each record. The mini-bundle's sha256 must equal `selected_sha256` in the metadata, and
  rerunning the selection with the same driver keys and options must give the same bytes (the engine is
  deterministic: gzip mtime 0, no file name, source order). The mini-bundle is not kept beside the build:
  `select` regenerates it from the recorded options, and `build --keep-work` keeps it in the work directory
  (design plan §10).
- **It works with any spec.** The engine knows bundle sections and pointers only; it knows nothing about tables.
  A pluggable spec gets selective builds for free.
- **Deltas.** The engine reads any record source that gives `(section, key, record bytes)` in section order, the
  same stream on every pass. A bundle file is one source; a replayed state (a monthly plus deltas) is another,
  so a selection can be cut from any release the tool can rebuild.

## Why `--sections` is separate from `--tables`

The pointer graph runs over bundle sections; `--tables` picks spec tables. They are different axes: in the
`clinvar-gkm` spec one section, `evidenceLine`, feeds three tables (`evidenceLine`, `vcv_evidenceLine`,
`rcv_evidenceLine`), and another spec may map sections to tables differently again. `--sections` bounds the walk
and the mini-bundle; `--tables` then picks which tables the build writes from it.

## The engine and its cost

- **Streaming, multi-pass, bounded memory.** Each pass streams the source once; nothing the size of the bundle is
  held in memory or staged. Memory holds the selected keys and the pending pointers, never records. Disk holds
  the output and a gzip spool of the selected records, which shrinks as the output is merged from it.
- **Outgoing pointers** are found by a regex over a selected record's text. **Incoming pointers** are found by
  scanning the records of each section that can point into the current frontier (a section-to-section map
  learned in pass 1), parsing only records that name a frontier section. Records are never JSON-parsed.
- **Shortest depth.** A pass may assign depth d only when every record of depth d − 2 or less is already
  selected, so no shorter path can exist. A pass therefore completes one level and part of the next.
- **About one bundle pass per hop, with an early stop.** Pass 1 always reads the whole source (sha256, grammar,
  counts, driver keys). Pointers mostly point to earlier sections (vcv → proposition → variation → allele →
  location → sequenceReference), so each hop back is a new pass; a later pass stops at the first section after
  which nothing is wanted, and skips sections with nothing wanted without parsing their keys. `in` needs one
  scanning pass per level. Measured on the real excerpt (331 records; Apple M2 Pro, 2026-10-26): `out` from the
  somatic VCV `VCV004530381.1-S-SCI-CP-T1`, unlimited depth, selects 14 records in 11 sections in 6 passes, the
  later ones stopping before `vcv`, `scv`, `evidenceLine`, `allele` and `location`; `in` from the same VCV
  selects 2 records in 2 passes, and `both` 15 records in 7 passes.
  On the excerpt replicated 1,000 times (331,000 records, keys and pointers suffixed per copy, records about
  4 times the full's average size), a pass that only looks up keys reads about 1 M records/s, and pass 1 of an
  `in` scan about 0.23 M records/s. **Estimate** for a 1.1.0 full (91.5 M records, 4.8 GB): 1.5–3 minutes per
  read-only pass, dominated by decompression; `out` from VCVs about 6 passes, most of them partial. Every pass
  re-reads the source, so a local copy is much faster than a URL.
- **`both` with no depth limit reaches most of the bundle** through hub records (`sequenceReference`, `gene`,
  `submitter`, `condition`): on the excerpt, `both` from one SCV reaches 60 of 331 records in 13 passes. Use
  `--max-depth` or `--sections` with `both`.

## Consequences

- A subset is never mistaken for the published set: the `clinvar_gkm_parquet.selection` entry and the manifest's
  `selection` object say so, and name the driver, the options and the mini-bundle's sha256. ClinVar-GKM's publish
  step should refuse a dataset that carries the entry, and the oracle (`verify --against`) compares full builds
  only.
- Wired (`driver.py`, `build.py`, `verify.py`): the `build` options (`--driver`, `--where`, `--ids`,
  `--ids-file`, `--follow`, `--max-depth`, `--sections`) and the `select` command; the `--where` evaluation, by
  rendering the driver table's SQL with the spec's renderer (any spec: built-in, or custom through the generic
  renderer) over the staged driver section (a full stage of one section) and selecting its keys; the key-value
  entry and the manifest object; and `verify --bundle-recompute` against the mini-bundle.
- `scripts/extract-fixtures.py` shares the engine's re-open check (`Tap`), pointer scan and line regexes.
- The engine relies on the one-record-per-line layout, as the reader does (design plan D4).
