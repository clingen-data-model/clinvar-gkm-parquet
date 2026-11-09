# 0008 — End state: one reversible default set, custom projections, and filters both ways

- **Status:** Proposed
- **Date:** 2026-11-08
- **Plan (if any):** [`docs/plans/2026-10-20-design.md`](../plans/2026-10-20-design.md) (to be revised on acceptance);
  supersedes [ADR 0003](0003-pluggable-table-specs.md); amends [ADR 0001](0001-parquet-derived-from-bundles.md),
  [ADR 0002](0002-independent-tool-versioning.md), [ADR 0004](0004-driver-based-selection.md),
  [ADR 0005](0005-lossless-sections-build.md), [ADR 0006](0006-sections-spec-compiled-from-schemas.md) and the proposed
  ADR 0007 ([PR #13](https://github.com/clingen-data-model/clinvar-gkm-parquet/pull/13))

## Context

ADRs 0001–0007 were written one decision at a time, while two goals ran side by side: matching ClinVar-GKM's 22-table
BigQuery export (the oracle), and a one-table-per-section set compiled from the bundle schema that can be turned back
into the bundle. On 2026-11-08 the maintainer set the end state and chose between them:

> I want the final outcome of all this effort to be a tool that is used by the clinvar-gkm pipeline during release
> creation that can build a consistent parquet set of tables from the bundle and then have the ability to use the
> parquet tables to recreate that bundle, when explicitly requested by a user of this tool.

The maintainer also made four decisions:

1. "The same bundle" means **byte-identical uncompressed text**.
2. ClinVar-GKM publishes `v1/1.1/parquet/` as **the default reversible set only**. "The current 22-table bigquery set is
   going away forever." ClinVar-GKM's docs are updated once the default works.
3. Filters follow pointers by default, in both directions, with a plain row filter as an option.
4. The end state is recorded here, with the earlier ADRs revised to match.

The maintainer also asked for custom column sets: a user's own, smaller extraction, which can't recreate the original
bundle but can produce a JSON bundle that round-trips with its own Parquet, and row filters for both the default and
custom outputs.

What already exists (evidence, with dates):

- **Byte-identical round trip.** A full `sections` build rebuilds the bundle text byte for byte: the 2026-09-23 weekly
  delta, 4,530,457 records (ADR 0005, 2026-10-30), and the 2026-10-31 full, 91.85 M rows, with the hand-written 1.1.0
  column set (2026-11-04, PR #13).
- **Schema-generated default.** A scratch generator built a column set from a narrowed pilot schema for five sections
  (`gene`, `condition`, `conditionSet`, `therapy`, `therapyGroup`; 217,327 records of the 10-31 full, 2026-11-06), and the
  build rebuilt them byte for byte. It needed a hand-added key order for `condition` and `conditionSet`: the schema
  doesn't state the order the producer writes keys in, and without it the build stops.
- **Full-release builds.** Tool 1.0.1 built the 10-18 and 10-24 weekly fulls (about 17.5 minutes and about 29 GB peak
  each, `e2-standard-8`), and a replay of the October monthly plus three deltas. Every typed column of all 22 tables
  matched BigQuery's (2026-11-06; reports in `gs://clinvar-gkm/oracle/reports-1.0.1/`).
- **Replay limits.** The replay of 10-18 differs from the 10-18 weekly in 1,519 `condition.aliases` lists: same values,
  another order. The published monthly and weekly order them differently, and no delta carries the record (checked on
  `clinvar.trait:1`). So monthly plus deltas doesn't reproduce a weekly byte for byte.

## Decision

### 1. The default set is the product, and it is reversible

1. **One default set per bundle schema.** It is generated from the bundle schema alone, by ADR 0007's policy: one table
   per section, every allowed property a column, nested objects kept as STRUCTs, `ordinal` last. A producer publishes its
   bundle schema; it publishes no column set for the default.
2. **The default round trip is byte-identical** (decision 1, which replaces ADR 0007 decision 5). `rebuild` turns a full
   default build back into the bundle's exact uncompressed text, checked against the text sha256 the build recorded.
   Rebuilding needs the Parquet set and the tool version that wrote it. It needs no column set, because the set carries
   its compiled spec (ADR 0006 decision 7). On request it also checks that spec against the bundle schema.
3. **Key order comes from the schema, not from a hand-written file.** The schema has to be enough to rebuild the bytes,
   so the default policy states each object's key order: **keys sorted by their UTF-8 bytes**, unless the schema states
   another order. A producer that wants byte-identical round trips from its schema alone writes its records that way.
   For ClinVar-GKM that means writing keys sorted (today 7 of its 20 sections use its STRUCT order) at the narrowed
   schema version, which changes every record's bytes anyway. A bundle whose key order the schema doesn't state still
   builds; the build checks key-sorted JSON, the round trip is JSON-equal, and the manifest says so. *Open for review:*
   whether ClinVar-GKM sorts its keys, or the schema states its order with an annotation the GKM schema tooling carries
   through. The first needs no new tooling, so it is recommended.
4. **It is the published set.** ClinVar-GKM's release builds the default set from the release's weekly full with a pinned
   tool release, runs `verify --bundle-recompute` and `verify --roundtrip --full <bundle>`, and publishes only if both
   pass. It publishes the manifest the tool writes (ADR 0001 decision 4). The round trip, not a comparison with
   another system, is what shows the set is complete and correct.

### 2. The 22-table set and the other built-in specs are retired

1. **`clinvar-gkm` goes** (decision 2): the spec, its SQL files, `verify --against` and the oracle as a gate. It is
   deprecated in the next tool release and removed in the following MAJOR. Nothing waits for an oracle pass any more.
   The 2026-11-06 reports stay as a record of how close the tool came to the BigQuery export.
2. **`baseline` goes too**, in the same MAJOR. It was an alternative default (id, pointers, record verbatim). The default
   set is reversible on its own, and a custom column set covers other shapes.
3. **A hand-written `tables.yaml` is no longer an input.** Custom output is defined by a column set (section 3). The
   table-spec format stays as the compiler's output and as the `table-spec.json` a set carries.

### 3. Custom column sets are projections, and round-trip with their own bundle

1. **A custom column set may leave things out, if it says so.** A column set that declares itself a **projection** may
   omit sections, properties and fields. The default stays strict: a record holding something its schema doesn't allow
   stops the build. In a projection, an omitted property is dropped by declaration. An undeclared one still stops the
   build, so nothing is dropped by accident.
2. **A projection's output is labelled lossy.** Every file's key-value metadata and the manifest record that the set is a
   projection, the column set's sha256 and the source bundle's sha256. It can't be mistaken for the default set.
3. **It can't recreate the original bundle.** `rebuild` refuses to write the source bundle from a projection. It writes
   the **projected bundle** instead: the same line layout, section order and record order, each record holding only the
   projected properties, under their bundle names (renames reversed), in the default key order.
4. **The projected bundle and the projection round-trip exactly.** The build records the projected text's sha256, and
   `rebuild` checks against it. Building the projected bundle with the same column set gives the same tables. This holds
   for every mapping a column set can state today (keep, rename, nest, pointer to bare id with its section, Range
   bounds, extension slots, JSON text). A later mapping that can't be reversed makes a set one-way, and `rebuild` refuses
   it with the reason.

### 4. Filters, in both directions, for the default and for projections

1. **Bundle → Parquet.** ADR 0004's selection (`--driver`, `--where`, `--ids`, `--ids-file`, `--max-depth`,
   `--sections`) works with any pairing and any column set, default or projection. It is generic GKM behaviour (`#/`
   pointers), not ClinVar-GKM's layer.
2. **Pointer-following is the default** (decision 3): `--follow out` keeps the selected records plus every record they
   point to, so the result has no dangling pointer inside the selected sections. **`--follow none` is the plain row
   filter**: only the matching records, with pointers out of them recorded as dangling in the selection, never an error.
3. **A filtered build is reversible to its mini-bundle.** It rebuilds byte for byte to the mini-bundle the selection
   wrote (`selected_sha256`, ADR 0004 decision 8), which lifts ADR 0005's refusal of selections.
4. **Parquet → bundle.** `rebuild` takes the same filter options over a full default set and writes the filtered bundle.
   It is byte-identical to what the forward selection with the same options writes from the source bundle, and it
   carries the same `selection` record. On a projection, the same options give the filtered projected bundle.

### 5. What stays

- ADR 0001: the bundle is the record, and every value is recomputable from it.
- ADR 0006's compiler, the two readers and the generic core, which still move to the GKM Toolkit when asked.
- ADR 0002's independent versioning. The version gate is ADR 0006's: a bundle is read when its pairing loads and
  compiles. The default's table-spec version is ADR 0007's policy version, and a set's identity is its
  `table_spec_sha256`.

## Earlier ADRs

| ADR | Disposition | What changes |
| --- | --- | --- |
| 0001 | Amended | Stands. Its consequence that the oracle measures how close the tool comes to BigQuery's content is no longer a gate (2.1). |
| 0002 | Amended | The explicit `schema_version` list for `baseline`, `clinvar-gkm` and custom specs goes with them (2); ADR 0006's gate applies to every set. |
| 0003 | **Superseded** | Its built-in `clinvar-gkm` and `baseline` specs and the custom `tables.yaml` input are retired (2). Its one-table-per-section shape lives on in ADRs 0005–0007. |
| 0004 | Amended | Selection becomes generic, gains `--follow none` and a reverse direction, and its "BigQuery-parity build" note no longer applies (4). |
| 0005 | Amended | The promise moves from `sections` to the default set, with key order from the schema (1.3), and a filtered build is reversible to its mini-bundle (4.3). |
| 0006 | Amended | The default needs no column set (ADR 0007). Selection now accepts a pairing (4.1); deltas still don't. A custom column set may be a projection (3). |
| 0007 (proposed) | Revise before acceptance | Decision 5 (JSON-equal default, `key_order` as an add-on) is replaced by 1.2 and 1.3. The rest stands. |

None is removed: superseded and amended ADRs stay, with a note at the top (README, Conventions).

## Consequences

- **Users of the published Parquet get a new layout.** The 22 tables are replaced by one table per section, typed, with
  no `data` column. That is a breaking change for every query against `v1/1.1/parquet/`. ClinVar-GKM announces it and
  updates its docs (`docs/parquet/tables.md` and the planned Ecosystem → Add-ons page) when the default ships.
- **The default needs ClinVar-GKM's narrowed schema** (ADR 0007 decision 6, the pilot on `feat/narrow-profiles-pilot`),
  and, for byte-identical round trips from the schema alone, keys written in the order the schema states (1.3). Until
  both ship, ClinVar-GKM keeps its BigQuery export.
- **A past week isn't byte-reproducible from a monthly plus deltas** while ClinVar-GKM's array order is unstable between
  releases (the 1,519 aliases above). Rebuilding a weekly's text needs that weekly's full, which is published only for the
  current month. A deterministic array order in the assembler would make replays byte-exact.
- **Removing `clinvar-gkm`, `baseline`, `tables.yaml` input and `verify --against` is a tool MAJOR** (2.0.0), after a
  release that deprecates them.
- **Work this needs, in order:** the replay fixes found on 2026-11-06 (`--full` builds read the line index by default; a
  memory limit that fits a 32 GB machine); ADR 0007's generator and compiler rules, with key order from the schema;
  selection on any pairing and in `rebuild`; projections; the deprecation release; the MAJOR; then the toolkit move,
  when asked.
- **On acceptance**, `CLAUDE.md` (the oracle gate, the explicit version lists), `README.md` (status, matrix, specs) and
  the design plan (§3, §8.2, §12, D9, D13, D14) are revised to match.
