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

1. "The same bundle" means **record-identical** (revised 2026-11-09; on 2026-11-08 it was byte-identical uncompressed
   text): "It really doesn't matter about the order of the records, but every record must be accounted for and each
   record must contain exactly the same content regardless of the ordering of attributes or arrays."
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
- **The schema gives every record's content; only text needs more.** The ADR 0007 prototype (2026-11-05) generated the
  default from the unnarrowed 1.1.0 schema and from CIViC's, and every fixture rebuilt JSON-equal. Bytes needed more:
  the order of each object's keys, which no schema states. A measurement on the whole 10-31 full (2026-11-09;
  91,854,927 records; results in `gs://clinvar-gkm/oracle/keyorder-2026-10-31/`) found that 54,644,043 records in 7
  sections aren't written key-sorted, and that one key order per path, learned at build time and stored once per table
  (7.2 KB in all), would reproduce them. That was workable, but the maintainer chose record identity as the simpler
  promise (decision 1).
- **Replays differ only in order.** The replay of 10-18 differs from the 10-18 weekly in 1,519 `condition.aliases`
  lists: same values, another order. The published monthly and weekly order them differently, and no delta carries the
  record (checked on `clinvar.trait:1`). Every typed column of all 22 tables matched otherwise (2026-11-06). So
  monthly plus deltas doesn't reproduce a weekly byte for byte, but it does record for record.

## Decision

### 1. The default set is the product, and it is reversible

1. **One default set per bundle schema.** It is generated from the bundle schema alone, by ADR 0007's policy: one table
   per section, every allowed property a column, nested objects kept as STRUCTs. Record order isn't content, so the
   default has no `ordinal` column. A producer publishes its bundle schema; it publishes no column set for the default.

   A section whose records may be any of several classes (a `oneOf` or `anyOf`), and a nested position that may, is
   still one table or one STRUCT: each property name is one column or field, however far up each class's `allOf` chain
   it is declared, and its type is what the classes allow there taken together. Nothing is repeated per class, and the
   record's own `type` says which class it is. In ClinVar-GKM 1.1.0, `variation` is one of four classes
   (`ClinvarCanonicalAllele`, `ClinvarCategoricalCnvChange`, `ClinvarCategoricalCnvCount`,
   `ClinvarNonConstrainedVariant`), all of which take their nine properties from `CategoricalVariant`, one to two levels
   up, and each narrows `constraints`. The generated default has the nine columns once each, and `constraints` is one
   list of one STRUCT whose 14 fields cover every constraint class the four allow (the ADR 0007 prototype, 2026-11-05).
   Same-named properties are shared because they share a name, whether or not they come from a common parent class.
   Where the classes disagree on a type, the position follows the rules for a position that may hold either form
   (ADR 0007's string-or-object split, or JSON text).
2. **The default round trip is record-identical** (decision 1, which replaces ADR 0007 decision 5). `rebuild` turns a
   full default build back into a bundle with the same root properties and, in every section, exactly the same records
   by key: none missing, none extra, none duplicated. Each rebuilt record equals its source as a JSON value:
   - an object is compared as a set of name–value pairs, so key order doesn't count;
   - an array is compared as a multiset, the same elements the same number of times, so element order doesn't count;
   - a string is compared as its decoded text, so escapes don't count, and a number as its value, so its spelling
     doesn't count.

   Whitespace, record order and section order don't count either. The rebuilt text is therefore not the published file,
   and its sha256 isn't the published `.sha256`; anyone who needs the original bytes downloads the original file.
   Rebuilding needs the Parquet set and the tool version that wrote it. It needs no column set, because the set carries
   its compiled spec (ADR 0006 decision 7). On request it also checks that spec against the bundle schema.
3. **A record digest is the round trip's identity.** Each record has a canonical form: RFC 8785 (JCS) serialization,
   with every array's elements put in canonical form first and then sorted by that text. The build hashes each record's
   canonical form and records a **records sha256** for the bundle: the sha256 of the sorted lines
   `<section> TAB <key> TAB <record sha256>`, plus a line for the root properties. `rebuild` and `verify --roundtrip`
   recompute it from their output and compare. The build's row-by-row check (ADR 0005 decision 5) stays: it compares
   each rebuilt record's canonical form with its source's, so a record the set can't reproduce stops the build, naming
   the record. Nothing about text is stored: no key order, no per-row order column, no `key_order`, and a producer
   changes nothing. Parquet lists keep the order the build read, so a rebuild still writes arrays in source order; the
   check just doesn't depend on it.
4. **Narrowing is optional.** Reversibility needs nothing from the producer's profiles. A property the producer never
   writes is an all-NULL column and rebuilds as absent. A string-or-object position keeps the string as written plus
   `<prop>_json` (ADR 0007). An open value stays JSON text. Every GKM bundle writes local pointers as
   `#/<section>/<key>`, so at every string position that may hold an IRI the default stores the bare id and a
   `<prop>_section` column. A non-NULL section rebuilds `#/<section>/<id>`, and any other string stays as written. This
   needs no `pattern` in the schema, and it relies only on conventions every VA-Spec/GKM bundle shares. A producer may
   still narrow its profiles to get fewer columns and typed values. That is a choice about table shape, not a condition
   for the round trip.
5. **It is the published set.** ClinVar-GKM's release builds the default set from the release's weekly full with a pinned
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
   the **projected bundle** instead: the same sections, each record holding only the projected properties, under their
   bundle names (renames reversed).
4. **The projected bundle and the projection round-trip exactly.** The build records the projected bundle's records
   sha256 (1.3), and `rebuild` checks against it. Building the projected bundle with the same column set gives the same tables. This holds
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
3. **A filtered build is reversible to its mini-bundle.** It rebuilds record-identical (1.2) to the mini-bundle the
   selection wrote, checked against that mini-bundle's records sha256, which lifts ADR 0005's refusal of selections.
4. **Parquet → bundle.** `rebuild` takes the same filter options over a full default set and writes the filtered bundle.
   It is record-identical to what the forward selection with the same options writes from the source bundle, and it
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
| 0005 | Amended | The promise moves from `sections` to the default set and weakens from byte-identical text to record-identical (1.2), checked by a records sha256 instead of the text sha256 (1.3). A spec's `json` block and `key_order` go. A filtered build is reversible to its mini-bundle (4.3). The row-by-row check stays. |
| 0006 | Amended | The default needs no column set (ADR 0007). Selection now accepts a pairing (4.1); deltas still don't. A custom column set may be a projection (3). |
| 0007 (proposed) | Revise before acceptance | Decision 5 (JSON-equal default, `key_order` as an add-on) is replaced by 1.2 and 1.3: JSON-equal with arrays as multisets, and no `key_order`. The default has no `ordinal` (1.1). Decision 6 (ClinVar-GKM narrows its profiles) becomes optional (1.4), and the pointer rule no longer needs a `pattern` (1.4). The rest stands. |

None is removed: superseded and amended ADRs stay, with a note at the top (README, Conventions).

## Consequences

- **Users of the published Parquet get a new layout.** The 22 tables are replaced by one table per section, typed, with
  no `data` column. That is a breaking change for every query against `v1/1.1/parquet/`. ClinVar-GKM announces it and
  updates its docs (`docs/parquet/tables.md` and the planned Ecosystem → Add-ons page) when the default ships.
- **The default doesn't wait on ClinVar-GKM's schema work.** It can be generated from the published 1.1.0 schema as it
  is. Narrowed profiles (the pilot on `feat/narrow-profiles-pilot`) would make the tables narrower and more typed, and
  would mean another table change for users if they land after the default ships. ClinVar-GKM keeps its BigQuery export
  until the default ships.
- **The rules are GKM-wide, not ClinVar-GKM's.** `#/` pointers, gkm-core `Extension`, VRS integer-or-Range,
  string-or-object positions and the record-identical check are conventions of every VA-Spec/GKM bundle, and none
  depends on how a bundle is laid out. A pretty-printed bundle (CIViC's) keeps the full promise, as the line layout
  does. So the generator suits other producers' schemas and the GKM Toolkit.
- **A past week is reproducible from a monthly plus deltas,** record for record: on 10-18 the replay differs from the
  weekly only in the order of 1,519 alias lists, which the check ignores. ClinVar-GKM's assembler needs no stable array
  order for this.
- **The check is weaker than bytes, on purpose.** A change that only reorders keys or array elements, or respells a
  string escape or a number, passes. Where an array's order carries meaning, the Parquet list still holds it, and a
  query sees it; the round trip just doesn't vouch for it.
- **Removing `clinvar-gkm`, `baseline`, `tables.yaml` input and `verify --against` is a tool MAJOR** (2.0.0), after a
  release that deprecates them.
- **Work this needs, in order:** the replay fixes found on 2026-11-06 (`--full` builds read the line index by default; a
  memory limit that fits a 32 GB machine); ADR 0007's generator and compiler rules, with the records sha256 and the
  canonical row check (1.3) and the generic pointer rule (1.4); selection on any pairing and in `rebuild`; projections; the deprecation release; the MAJOR; then the toolkit move,
  when asked.
- **On acceptance**, `CLAUDE.md` (the oracle gate, the explicit version lists), `README.md` (status, matrix, specs) and
  the design plan (§3, §8.2, §12, D9, D13, D14) are revised to match.
