# 0005 — A full sections build is a lossless encoding of the bundle text

- **Status:** Proposed
- **Date:** 2026-10-30
- **Plan (if any):** [`docs/plans/2026-10-20-design.md`](../plans/2026-10-20-design.md) §3.4, §7.2, §7.3, §10;
  builds on [ADR 0001](0001-parquet-derived-from-bundles.md) and [ADR 0003](0003-pluggable-table-specs.md)

## Context

The default `sections` spec (ADR 0003, amended 2026-10-29) writes one table per bundle section, every record
property its own typed column, with no `data` column. Without the record's text, a reader can't tell whether the
columns hold everything the bundle said, or get the bundle back from the Parquet. A check of the draft spec on
2026-10-30 found what such a build loses: the record order of each section, the key order of each object (some
sections are written in the producer's STRUCT order, others key-sorted by BigQuery), which section a multi-target
pointer named (`scv.proposition` points into one of four proposition sections), the difference between an
absent and an empty list inside a typed value (both were `[]`), and which extension slot held the value.

The bundle is the record (ADR 0001), so the question is whether the Parquet can be trusted to say the same
thing. If a full `sections` build can be written back into the bundle's exact text, the answer is yes, and it is
checkable.

## Decision

1. **A spec can declare itself lossless** (`lossless: true`, spec format in `src/clinvar_gkm_parquet/spec.py`).
   The built-in `sections` spec does. A full build of a lossless spec from one bundle (no deltas, no selection,
   every table) is a lossless encoding of the bundle's **uncompressed text**: `clinvar-gkm-parquet rebuild <dir>
   -o <bundle.json[.gz]>` writes it back byte for byte (section order, record order, key order, escaping,
   number text, the assembler's line layout). The gzip bytes are not part of the promise (header time, file
   name, zlib build); the text's sha256 is.
2. **What makes it lossless is in the spec, machine-readably**, so no rule lives only in code:
   - each table has an `ordinal` column, last: the record's position in its section (derived from the bundle,
     ADR 0001);
   - each table has a `json` block: the key order (`columns`: the column and STRUCT order; `sorted`: every key
     sorted by its UTF-8 bytes, as BigQuery's JSON type writes them; `sorted_columns`: the values ClinVar-GKM's
     export passes through `TO_JSON`) and whether the record carries its own `id`;
   - every pointer names its target section(s) (`targets`, and `pointers` as a mapping from position to
     sections); a pointer with more than one target has a `<column>_section` column beside it holding the
     section it names.
3. **Multi-target pointers keep their section in a column, not a lookup.** Resolving a bare id by looking it up
   in the target tables works in a full monthly, which holds every record its pointers name, but not in a weekly
   delta or any partial bundle: in the 2026-09-23 delta, 250,580 of 700,716 `vcv.proposition` targets and 835,253
   `evidenceLine.hasEvidenceItems` targets are not in the delta. A lookup would also tie the meaning of one table
   to the presence of others. The section column is exact, cheap (a few distinct values, dictionary-encoded) and
   tells a query which table to join.
4. **Absent and empty are kept apart at every depth.** In a table with a `json` block an absent list inside a
   typed value is NULL, as at the top level, and an empty one is `[]` (before, both were `[]`, BigQuery's rule,
   which the `clinvar-gkm` spec keeps). An extension's unused list slots are NULL too. The alternative, keeping
   `[]` for absent and refusing any bundle that holds an empty list, would rest on a producer convention
   (`gkm_json_cleanup.strip_empty`) instead of on the data, and make the columns mean something other than
   what the bundle says.
5. **The build proves it, row by row.** A table with a `json` block also carries the bundle line through its SQL;
   every row is written back as JSON from its columns (the same expression `rebuild` uses) and compared with
   the bundle line, key text included. Any difference stops the build, naming the key and the first differing
   character. So a record the columns can't reproduce (a JSON `null`, a float, a key order the spec doesn't
   state, an escape the producer doesn't make, an open Range `[null, null]`) never yields a "lossless" build.
6. **The build records the text's hashes and `rebuild` checks them.** While streaming, the build hashes the
   uncompressed text: each file carries `clinvar_gkm_parquet.section_text_sha256` (its section's record lines as
   written) and the manifest `source_text_sha256` and `source_text_size`. `rebuild` compares each section as it
   writes it and the whole text at the end, and keeps no output on any difference. It refuses, before writing,
   anything it can't reproduce: another spec, a selection, a build with deltas or `--tables`, a missing table,
   ordinals that are not `0..n-1`, a pointer section outside its column's targets. `verify --roundtrip` runs the
   same rebuild without writing (and, with `--full`, also compares with that bundle's text).

## Consequences

- The Parquet set can stand in for the bundle when it must: a user who has only the Parquet (and the tool
  version that wrote it) can recreate the bundle's text and check it against the hashes the build recorded. The
  published `.sha256` is of the gzip file, which a rebuild doesn't reproduce; the text's hash is in the manifest.
- The `sections` tables gain columns (`ordinal`; `members_section`, `concepts_section`, `object_section`,
  `conditionQualifier_section`, `hasEvidenceItems_section`, `proposition_section`), and nested absent lists read
  as NULL. `sections` 1.0 was an unreleased draft, so it changed in place.
- The build does more work per row (each record is written back once). On the 2026-09-23 weekly delta (4,530,457
  records) the build took 41.2 s (29.9 s without the check) and the rebuild 40.0 s (design plan §10).
- A new value kind in the bundle (a float, a key order the spec doesn't state) stops a `sections` build until the
  spec states it: that is the point, and it comes with a schema bump on ClinVar-GKM's side.
- Writing JSON text exactly depends on DuckDB's `to_json` (pinned, D8) and on the producer writing with Python's
  `json.dumps(ensure_ascii=False)` (checked on every record of the delta: the producer's text equals Python's
  re-serialization). The one difference found, upper-case hex in `\u` escapes, is corrected (`cgp_jstr`).
- A build with deltas is not one bundle's text, so it records no text hashes and can't be rebuilt; rebuilding a
  past week's bundle means building that week's full.
