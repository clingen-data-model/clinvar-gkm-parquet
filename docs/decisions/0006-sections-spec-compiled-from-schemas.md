# 0006 — Tables are compiled from a bundle schema and its column set

- **Status:** Accepted
- **Date:** 2026-11-02
- **Plan (if any):** [`docs/plans/2026-10-20-design.md`](../plans/2026-10-20-design.md) §3, §4.4, §7, §9, §11; amends
  the version gate of [ADR 0002](0002-independent-tool-versioning.md); builds on
  [ADR 0001](0001-parquet-derived-from-bundles.md), [ADR 0003](0003-pluggable-table-specs.md) and
  [ADR 0005](0005-lossless-sections-build.md)

## Context

The default `sections` spec (ADR 0003) was a checked-in file, `spec/sections/1.0/tables.yaml`, written by hand from
the ClinVar-GKM producer's procedures and checked against the schemas and real records. A new bundle
`schema_version` stopped the build until someone researched a new spec and cut a tool release. The maintainer wants
the conversion driven by the bundle's schemas.

A check on 2026-11-02 walked the 1.1.0 schemas (clinvar-gkm, va-spec, cat-vrs, vrs, gkm-core) with a prototype and
compared the result with the spec:

| Fact | In the 1.1.0 schemas? | Measured |
| --- | --- | --- |
| Sections, their order, each section's class | yes | matches the current list |
| Top-level properties | yes | every spec column present, 20 of 20 sections |
| Nested positions | mostly | 393 of 457; the other 64 are all inside extension values |
| Extension value shapes | no | 464 of 471 walked positions are open (`additionalProperties: true`) |
| Pointer targets | no | no `^#/` pattern in ClinVar-GKM's own schema files; the prototype's targets matched 14 of 28 |
| Pointer or inline | no | 22 positions allow `iriReference` or a class and are written inline |
| Which allowed properties are emitted | no | `scv`: 313 walked paths, 108 in the spec; `variation`: 563, 50 |
| Key order on the bundle line | no | 7 sections use the producer's STRUCT order; no record matches the schema's property order |

These counts come from a prototype, so they are indicative. The gaps are real: a schema states what a bundle *may* hold,
and the tables need what it *does* hold.

An earlier draft of this ADR asked ClinVar-GKM to close those gaps by narrowing its schema profiles (a "schema contract":
only emitted properties, closed extension values, pointer patterns, sorted keys). Trying it measured the cost: 12
hand-written narrowed classes for 9 of the 20 sections, because the bundle schema references GA4GH classes directly
(`MappableConcept` for `gene` and `condition`) and `Coding` alone is emitted with four field sets. A column file does the
same work: 90 lines for all 20 sections. And a real second producer would have had to do the same to its schema.

The maintainer decided on 2026-11-02: ClinVar-GKM publishes a **column-set file with each schema tag**, instead of
narrowing its profiles. And: "I'd like this function to work with any bundle and its schema/column-set file pairing. I do
not want clinvar-gkm specific logic embedded." It is meant to move to the `ga4gh.gkm` package (GKM Toolkit) in
`ga4gh/gkm-starter-kit`. GKM bundles are named collections from a producer's bundle schema, `#/...` local pointers and
`$ref`s to versioned GA4GH `w3id.org` URLs, distributed as `bundle.json` and `bundle.schema.json`. Real ones differ from
ClinVar-GKM's: CIViC's have no `schema_version`, are pretty-printed, hold empty collections and hold root values that
aren't collections (`metadata`, `failedAssertionIds`), and its schema's `$id` is a URN that can't be fetched.

## Decision

1. **A table spec is compiled from a pairing: a bundle schema and its column set.** `build --bundle-schema <path|url>
   --column-set <path|url> --full <bundle>` works on any bundle, as does `compile-spec` to read the result. The output
   of the compiler is an ordinary spec: the format, the loader's checks, the build engine, `rebuild` and `verify` are the
   same as for a spec file.
2. **The column set is required and authoritative.** The schema says what a position may hold; the column set says what
   is built. It names the schema it pairs with by `$id`, and a mismatch is refused. Its format is
   [`docs/column-set-format.md`](../column-set-format.md), written so another producer can follow it alone. An entry that
   names a property the schema doesn't have (outside an open value), contradicts the schema's type, or isn't used stops
   the compile.
3. **The rules live in the compiler and name no producer:**

   | Pattern | Rule |
   | --- | --- |
   | A root property with `patternProperties`, or a schema in `additionalProperties`; or one the column set lists under `collections` where the schema can't say | a collection: one table, in schema order |
   | A bundle-level `allOf` member: `patternProperties` from a collection-name pattern to `{additionalProperties: <record schema>}` | narrows the records of the collections it matches |
   | Any other root property but the declared version | a root value, kept as JSON text in a generic `root_values` table that exists only when a bundle has one |
   | A property the column set lists | a column of the same name, or `prop as name`; in schema order |
   | The record's `id`, if listed | the key column, which is the bundle key either way |
   | Object, or array of objects | a nested STRUCT, or a list of them |
   | A union of objects | one merged STRUCT: the first branch's fields, then each further branch's new fields |
   | `allOf` members declaring one property | both hold: a profile narrows its base |
   | `->a\|b` | a bare id in the property's column, plus `<column>_section` if it has several targets |
   | Integer or `Range` at the top level of a record | `<prop>`, `<prop>_min`, `<prop>_max` |
   | An `extensions` list | `{name, value_<json type>}` slots, one per JSON type its values can take |
   | `json`, on a record property | the value as compact JSON text, for a map whose keys are data or a position that is a pointer in one record and an object in another |
   | Anything else with no shape to settle it, a nested integer-or-Range, a pointer with several targets inside a value, or a class that reaches itself | stops the compile, naming the position and what the column set must state |

4. **Two readers behind one sink, chosen per file by its first lines.** Layout is a property of the file, not of the
   pairing: ClinVar's sub-bundle is pretty-printed under the same schema as its line-layout monthlies. A file in the line
   layout is streamed by the strict grammar (empty collections and root values now included). Any other JSON is read
   whole, with `json.load`, in memory, and turned into the canonical line layout with every record's keys sorted, then
   read by the same grammar; a duplicate key or a non-JSON number stops it. A bundle with no version works.
5. **The round trip says what it promises.** Line layout: byte-identical (ADR 0005). Any other layout: the file's bytes
   aren't kept, so the promise is JSON-equal, and `rebuild` writes the canonical line layout. The manifest records which
   (`round_trip`), `rebuild` and `verify --roundtrip` state it, and neither claims byte-identity on a normalized input.
   `key_order` applies only to line-layout input and defaults to sorted, so a producer who doesn't use the layout can
   omit it. The build's row-by-row check (ADR 0005) stays the safety net: a record the compiled spec can't reproduce
   stops the build, whatever the schema said. Floats are represented, as DOUBLE, and that check confirms their text.
6. **The default bundle is data.** With no flags, the tool reads ClinVar-GKM bundles through
   `spec/sections/default-bundle.yaml`: the version property and URL templates for the schema and the column set. A
   shipped snapshot of a version works offline, and any other is fetched once and cached. 1.1.0's column set ships here
   (`spec/sections/column-sets/1.1.0.columns.yaml`) because that tag is published. So a version is accepted for the
   default when its pairing loads and compiles; anything else is refused with the reason. That replaces, for `sections`,
   the fixed list of ADR 0002. `clinvar-gkm` and `baseline` keep their lists, and read only the line layout; a custom spec
   stays bound by the versions it lists, its sections checked against those versions' schema sets. A followed `$ref` must
   be a `https://w3id.org/` URL or a pointer within a file; the root schema can be anywhere, with any `$id` form.
7. **A set carries what it was compiled to.** A generic set can't assume its pairing is on hand later, so a compiled build
   writes its compiled spec into the output directory (`table-spec.json`, beside the manifest) and records
   `schema_set_sha256`, `column_set_sha256` and `table_spec_sha256` in every file and the manifest. `verify` and `rebuild`
   load that file, check its hash against the files, and refuse a mismatch, saying which tool version wrote the set. They
   don't recompile or need the schemas. `table_spec_sha256` covers only what affects the tables: `description`,
   `transform`, `sources` and `status` are left out, so rewording changes nothing and a type, column order, path, target or
   key order does. A bundle in another layout carries the key-sorted spec its records were checked against, with the same
   columns.
8. **The generic core holds no producer.** `compiler.py`, `schemas.py`, `columnset.py`, `bundle.py`, `transport.py`,
   `model.py` and `errors.py` hold no ClinVar-GKM name or URL and import nothing that does; a test enforces this by import
   graph and by string scan. The one allowed name is the tool's own distribution name, in one constant.

## Consequences

- Any producer can get Parquet by writing a column set, with no tool release: CIViC's two real bundles go through
  `build`, `verify`, `verify --roundtrip` and `rebuild` on a 56-line column set, JSON-equal, `metadata` included.
- A schema bump needs a matching column set, published by the producer. A new pattern the rules don't cover stops at
  compile time with the position named, and needs a tool change. That is the limit of the generic path.
- A column set is only as complete as the records it has been run on. A property a bundle holds that it doesn't list stops
  the build, with the record named; a new extension value kind stops it too. An empty collection's columns are the
  author's statement of what it would hold.
- `json` columns aren't typed: a query parses them, and a pointer inside one stays as written. CIViC needs them for maps
  keyed by VRS id (`variant.protein`), and for `assertion.specifiedBy`, a pointer in one record and an object in another.
- The general reader holds a bundle in memory, so a very large bundle needs the line layout. CIViC's and the sub-bundle are
  small; ClinVar-GKM's monthlies are in the line layout and stream as before.
- Deltas, selective builds, `sources.py`'s release logic and `verify --against` stay ClinVar-GKM's layer: deltas and
  selection read the default bundle's line layout, and refuse a pairing.
- Moving to the toolkit takes care with dependencies: the toolkit is Python >=3.11 with `jsonschema` and `requests`, no
  DuckDB, and PyYAML only in its docs extra. The compiler uses DuckDB only for its type printing, and YAML is the column
  set's format. The build needs DuckDB in any case, so it would be an optional extra. The key-value prefix
  `clinvar_gkm_parquet.` and the tool's name are one constant each, renamed at the move.
- The column set of a release is what the producer states, so a producer's change can change the columns with no tool
  change. A table-spec version alone doesn't name a column set: the identity of a compiled table set is `table_spec_sha256`
  (and the two source hashes say what it came from).
- 1.1.0's bundle schema declares `$id` `.../clinvar-gkm/1.1.0/json/ClinvarGkmBundle`, which returned 404 on 2026-11-02;
  the file is served at `.../clinvar-gkm/1.1.0/clinvar-gkm-bundle.schema.json`. ClinVar-GKM's `origin/main` has fixed it
  (`4c8ecf3`, that line only). The tool needs no exception for either: a root's `$id` can be any form. **Amended 2026-11-03
  by the maintainer:** the 1.1.0 column set names only the corrected `$id`, so the snapshot's root is ClinVar-GKM's fixed
  file. The file w3id serves for 1.1.0 still declares the original, so pairing it with that column set is refused, and
  re-vendoring the snapshot from w3id would undo the fix.
- A snapshot can differ from what ClinVar-GKM built against: `$ref`s resolve to each repo's tag, not to ClinVar-GKM's
  submodule pin. For 1.1.0, va-spec's `Statement` at tag 1.1.0 (`d9da20c`) adds `direction` to `required`, one commit
  after the pin `29573487`. The bundle schema differs only in its corrected `$id`, and the other 93 of the 95 files are byte-identical to the pins. The compiler doesn't read
  `required`, so this doesn't change the output.
- `spec/sections/1.0/tables.yaml` stopped being the source. It moved to `tests/fixtures/sections-golden/tables.yaml` and
  remains as the test that the compiler reproduces it on 1.1.0, apart from listed differences (none so far).
