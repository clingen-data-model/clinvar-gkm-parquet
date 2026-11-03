# Changelog

All notable changes to this tool. Each release states the table-spec versions it writes and the bundle
`schema_version`s they read (ADR 0002, ADR 0006). Tags are `vX.Y.Z`.

## Unreleased

- **The 1.1.0 column set and schema snapshot use the corrected bundle schema `$id`**,
  `https://w3id.org/clingen/schema/clinvar-gkm/1.1.0/clinvar-gkm-bundle.schema.json`, the one on ClinVar-GKM's `main` and in
  the GKM Toolkit's copy, which now pair with the shipped column set. The original `$id` is no longer accepted with it, so
  pairing the file w3id serves for 1.1.0 (it still declares the original) with that column set is refused. A default
  1.1.0 `sections` build has the same tables, rows and `table_spec_sha256`; `schema_set_sha256` and `column_set_sha256`
  change, and with them the files' bytes. Sets built by 1.0.0 still `verify` and `rebuild`.

## 1.0.0 (2026-11-03)

First release. Snapshots bundle `schema_version` 1.1.0, and `baseline` and `clinvar-gkm` read that version; `sections` reads
any bundle with its schema and column set. Writes the table specs `sections` 1.0 (the default), `baseline` 1.0 and
`clinvar-gkm` 1.0. Pins DuckDB 1.5.4.

**Released before the oracle, by the maintainer's decision (2026-11-03).** No build has run on a full monthly bundle, and
`clinvar-gkm` hasn't been compared with ClinVar-GKM's BigQuery export. All three specs stay `draft`, so their tables can
still change, and ClinVar-GKM's published Parquet set is still its BigQuery export. The largest real input so far is the
2026-09-23 weekly delta (4,530,457 records), which rebuilt byte-identical.

- First tool code (design plan §12, step 1): package scaffold, CLI (`build`, `verify`, `version --json`),
  table spec 1.0 (draft), streaming bundle reader, delta replay, DuckDB table build, `verify`.
- Reads bundle `schema_version` 1.1.0; writes table spec 1.0 (draft until captured from the 1.1.0 export).
- Streaming build engine (design plan §6.2): the full is read once, section by section, and each section's
  tables are built in a worker thread while the next section is read. There is no state database: the deltas
  are replayed in the stream from an in-memory change map, and only the current sections are staged.
  `verify --bundle-recompute` streams the same way. The Parquet files are byte-identical to the first engine's.
  New refusals: a full whose sections are out of the assembler's order. A full with no published sha256 is
  hashed in a first pass (its tables' metadata names it).
- Table specs on the CLI (ADR 0003): `build --spec <name|path>` and `verify --spec`. **Behaviour change:** with
  no `--spec`, `build` now writes the built-in `baseline` spec (20 tables: `id`, `refs`, `data`); the 22-table
  layout of the published set is `--spec clinvar-gkm-1.0` (also `clinvar-gkm`, `clinvar-gkm@1.0`). Every file
  gains the key-value entry `clinvar_gkm_parquet.table_spec` (plus `table_spec_builtin` and `table_spec_sha256`
  for a custom spec), and the manifest a `table_spec` object. That entry is the only byte change to a
  `clinvar-gkm` build: on the real excerpt, with and without deltas, every table's rows equal the previous
  build's, and the old key set gives byte-identical files. `verify` reads the spec from the files; a custom spec
  needs `--spec <file>` with the recorded sha256. A spec may now read a subset of the bundle sections; the other
  sections are still read, checked and counted. `version` and `version --json` list the built-in specs
  (`table_specs`, `default_table_spec`).
- Selective builds (ADR 0004): `build --driver <table>` with `--where`, `--ids`, `--ids-file`, `--follow`,
  `--max-depth` and `--sections`, and a new `select` command that writes only the mini-bundle. `--where` is a
  DuckDB expression evaluated on the driver table as the chosen spec builds it, on the replayed state when
  deltas are given. Every file and the manifest of a selective build carry `clinvar_gkm_parquet.selection`;
  the source keys still name the published bundles. `verify --bundle-recompute` checks a selection against its
  mini-bundle (`--full`, whose sha256 must equal `selected_sha256`), and `verify --against` refuses a selection
  or a set not built with `clinvar-gkm`. A build with no driver never runs the selection engine.
- Python API: `build.build()` called without `spec` now writes the default spec, as the CLI does (it wrote
  `clinvar-gkm`). Pass the `clinvar-gkm` spec for the published layout.
- Faster table SQL (design plan §6.2): each record is parsed once and each row checked by a node census before
  the parsed values are trusted, with the original expressions as the fallback. The output and every fail-loud
  message are unchanged. Synthetic benchmark (`scripts/bench-tables.py`): all 22 tables 3.8× faster, `variation`
  4.6×, `scv` 3.9×; for a full-size build, an estimated 31 → 8 minutes of table SQL.
- Custom typed tables (ADR 0003): a table with no shipped `sql/<table>.sql` is built by a generic renderer from
  its derivation kinds alone (`key`, `record`, `refs`, `scalar-path`, `bare-id`, `pointer-section`,
  `typed-copy`, `extension-slot`), so a user's `tables.yaml` can map any section to typed tables. Paths take
  `[*]` list steps (`$.contributions[*].contributor`) and are checked when the spec loads; a `computed` column
  or a row filter still needs a shipped SQL file, and is refused at load otherwise. The spec format gains two
  optional keys: a typed copy's `pointers` (pointer positions stripped to bare ids) and a table's
  `known_fields` (every top-level field must be read). `clinvar-gkm` 1.0 (draft) now states both; its output is
  unchanged, and the generic SQL of its 14 generic tables is byte-identical to the shipped files'.
- **Behaviour change: the default spec is now `sections`** (ADR 0003, amended 2026-10-29): with no `--spec`,
  `build` (and `build.build()`) writes 20 tables, one per bundle section, keyed by `id`, with every record
  property its own typed column and no `data` column. `--spec baseline` writes the previous default. The
  built-in `sections` 1.0 spec is a draft (a file then; compiled since ADR 0006, entry below).
- Spec format additions: derivation kinds `range-exact`, `range-min` and `range-max` (a VRS integer-or-Range: the
  integer, else the Range's bounds; any other value stops the build); extension slots by JSON type
  (`extension_slots: {<column>: json-type}`: each extension fills the `value_<type>` slot of its value's JSON
  type, at every depth; a type with no slot, a null value or a mixed-type array stops the build); a table may
  have no `record` column (at most one). A shipped `sql/<table>.sql` now renders only the `clinvar-gkm` table it
  implements; another spec's table of the same name takes the generic renderer.
- Every `known_fields` table now also checks that the record's own `id`, when present, equals its bundle key, and
  stops the build when it doesn't. `clinvar-gkm` output is unchanged (byte-identical on the real excerpt).
- **Lossless round trip** (ADR 0005): a full build of the `sections` spec from one bundle is a lossless encoding
  of the bundle's uncompressed text. New command `clinvar-gkm-parquet rebuild <dir> -o <bundle.json[.gz]>` writes
  the text back byte for byte and checks it against the hashes the build recorded; `verify --roundtrip` does the
  same without writing (with `--full`, also against that bundle's text). Both refuse a selection, a build with
  deltas or `--tables`, a missing table or manifest, ordinals that are not `0..n-1`, a pointer section outside its column's
  targets, and any spec that is not lossless; a differing hash leaves no output. Measured 2026-10-30 on the
  2026-09-23 weekly delta (4,530,457 records, 2.68 GB of text): rebuilt text identical (sha256 `fb247dc2…`),
  build 41.2 s / 3.5 GB peak RSS (29.9 s / 2.8 GB before the round-trip check), rebuild 40.0 s / 2.0 GB.
- **`sections` 1.0 (draft) changed in place** (it is unreleased): every table gains `ordinal` (BIGINT, last: the
  record's position in its bundle section); each bare-id column with several targets gains a `<column>_section`
  column right after it (`variation.members_section`, `conditionSet.concepts_section`, `object_section` in the
  four proposition tables, `propositionSomaticClinicalImpact.conditionQualifier_section`,
  `evidenceLine.hasEvidenceItems_section`, `proposition_section` in `scv`, `vcv` and `rcv`); an absent list inside
  a typed value is now NULL (was `[]`), and an extension's unused list slots NULL (were `[]`), so `[]` is always
  an empty list. The spec states its round trip: top-level `lossless: true`, a `json` block per table, `targets`
  on every bare-id column, and typed-copy `pointers` as a mapping from position to target section.
- The `sections` build now writes each row back as JSON and stops unless it equals the bundle line, key text
  included: a JSON `null`, a float, an open Range `[null, null]`, a record without its `id`, a key order or escape
  the spec doesn't state now stop a `sections` build (none occurs in the excerpt or the delta). A lossless build
  also writes `clinvar_gkm_parquet.section_text_sha256` in every file and `source_text_sha256` and
  `source_text_size` in the manifest. `baseline` and `clinvar-gkm` output is unchanged (byte-identical on the real
  excerpt).
- Spec format additions: derivation kind `ordinal`; `targets` on bare-id and pointer-section columns; `pointers`
  may map each position to its target sections; a table's `json` block (`key_order: columns | sorted`,
  `record_id`, `sorted_columns`); a spec's `lossless` flag. Staging: the build's chunk lines start with the
  record's ordinal, and `bundle_<section>` gains `ordinal` and `key_json` (the key as written).
- Tests only: the provisional fixture `tests/fixtures/examples-1.1.0/` and `scripts/make-example-fixtures.py`
  are removed. The real 1.1.0 excerpt `tests/fixtures/real-1.1.0/` is the suite's only bundle fixture;
  `CLINVAR_GKM_FIXTURE=<folder>` still runs the suite on another excerpt (the `provisional` and `real` values
  are gone), and the skips and strict xfails for the provisional fixture's shapes are removed. No tool
  behaviour changes.
- **The compile-and-build path is generic: any bundle plus its schema and column set** (ADR 0006; unreleased). `build
  --bundle-schema <path|url|dir> --column-set <path|url> --full <bundle>` compiles a table spec for any producer's bundle,
  and `compile-spec` shows the result. A column set ([`docs/column-set-format.md`](docs/column-set-format.md)) is required
  and authoritative: it states the columns, and a schema's `$id` pairs with it. No ClinVar-GKM name or URL is in that path
  (a test enforces it). Proved on two real CIViC bundles, whose schema has a URN `$id`, with no version, pretty-printed,
  with empty collections and root values: rebuilt JSON equals the source, `metadata` included.
- **The default bundle is a data file**, `spec/sections/default-bundle.yaml`. With no flags, a build reads the bundle's
  `schema_version` from its first lines and takes that version's pairing: the shipped snapshot `schemas/1.1.0/` with
  `spec/sections/column-sets/1.1.0.columns.yaml`, else the published URLs, fetched once and cached. On 1.1.0 the compiled
  tables equal the previous file's (names, types, nulls, derivations) with no listed difference, except column and field
  order in the 13 key-sorted tables, which is now schema order; rows are equal per column by name, and `rebuild` gives
  the same text (2026-09-23 weekly delta: sha256 `fb247dc2…`). `spec/sections/1.0/tables.yaml` moved to
  `tests/fixtures/sections-golden/`, the golden the compiler must reproduce; a build doesn't load it.
- **Behaviour change: no fixed list of bundle versions for `sections`.** A version is accepted when its pairing loads and
  compiles, and refused with the reason otherwise. `baseline`, `clinvar-gkm` and a custom spec keep the versions they list;
  a custom spec's sections are checked against those versions' schema sets, so one listing an unsnapshotted version reads or
  fetches its schemas when it loads. A bundle's collections and their order come from its schema in the reader, replay, the
  selection engine and `rebuild`. A delta manifest's sections are checked when its delta is read, because a manifest states
  no version. Error text changes: an unknown version's message gives the reason.
- **Two readers, chosen per file by its first lines.** The line layout is streamed by the strict grammar, as before, and now
  also takes `"<collection>": {}` and root values on one line, and a file with no version line under a pairing. Any other
  JSON is read whole (in memory), every record's keys sorted, a duplicate key or a non-JSON number refused. A plain `.json`
  file works as well as gzip. `clinvar-gkm`, `baseline` and selections read only the line layout.
- **The round trip states its promise.** The manifest gains `round_trip` (`promise`: `byte-identical` | `json-equal`,
  `source_layout`) and `source_root` (the root's members, in order). `rebuild` and `verify --roundtrip` state which holds, and
  a file in another layout is written back in the canonical line layout.
- **Root values** (`metadata`, say) are kept in a `root_values` table (`name`, `value` as JSON text), which exists only when a
  bundle has any and which `rebuild` writes back. An empty collection is a zero-row table, written back as `{}`.
- **New value kinds in a compiled spec:** a float is a DOUBLE column or `value_number` slot, its text confirmed by the row
  check; `json` keeps a record property as JSON text (derivation kind `json-text`), for a map whose keys are data or a
  position that is a pointer in one record and an object in another; `prop as name` renames a column, which replaces the
  tool's built-in `loc_` rename. Two `allOf` declarations of one property now both hold: a profile narrows its base.
- **New key-value entries on a compiled build:** `table_spec_sha256` (of what affects the tables: not `description`,
  `transform`, `sources` or `status`), `schema_set_sha256` and `column_set_sha256`, also in the manifest's `table_spec`, and
  its compiled spec beside the manifest as `table-spec.json`. `verify` and `rebuild` read that file and refuse a hash that
  differs, naming the tool that wrote the set; they don't recompile or need the schemas. So a `sections` set built before
  this change must be rebuilt (it was an unreleased draft). `clinvar-gkm` and `baseline` output is byte-identical on the
  real excerpt.
- New command `compile-spec <schema_version>` or `compile-spec --bundle-schema ... --column-set ...`: writes the compiled spec
  as YAML, with the hashes in a comment. `scripts/vendor-schemas.py <schema> <directory>` writes a schema set a build can read
  with no network.
- **`version --json` changes:** `accepted_schema_versions` is removed, because it can no longer be a closed list.
  `schema_version_rule` states the rule and `snapshotted_schema_versions` lists what works offline. Each `table_specs`
  entry gains `compiled`, and the compiled entry has `tables_by_schema_version` instead of `tables`. The other keys keep
  their meaning. `spec.resolve_spec("sections")` needs the bundle's version; `build.resolve_spec_for` takes it from the
  inputs.
