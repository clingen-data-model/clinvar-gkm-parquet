# 0006 — The default spec is compiled from the bundle's schemas

- **Status:** Proposed
- **Date:** 2026-11-02
- **Plan (if any):** [`docs/plans/2026-10-20-design.md`](../plans/2026-10-20-design.md) §3, §9; amends the version
  gate of [ADR 0002](0002-independent-tool-versioning.md); builds on [ADR 0001](0001-parquet-derived-from-bundles.md),
  [ADR 0003](0003-pluggable-table-specs.md) and [ADR 0005](0005-lossless-sections-build.md)

## Context

The default `sections` spec (ADR 0003) is a checked-in file, `spec/sections/1.0/tables.yaml`, written by hand from
the ClinVar-GKM producer's procedures and checked against the schemas and real records. A new bundle
`schema_version` stops the build until someone researches a new spec and cuts a tool release. The maintainer wants
the conversion driven by the bundle schema and the schemas it references (clinvar-gkm, va-spec, cat-vrs, vrs,
gkm-core), so it keeps working as new schema versions are published.

A check on 2026-11-02 walked the 1.1.0 schemas with a prototype and compared the result with the spec:

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

These counts come from a prototype, so they are indicative. The gaps are real: the schemas allow more than the
producer writes and leave open what it does write. The maintainer decided on 2026-11-02 that ClinVar-GKM owns these
facts and adds them to its schemas at its next schema bump, that a property the producer never emits should not
become a column, and that the tool compiles at run time.

## Decision

1. **`sections` is compiled, at run time, from the schema set of the bundle's `schema_version`.** The tool reads
   the version from the bundle, loads the bundle schema and the closure of its `$ref`s, and generates the spec with
   fixed rules. The output of the compiler is an ordinary spec: the format, the loader's checks, the build engine,
   `rebuild` and `verify` are the same as for a spec file.
2. **Schemas are found by their published identifiers.** The bundle schema is at
   `https://w3id.org/clingen/schema/clinvar-gkm/<version>/clinvar-gkm-bundle.schema.json`; each `$ref` is an absolute
   `w3id.org` URL that resolves to a tagged file. A release ships a snapshot of the schema sets it was tested with,
   so those versions work offline; another version is fetched once and cached.
3. **The mapping rules live in the tool and hold for every version:**

   | Schema pattern | Rule |
   | --- | --- |
   | A property of the bundle schema other than `schema_version` | a section: one table, in schema order |
   | A property of the section's class | a column of the same name, in schema order |
   | The record's `id` | the key column |
   | Object, or array of objects | a nested STRUCT, or a list of them |
   | A union of objects | one merged STRUCT: the first branch's fields, then each further branch's new fields |
   | A pointer position | a bare id in the property's column, plus `<column>_section` if it has several targets |
   | Integer or `Range` at the top level | `<prop>`, `<prop>_min`, `<prop>_max` |
   | An `extensions` list | `{name, value_<json type>}` slots, one per JSON type its values can take |
   | Any other pattern, a nested integer-or-Range, or a class that reaches itself | stops the compile, naming the position |

   One naming exception stays, because it is a choice of this tool and not a schema fact: `location.start` and
   `location.end` are `loc_start` and `loc_end` (`end` is an SQL keyword).
4. **The schemas must state what the rules read (the schema contract).** From the next schema bump, ClinVar-GKM's
   profiles:
   - allow only the properties the producer emits, so the schema is the column set;
   - define every extension value fully, with no open object;
   - write each pointer as a string whose `pattern` names its target sections (`^#/(gene)/`, `^#/(a|b)/`), and drop
     `iriReference` from positions that are always inline;
   - narrow an integer-or-Range to the kind the producer writes when it never writes the other;
   - leave no class that reaches itself;
   - and the producer writes every key sorted, so the tool needs one key-order rule.
5. **1.1.0 needs an overlay, because its schemas and bundles are published.** `spec/sections/overlays/1.1.0.yaml`
   states only what the 1.1.0 schemas lack, in those same terms. The compiler refuses an overlay entry that
   contradicts the schema or matches nothing, so the file can't drift or hide a mistake. A version whose schemas meet
   the contract has no overlay.
6. **The version gate moves.** A bundle's version is accepted for `sections` when its schema set loads and compiles;
   anything else is refused and names the reason. This replaces, for `sections`, the fixed list of ADR 0002. The
   sections a bundle may hold come from its schema, not from a list in the tool. `clinvar-gkm` stays hand-maintained
   and keeps its fixed list, because it has to match the BigQuery export (the oracle).
7. **Each build records what it compiled from.** Every file and the manifest carry the sha256 of the schema set and of
   the compiled spec, so a build can be reproduced and checked, and `verify` and `rebuild` recompile and compare.
8. **The build's row-by-row round trip (ADR 0005) stays the safety net.** A record the compiled spec can't reproduce
   stops the build, whatever the schema said.

## Consequences

- A schema bump that meets the contract needs no tool release and no spec research. A bump that adds a new kind of
  pattern stops at compile time, with the position named, and needs a tool change. That is the limit of "schema-driven".
- The tool still holds the mapping rules, the `loc_` exception, the 1.1.0 overlay and the bundle's outer line layout,
  which the assembler defines and the schema doesn't describe.
- The column set of a release is now what ClinVar-GKM's schema says, so a schema change can change the columns without
  a tool change. The producer announces those changes (the maintainer's decision); a table-spec version alone no
  longer names a column set. The identity of a `sections` table set is the spec's rules version plus the schema set's
  sha256.
- The contract is a change ClinVar-GKM has to make. Until it does, a new schema version fails to compile, loudly,
  rather than producing a wrong table. This repo doesn't edit ClinVar-GKM; the request goes through a paired note.
- The bundle schema's own `$id` (`.../clinvar-gkm/1.1.0/json/ClinvarGkmBundle`) returned 404 on 2026-11-02, while the
  file is served at `.../clinvar-gkm/1.1.0/clinvar-gkm-bundle.schema.json`. The tool uses the second URL; the paired
  note asks ClinVar-GKM to align them.
- A first run on an unsnapshotted version needs the network; after that it reads the cache.
- `spec/sections/1.0/tables.yaml` stops being the source. It remains as the test that the compiler reproduces it on
  1.1.0, apart from listed differences.
