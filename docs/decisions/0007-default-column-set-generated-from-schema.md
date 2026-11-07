# 0007 — The default column set is generated from the bundle schema

- **Status:** Proposed
- **Date:** 2026-11-05
- **Plan (if any):** none yet; amends [ADR 0006](0006-sections-spec-compiled-from-schemas.md) (decisions 1, 2 and 6) and,
  for the default, the round-trip promise of [ADR 0005](0005-lossless-sections-build.md); builds on
  [ADR 0001](0001-parquet-derived-from-bundles.md) and [ADR 0002](0002-independent-tool-versioning.md)

## Context

ADR 0006 compiles a table spec from a bundle schema and a column set, and requires the column set: a producer writes it
by hand, from what its records hold, and publishes it with each schema tag. That has three costs.

- **Every producer repeats the same research.** A column set records what the producer emits, so writing one means
  reading the producer's code. ClinVar-GKM's 1.1.0 set was researched from its procedures at one commit, and it ships in
  this tool because the 1.1.0 tag was published before ClinVar-GKM published column sets.
- **It can drift from the producer.** A procedure change can make the column set wrong with no change to the schema.
- **A property the schema allows but the set doesn't list stops the build.** A column set is only as complete as the
  records it has been run on (ADR 0006, Consequences).

The maintainer set the direction on 2026-11-04: each producer defines its own bundle schema and default bundle; the
default Parquet transformation follows one policy for every producer (every section a table, every property a column,
nested structures kept, arrays kept, fixed rules for extension values, Ranges and polymorphic content); the tool generates
the default column set from the bundle schema alone, can check a supplied column set against it, and can still build
column sets that differ from it. On 2026-11-05 the maintainer decided three details: nested objects stay nested STRUCTs
(no flattening into separate columns); the default promises a JSON-equal round trip, with key order an optional add-on;
and a column set complies only if it compiles to exactly the tables the generated default compiles to.

A schema says what a bundle *may* hold, not what it does hold, so a schema-only default needs a rule for each position
the schema leaves open. A prototype measured them on 2026-11-05 (scratch scripts, not committed): it walked every
section's record schema with the compiler's own reader, listed every allowed property at every depth, and wrote JSON
text wherever the schema can't settle a position. It ran on the 1.1.0 schema snapshot (`schemas/1.1.0/`) and on CIViC's
(`tests/fixtures/civic/schema-set/`), and built the real excerpt (`tests/fixtures/real-1.1.0/`), the ClinVar sub-bundle
and both CIViC bundles.

| | 1.1.0 default | 1.1.0 hand-written | CIViC default | CIViC hand-written |
| --- | ---: | ---: | ---: | ---: |
| Top-level columns (with key and `ordinal`) | 253 | 169 | 193 | 126 |
| Leaf fields (through STRUCTs and lists) | 981 | 425 | 981 | 198 |
| Of those, JSON text | 189 | 0 | 166 | 5 |

The widest 1.1.0 table is `variation`: 370 leaves, five levels deep (hand-written: 42). Where the schema can't settle a
position:

| Kind | 1.1.0 positions | CIViC | Example |
| --- | ---: | ---: | --- |
| String or object (an `iriReference` or an inline class) | 67 | 31 | `scv.classification`, `allele.location`, every `#/` pointer |
| An open extension value | 58 | 63 | `gene.primaryCoding.extensions[].value` |
| A class that reaches itself (always `Extension.extensions`) | 59 | 63 | `allele.state.extensions[].extensions` |
| A nested integer-or-Range | 5 | 4 | `variation.constraints[].copies` |
| A map whose keys are data | 0 | 4 | CIViC `variant.protein` |
| A keyword the rules can't read (`if`) | 0 | 1 | CIViC `assertion.hasEvidenceLines[]` |

The string-or-object kind is the largest and the most important. It covers every pointer the hand-written 1.1.0 set
types (27 positions) and the positions queried most (`classification`, `strength`, `quality`, the qualifiers). Typing
its object branch instead of keeping it as text would follow each class into the next: about 7,700 leaves for 1.1.0,
eight levels deep, about 1,140 each in `scv`, `vcv` and `rcv` (an estimate; that column set wasn't compiled).

With JSON text at those positions the default builds on every fixture and rebuilds a JSON-equal bundle, after two
fixes: records with no `id` of their own (below), and the key order of line-layout input. The hand-written sets, as a
control, still pass, and rebuild the line-layout excerpt byte for byte.

Most of the default's width is what the GA4GH classes allow and a producer never writes. A producer can say so in its
own profiles, and a second test on 2026-11-05 (a two-class schema, scratch only) showed that the GKM tooling carries it
through:

- `gkm-metaschema`'s `source2splitjs` (0.4.4.dev1) accepts a profile that adds `<property>: false` to an `allOf` beside
  its base class, and one that restates a string-or-object property as only the object. It warns that the second drops
  `iriReference`, and asks that a deliberate drop be ignored.
- The JSON Schema validator in `ga4gh.gkm` 0.4.0 enforces both. A record holding a `false` property fails, at the top level
  and nested, and so does an IRI where the profile now requires the object.
- The compiler follows the restated property, so the position becomes a typed STRUCT. It ignores `false`, so a removed
  property is still a column today.
- A profile can also restate a string-or-object position as a pointer to named sections: `$ref` to `iriReference` with
  `pattern: "^#/location/"`, or `"^#/(allele|copyNumberCount)/"` for several, on a property or on a list's items. The
  metaschema passes it through, and the validator rejects a pointer to another section and an inline object. The
  compiler doesn't read `pattern` today.

The bundle writes every pointer as `#/<section>/<key>` (in the 1.1.0 excerpt, 34 section-and-target pairs, from
`#/location/` in `allele` to `#/propositionSomaticClinicalImpact/` in `evidenceLine`), and every key is already a bare id
(`clinvar.submission:SCV002077637.1`, `ga4gh:VA.SvLXsMO1…`). The hand-written 1.1.0 set types 27 positions as pointers,
so today's tables join by bare id, with a `<prop>_section` column where a pointer has several targets (ADR 0005,
decision 3).

## Decision

1. **The default transformation is a versioned policy.** The rules below are the default policy, version `1.0`. A change
   to them is a new policy version, bumped by the table in `.claude/rules/table-spec.md` (what an existing schema compiles
   to is what the table judges).
   - One table per bundle collection, in schema order: the key column, then the columns, then `ordinal`.
   - Every property the record's schema allows, through its whole `$ref`, `allOf`, `oneOf` and `anyOf` closure, is a
     column at the top level and a field at every depth below it. An object is a nested STRUCT and an array is a list.
     Nothing is flattened into separate columns.
   - A property that any `allOf` member declares `false` is not allowed, so it is not a column or a field. This is how a
     producer's profile removes what a base class allows and the producer never writes.
   - Names are the schema's property names. The tool quotes every name, so a reserved word such as `end` keeps its name.
   - A union of objects is one merged STRUCT, as the compiler does today.
   - **`id`.** The key column holds the bundle key. Where the record's schema allows `id` without requiring it, an
     `id_present` BOOLEAN column says whether the record carried its own; where it requires it, the record's `id` must
     equal its key; where it declares it `false`, there is neither. In ClinVar-GKM's 1.1.0 records, `therapy`,
     `therapyGroup` and `sequenceReference` carry no `id`.
   - **Extensions.** An `extensions` list is a list of STRUCTs with the element's fields (`name`, `value`, and the
     `id` and `description` that gkm-core's `Extension` also allows). Its value is one slot per JSON type: `value_string`,
     `value_boolean`, `value_integer` and `value_number` for scalars (a schema `number` gets both of the last two), and
     `value_json` for an object or an array. An element's own `extensions` is JSON text, because `Extension` reaches
     itself.
   - **Ranges.** An integer-or-Range is `<prop>`, `<prop>_min` and `<prop>_max` at any depth: three fields in a STRUCT,
     three columns at the top level.
   - **A position the schema can't settle is JSON text**, so generating the default never stops: a map whose keys are
     data, a keyword the rules can't read, and a class that reaches itself, at the position where it re-enters. JSON text
     is a DuckDB `JSON` value, a list of them where the schema says array.
   - **String or object is split** (maintainer, 2026-11-05). A position that may be a string (an `iriReference`, so a
     pointer or an IRI) or an object is `<prop>`, VARCHAR, holding the string as written, and `<prop>_json`, holding an
     inline object as JSON text. In a list, each element is a STRUCT of the two. A producer who always writes one form
     narrows the position to it, and then it is one typed column.
   - **Pointers name their sections by pattern** (maintainer, 2026-11-06). A string position whose schema has a
     `pattern` of the form `^#/<section>/` or `^#/(<section>|<section>…)/`, each a collection of the bundle, is a pointer
     into those sections: the column holds the bare id (the text after `#/<section>/`), plus `<prop>_section` holding the
     section when there are several, as for a column set's `->a|b`. Any other pattern, or a section that isn't a
     collection, stops the compile. With no such pattern, a string stays as written.
2. **The tool generates the default.** A new command writes the default column set of a bundle schema (a file, a URL or a
   schema-set directory). The same schema set and policy version always give the same bytes. A build given a schema and
   no column set uses the generated default. A producer publishes its bundle schema and default-bundle description; it
   publishes a column set only for a variant.
3. **The tool checks a column set against the default.** A check compiles the supplied column set and the generated
   default, and compares their tables. It reports compliant, or lists each difference with its position and both
   sides. `key_order` is left out of the comparison.
4. **A variant is any other column set the compiler accepts**, and it can still be built. Every built set records the
   policy version and whether its column set is the default, compliant, or a variant, in its manifest and in every file's
   key-value metadata.
5. **The default promises a JSON-equal round trip.** A schema can't state key order, so the generated default rebuilds a
   bundle that is JSON-equal to its source. For line-layout input, the build's row-by-row check therefore compares
   key-sorted JSON when a table has no `key_order`; today it compares bytes, and stops on the 7 ClinVar-GKM sections not
   written in sorted order. A column set may add `key_order` to the default; that leaves it compliant and restores ADR
   0005's byte-identical round trip for a line-layout bundle. ClinVar-GKM keeps its byte-identical check that way.

6. **ClinVar-GKM publishes the default, from narrowed profiles** (maintainer, 2026-11-05). It narrows its own
   `schema/clinvar-gkm/*-source.yaml` profiles to what its pipeline writes, rather than publishing a variant column set:
   unused properties `false`, string-or-object positions narrowed to the form it writes (pointers with their section
   pattern), open extension values closed, and integer-or-Range positions narrowed to `integer` where they are never a
   Range. That is ClinVar-GKM's work, in its repo, as a new schema version; this tool needs the `false` and pointer
   rules above.

   On the 1.1.0 schema the default has 67 string-or-object positions. The hand-written set, which covered every record
   of the 10-31 full, shows what narrowing makes of them: 27 are always a pointer (one bare-id column each, with
   `<prop>_section` where several sections are named), 21 are always an inline object (a typed STRUCT), and 19 are never
   written (`false`, no column). So ClinVar-GKM's narrowed default has no `<prop>_json` column; a later record in the
   other form fails strict validation before any Parquet is built.

The other ways to store string-or-object, measured on 1.1.0, were JSON text (smallest, but every pointer becomes a
quoted string inside JSON) and typing the object branch (about 7,700 leaves, an estimate, with every cycle in the GKM
classes cut).

## Consequences

- A property the schema allows never stops the build: the default already has its column, NULL until a record holds it.
- A producer needs no column set for the default, so the default can't drift from the producer's code. A record holding
  something its schema doesn't allow still stops the build, naming the record.
- The default is wide: 253 columns and 981 leaves for 1.1.0, against 169 and 425 hand-written, most of them NULL in every
  row. Parquet stores an all-NULL column cheaply, but a `DESCRIBE` is long.
- Values the schema leaves open are JSON text in the default, so a query parses them. On the 1.1.0 schema these include
  ClinVar-GKM's submitted conditions (`scv.extensions` and `evidenceLine.extensions`) and the inline half of
  `classification` and the qualifiers. A producer who wants them typed narrows its schema.
- ClinVar-GKM's hand-written 1.1.0 column set becomes a variant. Against the default it omits 161 allowed positions (96 of
  them record properties), renames `start` and `end`, narrows `copies` and `length` to `integer`, types 21
  string-or-object properties and two extension values, and types 27 pointers. That list is also the narrowing work: the
  hand-written set covered every record of the 10-31 full (91.85 M rows, a lossless build on 2026-11-04), so it is a
  checked inventory of what the pipeline writes.
- The `sections` tables change again when the default is generated, and again when ClinVar-GKM's narrowed schema version
  ships. ClinVar-GKM should not publish `sections` Parquet until both have landed.
- With narrowed profiles, ClinVar-GKM's strict validation of every delta (its `release-gkm-delta.sh`) also stops a
  release whose pipeline writes something the profiles don't allow. The pipeline and the profiles have to change
  together.
- A producer that doesn't state pointer patterns gets its pointers as written (`#/allele/…`), so a join strips the
  prefix. ClinVar-GKM states them, so its tables keep bare ids and `<prop>_section` columns.
- Compiler changes: `false` properties; pointer sections from `pattern`; JSON text inside a value (today only a record
  property can be `json`); the string-or-object split; the extension element's extra fields and the `value_json` slot;
  Range fields at any depth; the `id_present` column; the key-sorted row check for line-layout input with no
  `key_order`. The extension change also adds
  NULL fields to the extension STRUCTs of existing column sets, so it is a new rules version for every pairing, not only
  the default.
- ADR 0006's other limits are unchanged: deltas, selective builds, `sources.py`'s release logic and `verify --against`
  read only ClinVar-GKM's line layout.
