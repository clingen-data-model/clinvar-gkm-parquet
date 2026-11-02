# Column-set format

A **column set** is a YAML file a bundle producer publishes with a bundle schema. It states the Parquet tables built
from bundles of that schema. A bundle schema says what a bundle *may* hold; the column set says what is *built*: which
properties become columns, and what the schema leaves open. The tool compiles the two into a table spec
([ADR 0006](decisions/0006-sections-spec-compiled-from-schemas.md)). Unreleased.

```sh
clinvar-gkm-parquet build --bundle-schema my.schema.json --column-set my.columns.yaml --full my-bundle.json --out out/
clinvar-gkm-parquet compile-spec --bundle-schema my.schema.json --column-set my.columns.yaml   # read the result
```

## A small example

```yaml
schema: urn:example:gkm-bundle:1.0                # the bundle schema's $id
sections:
  gene: {id, type, name, mappings: {coding: {code, system}, relation}}
  variant: {id, type, name, gene: ->gene, start, end as stop_pos, extensions: [string, boolean]}
```

## The file

| Key | Required | Meaning |
| --- | --- | --- |
| `schema` | yes | The `$id` of the bundle schema it pairs with. Any other schema is refused. |
| `sections` | yes | One entry per collection of the bundle: its properties, as shapes (below). |
| `version_property` | no | The root property that holds the bundle's version, if it has one. |
| `collections` | no | Root properties that are collections, only where the schema can't say. |
| `key_order` | no | Tables whose line-layout bundle text isn't key-sorted. Default: every key sorted. |

**What is a collection.** A root property of the schema is a collection when it has `patternProperties`, or
`additionalProperties` holding a schema. It is one table, named as the property. Any other root property is a root value
(such as `metadata`): it needs no entry, and the tool keeps it as JSON text in the table `root_values`, name and value,
which exists only when a bundle has any. A root member the schema doesn't name is kept the same way, unless the schema's
root has `additionalProperties: false`; then it stops the build. The `version_property` is neither. Name a property under `collections` only if
the schema is silent about it, such as a bare `{"type": "object"}`.

## Shapes

An entry in `sections` is a mapping from property to shape. A table holds `id` (the bundle key), one column per listed
property, and `ordinal` (the record's position). List `id` only if the record carries its own, which must equal its key.

A shape says what is at one position. Where the schema declares an array, it says what each element is.

| Shape | Meaning | Use it |
| --- | --- | --- |
| *(nothing)*, as in `{id, type}` | What the schema says | The schema is definite: one scalar type, or a closed object all of whose properties you want |
| `{a, b: <shape>}` | An inline object; only these properties | Almost every object: this narrows a general class to what you emit |
| `->gene`, `->a\|b` | A pointer (`#/gene/<id>`) into those collections | A string that is always a bundle-local pointer. The id is what follows the section, so it may hold more path |
| `string`, `integer`, `number`, `boolean` | That scalar | The schema allows several types, or leaves the position open; `integer` also narrows an integer-or-Range |
| `[<shape>]` | A list | The schema leaves the position open (where it says array, give the element's shape directly) |
| `extensions: [<shape>, ...]` | An `extensions` list: one shape per kind of value | Any property named `extensions`. List every kind: `[string, boolean, [string], {a: string}]` |
| `json` | The value as compact JSON text | A record property with no typed form (below). Not inside a value |

Inside a position the schema leaves open, every property needs a shape: `{code: string, count: integer}`.

**Renaming.** `start as loc_start` names the column differently from the property, for a property of the record (useful
where the name is awkward in SQL, although the tool quotes every name).

**An integer-or-Range** (a two-element array of integers or nulls) with no shape, as a record property, becomes three
columns: `<prop>`, `<prop>_min` and `<prop>_max`. Give it `integer` if it is never a Range.

**When to use `json`.** Two patterns have no typed column: a map whose keys are data (`additionalProperties` holding a
schema), and a position that is a pointer in one record and an inline object in another. Keep those as `json`. The column
holds the value's JSON text, so a query parses it, and pointers inside it stay as written.

**A pointer with several targets** gets a `<column>_section` column beside it, holding the section each pointer names.
Only a record property has that column, so inside a value a pointer names one target; a value with a many-target pointer
is kept as `json`.

## The rules the compile enforces

The compile stops, naming the section and position, on:

- a `schema` other than the schema's `$id`;
- a collection with no entry, or an entry for something that is not a collection;
- a property the schema doesn't have, unless the object is open;
- a shape contradicting the schema: `integer` on a string, an object on a scalar, a pointer where no string is allowed;
- a position it can't resolve with no shape: an open object, a union of scalars, a string-or-object, an integer-or-Range
  inside a value, a map;
- a class that reaches itself, when you list it all the way down: narrow around it;
- a pointer naming something that is not a collection; a `sorted_columns` entry that is not a column; a key written twice.

A property you list that a bundle doesn't hold is fine: the column is NULL. A property a bundle holds that you don't list
stops the **build**, naming the record: nothing is dropped. So a column set is only as complete as the records it has
been run on; an empty collection's columns are your statement of what it would hold.

## Key order, for line-layout bundles only

A bundle in the *line layout* (one record per line, `    "<key>": <record>`) is rebuilt byte for byte, so the tool has to
know its key order. By default every key at every depth is sorted. If yours isn't, list those collections, and the
column-set order is then the order in the text:

```yaml
key_order:
  variant:                                 # columns and fields in the order of this file
  gene: {sorted_columns: [extensions]}     # as above, but these columns' values are key-sorted
```

Any other layout (pretty-printed, minified) ignores this. The file bytes can't be kept, so the tool reads records with
keys sorted, and `rebuild` writes the canonical line layout: JSON-equal to the source, not byte-identical. The manifest
records which promise a set carries (`round_trip`).

## What a build checks

Every row is written back as JSON from its columns and compared with the record; any difference stops the build. This is
what makes a wrong shape loud rather than a wrong table. A JSON `null` stops it too, since a NULL column reads as absent.

The general reader holds a whole bundle in memory. For a bundle too large for that, write it in the line layout, which is
streamed.

## The schema

The bundle schema is a file, a URL, or a directory holding a schema set (`scripts/vendor-schemas.py` writes one, so a
build needs no network). Its `$id` can have any form, such as a URN. `$ref`s followed from it must be
`https://w3id.org/` URLs, or JSON pointers within a file.
