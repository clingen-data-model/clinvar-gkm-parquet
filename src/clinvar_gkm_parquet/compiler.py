"""Compile a table spec from a bundle schema set and its column set, for any producer's bundles (ADR 0006).

`compile_sections(schema_set, column_set)` returns a spec mapping, the format of a tables.yaml (spec.py). The
schema says what a position may hold; the column set says what is built from it, and it is required. The column
set's format is in docs/column-set-format.md. No producer, section, class or property is named here, except the
structural names the rules read (`extensions`, `id`, `name`, `value`).

- A root property of the schema is a collection when the schema gives it one record class (`patternProperties`, or
  `additionalProperties` holding a schema), or when the column set says so; the version property is neither;
  anything else is a root value, which the tool keeps as JSON text.
- A collection is one table, in schema order: the key column (the bundle key), one column per property the column
  set lists (`id` listed means the record has its own, equal to the key), and `ordinal`, last.
- An object or array of objects is a nested STRUCT or a list of them. A union of objects is one merged STRUCT: the
  first branch's fields, then each further branch's new fields.
- `->a|b` is a pointer: a bare id in the column, plus `<column>_section` if it has several targets.
- An integer or Range at a record's top level is `<prop>`, `<prop>_min`, `<prop>_max`; a Range is known by its
  shape, a two-element array of integers or nulls.
- An `extensions` list is {name, value_<json type>} slots, one per JSON type its values can take.
- `json` keeps a record property as compact JSON text, for what has no typed form (data-dependent keys, a position
  that is a string in one record and an object in another).
- Anything else stops the compile, naming the section and position and what the column set must state: an open
  object it doesn't list, a scalar union, a position that may be a string or an object, a nested integer-or-Range,
  a pointer with several targets inside a value, a class that reaches itself.

How a schema position is read: `$ref` is followed. `allOf` members all hold, so a property two of them declare
holds both declarations (a profile narrows its base). `oneOf` and `anyOf` branches are a union. A member or
declaration with no shape keyword (`required`, `contains`, `minItems`) says nothing about the columns. A keyword
the rules can't read (`if`, `patternProperties`, ...) makes the position opaque, so only the column set can state
it.

Order: a key-sorted table (`key_order` absent, the default) has its columns and fields in schema order, then the
open positions in column-set order. A table in `key_order` takes every order from the column set, because a
line-layout bundle's text depends on it.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field, replace
from functools import reduce

import duckdb

from .bundle import ROOT_VALUES_TABLE
from .errors import CompileError, SchemaSetError
from .schemas import SchemaSet, split_ref

SPEC_NAME = "sections"
RULES_VERSION = "1.0"  # the version of these rules
EXTENSIONS, ID = "extensions", "id"
WRITER = {"compression": "snappy", "row_group_size": 122880}
JSON_TEXT = "json"  # the column set's shape for a property kept as JSON text

_DUCK = {"string": "VARCHAR", "integer": "BIGINT", "boolean": "BOOLEAN", "number": "DOUBLE"}
_SLOT_ORDER = (
    "string",
    "boolean",
    "integer",
    "number",
    "object",
    "string_array",
    "boolean_array",
    "integer_array",
    "number_array",
    "object_array",
)
_POINTER_SHAPE = re.compile(r"->(\w+(?:\|\w+)*)")
_RENAME = re.compile(r"(\w+) as (\w+)")
# Keywords that say what a value is. Anything else a node holds is validation or annotation, except _OPAQUE.
_SHAPE_KEYS = frozenset({"$ref", "type", "allOf", "oneOf", "anyOf", "properties", "items", "const", "enum"})
_OPAQUE = (
    "if",
    "then",
    "else",
    "patternProperties",
    "dependentSchemas",
    "propertyNames",
    "prefixItems",
    "$dynamicRef",
)
_STRIP = "strip ^#/[^/]+/"
_SLOT_TRANSFORM = "{name, value} -> {name, value_<kind>}: value goes to the slot of its JSON kind, at every depth"
_NESTED_SLOT_TRANSFORM = "nested extensions: {name, value} -> {name, value_<kind>} by the JSON kind of value"


@dataclass(frozen=True)
class _Both:
    """A position two `allOf` members declare: both hold, the later narrowing the earlier."""

    earlier: tuple
    later: tuple


_Decl = tuple[dict, str] | _Both  # a schema node and the $id of the file it sits in; or two declarations in one


@dataclass
class _Alts:
    """What a schema position allows: at most one merged object, one array, and any scalars."""

    scalars: list[str] = field(default_factory=list)  # JSON types
    items: list[_Decl] | None = None  # an array's element declarations; []: no `items`, so open
    fields: dict[str, list[_Decl]] | None = None  # an object's properties, each with its declarations
    closed: bool = False  # the object allows no other property
    strict: bool = False  # closed by its own `additionalProperties: false`, which sees only its own properties
    map: bool = False  # an object whose keys are data: `additionalProperties` holds a schema
    range: bool = False  # a two-element array of integers or nulls: a VRS Range, by its shape
    opaque: str | None = None  # a keyword the rules can't read
    refs: set[str] = field(default_factory=set)  # the object classes this position went through

    def says_something(self) -> bool:
        return bool(self.scalars or self.range) or self.items is not None or self.fields is not None

    def split(self) -> list[_Alts]:
        """One _Alts per alternative: each scalar, the array, the object."""
        out = [_Alts(scalars=[t], refs=self.refs) for t in self.scalars]
        if self.items is not None:
            out.append(_Alts(items=self.items, refs=self.refs))
        if self.fields is not None:
            out.append(_Alts(fields=self.fields, closed=self.closed, map=self.map, refs=self.refs))
        return out


def _has_shape(node: dict) -> bool:
    return any(k in node for k in _SHAPE_KEYS) or any(k in node for k in _OPAQUE)


def _says(decl: _Decl) -> bool:
    return isinstance(decl, _Both) or _has_shape(decl[0])


def _both(earlier: list[_Decl], later: list[_Decl]) -> list[_Decl]:
    """A position declared twice in an `allOf`. A declaration with no shape keyword (`contains`) says nothing."""
    later = [d for d in later if _says(d)]
    if not later:
        return earlier
    return [_Both(tuple(earlier), tuple(later))] if earlier else later


def _refine(a: _Alts, b: _Alts) -> _Alts:
    """Both hold (`allOf`): what both allow, and a property both declare holds both declarations. Two members that
    each close themselves with `additionalProperties: false` allow only what both name."""
    refs, opaque = a.refs | b.refs, a.opaque or b.opaque
    if not a.says_something():
        return replace(b, refs=refs, opaque=opaque)
    if not b.says_something():
        return replace(a, refs=refs, opaque=opaque)
    out = _Alts(opaque=opaque, refs=refs, range=a.range and b.range, map=a.map or b.map)
    out.scalars = [t for t in a.scalars if t in b.scalars]
    if a.items is not None and b.items is not None:
        out.items = _both(a.items, b.items) if a.items else b.items
    if a.fields is not None and b.fields is not None:
        out.fields = dict(a.fields)
        for name, decls in b.fields.items():
            out.fields[name] = _both(out.fields.get(name, []), decls)
        if a.strict and b.strict and a.fields and b.fields:
            out.fields = {k: v for k, v in out.fields.items() if k in a.fields and k in b.fields}
        out.closed = a.closed or b.closed
        out.strict = a.strict or b.strict
    return out


def _union(a: _Alts, b: _Alts) -> _Alts:
    """Either holds (`oneOf`, `anyOf`): the union rule. The first branch's fields, then each further one's new."""
    out = _Alts(opaque=a.opaque or b.opaque, refs=a.refs | b.refs, range=a.range or b.range, map=a.map or b.map)
    out.scalars = a.scalars + [t for t in b.scalars if t not in a.scalars]
    if a.items is not None or b.items is not None:
        # An open array in any branch leaves the union open.
        open_items = a.items == [] or b.items == []
        out.items = [] if open_items else (a.items or []) + (b.items or [])
    if a.fields is not None or b.fields is not None:
        out.fields = {k: list(v) for k, v in (a.fields or {}).items()}
        for name, decls in (b.fields or {}).items():
            out.fields.setdefault(name, []).extend(decls)
        out.closed = (a.closed or a.fields is None) and (b.closed or b.fields is None)
    return out


@dataclass
class _Value:
    """A compiled position: its DuckDB type, and what the column and an extension slot need to know of it."""

    type: str
    slot: str  # the JSON type's name in a slot: string, boolean, integer, number, object, <k>_array
    pointers: dict[str, list[str]] = field(default_factory=dict)  # position inside the value -> target sections
    extension: bool = False  # holds an extension struct
    range: bool = False  # an integer-or-Range: three columns
    json_text: bool = False  # kept as JSON text


@dataclass(frozen=True)
class RootParts:
    """What the root of a bundle may hold, by its schema."""

    collections: tuple[str, ...]  # in schema order
    other_values: bool  # the root may hold values beyond the collections and the version property


class _Compiler:
    def __init__(self, schema_set: SchemaSet, column_set: dict):
        self.schema_set = schema_set
        self.column_set = column_set
        self.version_property = column_set.get("version_property")
        self.parts = root_parts(schema_set, self.version_property, column_set.get("collections") or ())
        self.section_names = list(self.parts.collections)
        self.section = ""
        self.ordered = False  # the table is in `key_order`
        self.path: frozenset[str] = frozenset()  # the classes open on the way to the current position

    # --- errors --------------------------------------------------------------------------------------------------

    def stop(self, pos: str, message: str, must: str | None = None) -> CompileError:
        where = f"schema {self.schema_set.bundle_id}: section {self.section}: position {pos}"
        return CompileError(f"{where}: {message}" + (f"; the column set must {must}" if must else ""))

    def bad_entry(self, pos: str, message: str) -> CompileError:
        return CompileError(
            f"column set for {self.schema_set.bundle_id}: section {self.section}: position {pos}: {message}"
        )

    # --- reading the schema --------------------------------------------------------------------------------------

    def read(self, decls: list[_Decl], pos: str) -> _Alts:
        """What the declarations of one position allow, as a union."""
        alts = [self._decl(d, pos) for d in decls if _says(d)]
        return reduce(_union, alts) if alts else _Alts()

    def _decl(self, decl: _Decl, pos: str) -> _Alts:
        if isinstance(decl, _Both):
            return _refine(self.read(list(decl.earlier), pos), self.read(list(decl.later), pos))
        return self._node(decl[0], decl[1], (), pos)

    def _node(self, node: dict, base: str, stack: tuple[str, ...], pos: str) -> _Alts:
        out = _Alts()
        ref = node.get("$ref")
        if ref is not None:
            doc_id, pointer = split_ref(ref, base)
            key = f"{doc_id}#{pointer}"
            if key in stack:
                raise self.stop(
                    pos, f"class {key} reaches itself", "state this position as `json` or narrow around it"
                )
            try:
                target, file_id = self.schema_set.resolve(ref, base)
            except SchemaSetError as e:
                raise self.stop(pos, f"unknown $ref: {e}") from e
            out = self._node(target, file_id, (*stack, key), pos)
            if out.fields is not None:  # only an object can reach itself through a property
                out.refs = out.refs | {key}
        for member in node.get("allOf", ()):
            if _has_shape(member):
                out = _refine(out, self._node(member, base, stack, pos))
        out = _refine(out, self._own(node, base, stack, pos))
        for keyword in ("oneOf", "anyOf"):
            branches = [self._node(b, base, stack, pos) for b in node.get(keyword, ()) if _has_shape(b)]
            if branches:
                out = _refine(out, reduce(_union, branches))
        return out

    def _own(self, node: dict, base: str, stack: tuple[str, ...], pos: str) -> _Alts:
        """What the node itself states, without following $ref, allOf, oneOf or anyOf."""
        out = _Alts(opaque=next((k for k in _OPAQUE if k in node), None))
        extra = node.get("additionalProperties")
        closes = extra is False or node.get("unevaluatedProperties") is False
        types = node.get("type")
        if types is None and "const" in node:
            types = _json_type(node["const"])
        if types is None and node.get("enum"):
            types = sorted({_json_type(v) for v in node["enum"]})
        if types is None and ("properties" in node or closes or isinstance(extra, dict)):
            types = "object"
        if types is None and "items" in node:
            types = "array"
        for t in [types] if isinstance(types, str) else (types or ()):
            if t == "object":
                out.fields = {
                    name: [(decl, base)]
                    for name, decl in node.get("properties", {}).items()
                    if isinstance(decl, dict)
                }
                out.closed = closes
                out.strict = extra is False
                out.map = isinstance(extra, dict)
            elif t == "array":
                self._own_array(out, node, base, stack, pos)
            elif t != "null":
                out.scalars.append(t)
        return out

    def _own_array(self, out: _Alts, node: dict, base: str, stack: tuple[str, ...], pos: str) -> None:
        items = node.get("items")
        if not isinstance(items, dict):
            out.items = []
            return
        if node.get("minItems") == 2 and node.get("maxItems") == 2:
            inner = self._node(items, base, stack, pos)
            if inner.scalars == ["integer"] and not (inner.fields or inner.items):
                out.range = True
                return
        out.items = [(items, base)]

    # --- a position, as the schema states it ---------------------------------------------------------------------

    def from_schema(self, a: _Alts, pos: str, top: bool) -> _Value:
        if a.map:
            raise self.stop(
                pos, "is a map whose keys are data, which has no typed form", f"state it as `{JSON_TEXT}`"
            )
        if a.opaque:
            raise self.stop(pos, f"uses {a.opaque}, which the rules can't read", "state its shape")
        if not a.says_something():
            raise self.stop(pos, "the schema says nothing about its value", "state its shape")
        if a.range and a.scalars == ["integer"] and a.items is None and a.fields is None:
            if top:
                return _Value("BIGINT", "integer", range=True)
            raise self.stop(
                pos,
                "an integer-or-Range inside a value has no column form",
                "narrow it to `integer`, or keep the record property as `json`",
            )
        kinds = len(a.scalars) + (a.items is not None) + (a.fields is not None) + a.range
        if kinds > 1:
            if a.fields is not None and "string" in a.scalars:
                raise self.stop(
                    pos,
                    "may be a string or an inline object",
                    "state which: `->section` for a pointer, a property list for an inline object, `string`",
                )
            raise self.stop(pos, f"is a union of {_describe(a)}", "state which it holds")
        if a.scalars:
            return self.scalar(a.scalars[0])
        if a.range:
            raise self.stop(pos, "is a Range with no integer form", f"keep the record property as `{JSON_TEXT}`")
        if a.items is not None:
            if not a.items:
                raise self.stop(pos, "is an array with no `items`", "state its elements")
            return self.list_of(self.value(a.items, None, pos), pos)
        if not a.closed or not a.fields:
            raise self.stop(pos, "is an open object", "list its properties")
        names = list(a.fields)
        return self.struct(a, {n: None for n in names}, names, pos)

    def scalar(self, json_type: str) -> _Value:
        return _Value(_DUCK[json_type], json_type)

    def list_of(self, v: _Value, pos: str) -> _Value:
        if v.slot.endswith("_array"):
            raise self.stop(pos, "is an array of arrays, which has no typed form", f"keep it as `{JSON_TEXT}`")
        pointers = {f"[*]{p}": ts for p, ts in v.pointers.items()}
        return _Value(f"{v.type}[]", f"{v.slot}_array", pointers, v.extension)

    def struct(self, a: _Alts, shape: dict, names: list[str], pos: str) -> _Value:
        """The object at `pos` with the properties `names`, each by its shape."""
        reenters = a.refs & self.path
        if reenters:
            raise self.stop(pos, f"class {sorted(reenters)[0]} reaches itself", "narrow around it")
        outer, self.path = self.path, self.path | a.refs
        fields, pointers, extension = [], {}, False
        for name in names:
            v = self.field(name, (a.fields or {}).get(name, []), shape[name], f"{pos}.{name}")
            fields.append(f'"{name}" {v.type}')
            extension = extension or v.extension
            pointers.update(self.nested_pointers(f".{name}", v, f"{pos}.{name}"))
        self.path = outer
        return _Value(f"STRUCT({', '.join(fields)})", "object", pointers, extension)

    def nested_pointers(self, prefix: str, v: _Value, pos: str) -> dict[str, list[str]]:
        """The pointers of a value placed inside another. Only a column can hold a pointer's section."""
        for targets in v.pointers.values():
            if len(targets) > 1:
                raise self.stop(
                    pos,
                    f"a pointer inside a value has several targets ({', '.join(targets)}), and only a column can "
                    "hold its section",
                    f"name one target, or keep the record property as `{JSON_TEXT}`",
                )
        return {f"{prefix}{p}": targets for p, targets in v.pointers.items()}

    def field(self, name: str, decls: list[_Decl], shape: object, pos: str, top: bool = False) -> _Value:
        if name == EXTENSIONS and shape != JSON_TEXT:
            return self.extensions(decls, shape, pos)
        return self.value(decls, shape, pos, top)

    # --- a position, with its column-set shape -------------------------------------------------------------------

    def value(self, decls: list[_Decl], shape: object, pos: str, top: bool = False) -> _Value:
        return self.from_shape(self.read(decls, pos), shape, pos, top)

    def from_shape(self, a: _Alts, shape: object, pos: str, top: bool = False) -> _Value:
        if shape is None:
            return self.from_schema(a, pos, top)
        if shape == JSON_TEXT:
            if not top:
                raise self.bad_entry(pos, f"`{JSON_TEXT}` is for a property of the record, not one inside a value")
            return _Value("VARCHAR", "string", json_text=True)
        known = a.says_something()
        must_unwrap = a.items is not None and a.fields is None and not a.scalars and not a.range
        if isinstance(shape, list):
            if len(shape) != 1:
                raise self.bad_entry(pos, f"a list shape holds one element shape, not {shape!r}")
            if known and a.items is None:
                raise self.bad_entry(
                    pos, f"contradicts the schema: it says a list, the schema allows {_describe(a)}"
                )
            return self.list_of(self.value(a.items or [], shape[0], pos), pos)
        if must_unwrap and a.items:  # the schema says the list; the shape says what each element is
            return self.list_of(self.value(a.items, shape, pos), pos)
        if isinstance(shape, str):
            return self.from_word(a, shape, pos, known)
        if not isinstance(shape, dict) or not shape:
            raise self.bad_entry(pos, f"unknown shape {shape!r}")
        if a.map:
            raise self.bad_entry(
                pos, f"the schema makes this a map whose keys are data: state it as `{JSON_TEXT}`"
            )
        if known and a.fields is None:
            raise self.bad_entry(
                pos, f"contradicts the schema: it says an object, the schema allows {_describe(a)}"
            )
        return self.struct(a, shape, self.emitted(a, shape, pos), pos)

    def from_word(self, a: _Alts, shape: str, pos: str, known: bool) -> _Value:
        m = _POINTER_SHAPE.fullmatch(shape)
        if m:
            targets = m.group(1).split("|")
            unknown = [t for t in targets if t not in self.section_names]
            if unknown:
                raise self.bad_entry(pos, f"a pointer names {unknown}, not collections of the bundle schema")
            if known and "string" not in a.scalars:
                raise self.bad_entry(
                    pos, f"contradicts the schema: it says a pointer, the schema allows {_describe(a)}"
                )
            return _Value("VARCHAR", "string", pointers={"": targets})
        if shape not in _DUCK:
            raise self.bad_entry(
                pos, f"unknown shape {shape!r} (string, integer, number, boolean, json, ->section)"
            )
        if known and shape not in a.scalars and not (shape == "integer" and "number" in a.scalars):
            raise self.bad_entry(pos, f"contradicts the schema: it says {shape}, the schema allows {_describe(a)}")
        return self.scalar(shape)

    def emitted(self, a: _Alts, shape: dict, pos: str) -> list[str]:
        """The properties a mapping shape emits, in table order; refuses one the schema doesn't have."""
        fields = a.fields or {}
        unknown = [k for k in shape if k not in fields] if a.closed else []
        if unknown:
            raise self.bad_entry(
                pos, f"names {unknown}, which the schema doesn't have here (it has: {', '.join(fields)})"
            )
        return self.order(fields, shape)

    def order(self, fields: dict, shape: dict) -> list[str]:
        """The column set's order for a table in `key_order`; else schema order, then the open positions."""
        if self.ordered:
            return list(shape)
        return [k for k in fields if k in shape] + [k for k in shape if k not in fields]

    # --- extensions ----------------------------------------------------------------------------------------------

    def extensions(self, decls: list[_Decl], shape: object, pos: str) -> _Value:
        a = self.read(decls, pos)
        if a.says_something() and a.items is None:
            raise self.stop(
                pos, f"an {EXTENSIONS} property is {_describe(a)}, not a list", f"keep it as `{JSON_TEXT}`"
            )
        if shape is not None and not (isinstance(shape, list) and shape):
            raise self.bad_entry(pos, f"an {EXTENSIONS} shape is the list of its value shapes, not {shape!r}")
        element = self.read(a.items or [], pos)
        value_pos = f"{pos}.value"
        values = self.read((element.fields or {}).get("value", []), value_pos)
        if shape is not None and element.says_something():
            if element.fields is None or (element.closed and "value" not in element.fields):
                raise self.bad_entry(pos, f"has value shapes, but the schema's elements have no value: {shape!r}")
        if shape is None:
            slots = self.schema_slots(a, element, values, pos)
        else:
            slots = [self.from_shape(values, s, value_pos) for s in shape]
        return self.extension_struct(slots, value_pos)

    def schema_slots(self, a: _Alts, element: _Alts, values: _Alts, pos: str) -> list[_Value]:
        """The slots of an `extensions` position the column set leaves to the schema."""
        value_pos = f"{pos}.value"
        must = "list the value shapes"
        if not a.items or element.opaque or element.fields is None:
            raise self.stop(pos, "its elements are open", must)
        other = set(element.fields) - {"name", "value"}
        if not element.closed or other or "name" not in element.fields or "value" not in element.fields:
            raise self.stop(pos, f"its elements are not {{name, value}} (extra: {sorted(other)})", must)
        if values.opaque or not values.says_something() or (values.fields is not None and not values.closed):
            raise self.stop(value_pos, "the value is open", must)
        if values.items == []:
            raise self.stop(value_pos, "the value is an array with no `items`", must)
        if values.range:
            raise self.stop(value_pos, "the value is a Range", must)
        slots = []
        for alt in values.split():
            if alt.items is None:
                slots.append(self.from_schema(alt, value_pos, False))
                continue
            elements = self.read(alt.items, value_pos)
            if elements.opaque or not elements.says_something():
                raise self.stop(value_pos, "the value's elements are open", must)
            for element_alt in elements.split():
                slots.append(self.list_of(self.from_schema(element_alt, value_pos, False), value_pos))
        return slots

    def extension_struct(self, slots: list[_Value], pos: str) -> _Value:
        by_slot: dict[str, _Value] = {}
        for v in slots:
            if v.slot in by_slot:
                raise self.stop(pos, f"two value shapes fill the slot value_{v.slot}")
            by_slot[v.slot] = v
        fields, pointers = ['"name" VARCHAR'], {}
        for slot in sorted(by_slot, key=_SLOT_ORDER.index):
            v = by_slot[slot]
            fields.append(f'"value_{slot}" {v.type}')
            pointers.update(self.nested_pointers(f"[*].value_{slot}", v, pos))
        return _Value(f"STRUCT({', '.join(fields)})[]", "object_array", pointers, True)

    # --- a table -------------------------------------------------------------------------------------------------

    def record_alts(self, name: str) -> _Alts:
        """The section's record schema: its class, narrowed by any bundle-level `allOf` that names that section."""
        pos = "(the record)"
        bundle, bundle_id = self.schema_set.bundle, self.schema_set.bundle_id
        record = self.read([_record_decl(self.schema_set, bundle["properties"][name], bundle_id)], pos)
        for i, member in enumerate(bundle.get("allOf", ())):
            narrowing = member.get("patternProperties")
            if not isinstance(narrowing, dict) or set(member) - {"description", "patternProperties"}:
                raise CompileError(
                    f"schema {bundle_id}: allOf[{i}]: the rules read only patternProperties, from a "
                    "collection-name pattern to {additionalProperties: <record schema>}"
                )
            for regex, inner in narrowing.items():
                try:
                    applies = re.search(regex, name)
                except re.error as e:
                    raise CompileError(f"schema {bundle_id}: allOf[{i}]: bad pattern {regex!r}: {e}") from e
                if applies:
                    narrow = inner.get("additionalProperties")
                    if set(inner) != {"additionalProperties"} or not isinstance(narrow, dict):
                        raise CompileError(
                            f"schema {bundle_id}: allOf[{i}].{regex}: the rules read only "
                            "{additionalProperties: <record schema>} here"
                        )
                    record = _refine(record, self.read([(narrow, bundle_id)], pos))
        return record

    def record_shape(self, name: str, record: _Alts) -> tuple[dict, dict[str, str]]:
        """(the section's entry as {property: shape}, {property: column name} for the renamed ones)."""
        pos = "(the record)"
        entry = self.column_set["sections"].get(name)
        if entry is None:
            raise self.stop(pos, "the collection has no entry in the column set", "list its properties")
        if not isinstance(entry, dict) or not entry:
            raise self.bad_entry(pos, f"a section's entry is a mapping of properties, not {entry!r}")
        shape, renames = {}, {}
        for key, value in entry.items():
            m = _RENAME.fullmatch(key)
            prop = m.group(1) if m else key
            if prop in shape:
                raise self.bad_entry(pos, f"lists {prop!r} twice")
            shape[prop] = value
            if m:
                if prop == ID:
                    raise self.bad_entry(pos, f"the record's {ID} is the key column; it can't be renamed")
                renames[prop] = m.group(2)
        return shape, renames

    def table(self, name: str) -> dict:
        self.section, self.path = name, frozenset()
        order = self.column_set.get("key_order") or {}
        self.ordered = name in order
        record = self.record_alts(name)
        if record.fields is None:
            record = replace(record, fields={}, closed=False)  # a collection the column set declares, classless
        shape, renames = self.record_shape(name, record)
        names = self.emitted(record, shape, "(the record)")
        self.path = frozenset(record.refs)
        columns = [_column("id", "VARCHAR", "never", _derive("key", None, None), "The bundle key.")]
        slots: dict[str, str] = {}
        for prop in names:
            decls = record.fields.get(prop, [])
            if prop == ID:
                self.check_id(decls, shape[prop])
            else:
                columns.extend(self.columns(prop, decls, shape[prop], slots, renames.get(prop, prop)))
        where = "The record's position in its bundle section (0 = its first record line)."
        columns.append(_column("ordinal", "BIGINT", "never", _derive("ordinal", None, None), where))
        layout: dict = {"key_order": "columns" if self.ordered else "sorted", "record_id": ID in shape}
        if self.ordered:
            layout.update(self.sorted_columns(name, order[name], columns))
        described = self.schema_set.bundle["properties"][name].get("description")
        table = {
            "name": name,
            "sections": [name],
            "row_filter": None,
            "sort_key": "id",
            "known_fields": True,
            "description": _clean(described) or f"Section {name}.",
            "columns": columns,
        }
        if slots:
            table["extension_slots"] = slots
        table["json"] = layout
        return table

    def check_id(self, decls: list[_Decl], shape: object) -> None:
        v = self.value(decls, shape, ID)
        if v.type != "VARCHAR" or v.pointers:
            raise self.stop(ID, "the record's id must be a string, the bundle key", "not list it as anything else")

    def sorted_columns(self, name: str, entry: object, columns: list[dict]) -> dict:
        if entry is None:
            return {}
        if not (
            isinstance(entry, dict)
            and set(entry) == {"sorted_columns"}
            and isinstance(entry["sorted_columns"], list)
        ):
            raise self.bad_entry("(key_order)", f"an entry is null or {{sorted_columns: [...]}}, not {entry!r}")
        have = {c["name"] for c in columns}
        unknown = [c for c in entry["sorted_columns"] if c not in have]
        if unknown:
            raise self.bad_entry("(key_order)", f"sorted_columns names {unknown}, which are not columns of {name}")
        return {"sorted_columns": list(entry["sorted_columns"])}

    def columns(
        self, prop: str, decls: list[_Decl], shape: object, slots: dict[str, str], name: str
    ) -> list[dict]:
        v = self.field(prop, decls, shape, prop, top=True)
        path = f"$.{prop}"
        description = _description(decls) or f"The {prop} property."
        if v.json_text:
            transform = "the value as compact JSON text"
            return [_column(name, "VARCHAR", "when-absent", _derive("json-text", path, transform), description)]
        if v.range:
            return self.range_columns(prop, name, path, description)
        when_absent = "when-absent"
        bare = v.pointers.get("") or v.pointers.get("[*]")
        if bare:
            many = "[*]" in v.pointers
            strip = f"{_STRIP} from each pointer, in order" if many else _STRIP
            out = [_column(name, v.type, when_absent, _derive("bare-id", path, strip, targets=bare), description)]
            if len(bare) > 1:
                which = "the <section> of each pointer, in order" if many else "the <section> of the pointer"
                out.append(
                    _column(
                        f"{name}_section",
                        v.type,
                        when_absent,
                        _derive("pointer-section", path, which, targets=bare),
                        f"The section each pointer of {prop} names." if many else f"The section {prop} names.",
                    )
                )
            return out
        if v.extension:
            slots[name] = "json-type"
        if v.type in _DUCK.values():
            return [_column(name, v.type, when_absent, _derive("scalar-path", path, None), description)]
        kind = "extension-slot" if prop == EXTENSIONS else "typed-copy"
        transform = _SLOT_TRANSFORM if prop == EXTENSIONS else _NESTED_SLOT_TRANSFORM if v.extension else None
        if v.pointers and prop != EXTENSIONS:
            transform = "pointers stripped"
        return [
            _column(name, v.type, when_absent, _derive(kind, path, transform, pointers=v.pointers), description)
        ]

    def range_columns(self, prop: str, base: str, path: str, description: str) -> list[dict]:
        parts = (
            (
                base,
                "range-exact",
                f"the integer {prop}; NULL when {prop} is a Range ([min, max] list)",
                description,
            ),
            (
                f"{base}_min",
                "range-min",
                f"COALESCE(integer {prop}, {prop}[0])",
                f"Lower bound of {prop}: the integer, or the Range's first bound (NULL when open).",
            ),
            (
                f"{base}_max",
                "range-max",
                f"COALESCE(integer {prop}, {prop}[1])",
                f"Upper bound of {prop}: the integer, or the Range's second bound (NULL when open).",
            ),
        )
        return [_column(n, "BIGINT", "when-absent", _derive(k, path, t), d) for n, k, t, d in parts]

    # --- the spec ------------------------------------------------------------------------------------------------

    def check_column_set(self) -> None:
        doc, schema_id = self.column_set, self.schema_set.bundle_id
        where = f"column set for {schema_id}"
        if doc.get("schema") != schema_id:
            raise CompileError(
                f"{where}: it says it pairs with schema {doc.get('schema')!r}, but the schema's $id is "
                f"{schema_id!r}"
            )
        for key in ("key_order", "sections"):
            entries = doc.get(key) or {}
            if not isinstance(entries, dict):
                raise CompileError(f"{where}: {key} must be a mapping")
            stray = [s for s in entries if s not in self.section_names]
            if stray:
                raise CompileError(
                    f"{where}: {key} names {stray}, not collections of the schema "
                    f"({', '.join(self.section_names)})"
                )
        if ROOT_VALUES_TABLE in self.section_names:
            raise CompileError(f"{where}: a collection can't be named {ROOT_VALUES_TABLE}, the root values' table")

    def spec(self) -> dict:
        schema_id = self.schema_set.bundle_id
        if not self.section_names:
            raise CompileError(f"schema {schema_id}: it has no collections")
        self.check_column_set()
        tables = []
        for name in self.section_names:
            try:
                tables.append(self.table(name))
            except (AttributeError, KeyError, TypeError, ValueError) as e:
                # A schema or column set of the wrong JSON shape (a list where a mapping belongs, say).
                raise CompileError(f"schema {schema_id}: section {name}: malformed: {e!r}") from e
        const = self.schema_set.bundle["properties"].get(self.version_property, {}).get("const")
        return {
            "name": SPEC_NAME,
            "spec_version": RULES_VERSION,
            "status": "draft",
            "bundle_schema_versions": [const] if isinstance(const, str) else [],
            "root": {"version_property": self.version_property, "other_values": self.parts.other_values},
            "lossless": True,
            "sources": [
                f"Compiled by the rules {RULES_VERSION} from bundle schema {schema_id} (schema set sha256 "
                f"{self.schema_set.sha256}) and its column set",
            ],
            "writer": dict(WRITER),
            "tables": tables,
        }


def _record_decl(schema_set: SchemaSet, node: dict, base: str) -> _Decl:
    """The record schema of a collection's root property: its `patternProperties` (a union of them), or the schema
    in its `additionalProperties`, following a `$ref` to the collection's definition. {} when it states none."""
    if "$ref" in node and "patternProperties" not in node:
        target, file_id = schema_set.resolve(node["$ref"], base)
        return _record_decl(schema_set, target, file_id)
    patterns = node.get("patternProperties")
    if isinstance(patterns, dict) and patterns:
        values = list(patterns.values())
        return (values[0] if len(values) == 1 else {"anyOf": values}), base
    extra = node.get("additionalProperties")
    return (extra if isinstance(extra, dict) else {}), base


def _root_kind(schema_set: SchemaSet, node: dict, base: str, seen: tuple[str, ...] = ()) -> str:
    """Whether a root property is a `collection` or a `value`; `unknown` when the schema can't say."""
    if "patternProperties" in node or (
        isinstance(node.get("additionalProperties"), dict) and "properties" not in node
    ):
        return "collection"
    ref = node.get("$ref")
    if isinstance(ref, str) and ref not in seen:
        target, file_id = schema_set.resolve(ref, base)
        return _root_kind(schema_set, target, file_id, (*seen, ref))
    if node.get("properties") or (node.get("type") not in ("object", None)) or "items" in node or "const" in node:
        return "value"
    return "unknown"


def root_parts(schema_set: SchemaSet, version_property: str | None = None, declared: object = ()) -> RootParts:
    """The root of a bundle, by its schema and the column set's `collections` (`declared`). A property the schema
    can't place and the column set doesn't declare is a value, kept as JSON text. Entries with no use stop it."""
    schema_id = schema_set.bundle_id
    bundle = schema_set.bundle
    properties = bundle.get("properties", {})
    if version_property is not None and version_property not in properties:
        raise CompileError(
            f"schema {schema_id}: version_property {version_property!r} is not a root property of it"
        )
    if not isinstance(declared, (list, tuple)):
        raise CompileError(f"column set for {schema_id}: collections must be a list of root properties")
    for name in declared:
        if name not in properties:
            raise CompileError(f"column set for {schema_id}: collections names {name!r}, not a root property")
    collections, values = [], False
    if version_property in declared:
        raise CompileError(f"column set for {schema_id}: {version_property!r} is the version, not a collection")
    for name, node in properties.items():
        if name == version_property:
            continue
        kind = _root_kind(schema_set, node, schema_id) if isinstance(node, dict) else "unknown"
        if name in declared and kind != "unknown":
            word = "already says it is a collection" if kind == "collection" else "says it is not a collection"
            raise CompileError(f"column set for {schema_id}: collections names {name!r}, but the schema {word}")
        if kind == "collection" or name in declared:
            collections.append(name)
        else:
            values = True
    return RootParts(tuple(collections), values or bundle.get("additionalProperties") is not False)


def _description(decls: list[_Decl]) -> str:
    """The schema's words for a position: the first declaration that has any, the narrowest first."""
    for decl in decls:
        if isinstance(decl, _Both):
            found = _description(list(decl.later)) or _description(list(decl.earlier))
        else:
            found = _clean(decl[0].get("description"))
        if found:
            return found
    return ""


def _json_type(value: object) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object" if isinstance(value, dict) else "null"


def _describe(a: _Alts) -> str:
    kinds = list(a.scalars)
    if a.range:
        kinds.append("a Range")
    if a.items is not None:
        kinds.append("an array")
    if a.fields is not None:
        kinds.append("an object")
    return ", ".join(kinds) or "nothing stated"


def _clean(text: object) -> str:
    return " ".join(str(text).split()) if text else ""


def _derive(kind: str, path: str | None, transform: str | None, *, targets=None, pointers=None) -> dict:
    d: dict = {"kind": kind, "path": path, "transform": transform}
    if targets:
        d["targets"] = list(targets)
    if pointers:
        d["pointers"] = {p: list(ts) for p, ts in pointers.items()}
    return d


def _column(name: str, type_: str, nulls: str, derivation: dict, description: str) -> dict:
    return {
        "name": name,
        "type": str(duckdb.sqltype(type_)),  # as DESCRIBE prints it: the database's own quoting
        "nulls": nulls,
        "derivation": derivation,
        "description": description,
    }


def compile_sections(schema_set: SchemaSet, column_set: dict) -> dict:
    """The spec of `schema_set` and its column set, as a mapping (module docstring). Raises CompileError naming the
    section and position when the two don't determine a table."""
    return _Compiler(schema_set, column_set).spec()


def spec_for_sorted_input(raw: dict) -> dict:
    """The spec for a bundle read through the general reader, whose records come with every key sorted: `key_order`
    applies only to line-layout text, so every table is key-sorted. Columns and their order stay."""
    tables = []
    for t in raw["tables"]:
        tables.append({**t, "json": {"key_order": "sorted", "record_id": t["json"]["record_id"]}})
    return {**raw, "tables": tables}


def table_spec_sha256(raw: dict) -> str:
    """The sha256 of what a compiled spec makes of the tables: its mapping as canonical JSON, without what doesn't
    affect them (`description`, `transform`, `sources`, `status`)."""
    kept = {k: v for k, v in raw.items() if k not in ("sources", "status", "tables")}
    kept["tables"] = [
        {
            **{k: v for k, v in t.items() if k not in ("description", "columns")},
            "columns": [
                {
                    **{k: v for k, v in c.items() if k not in ("description", "derivation")},
                    "derivation": {k: v for k, v in c["derivation"].items() if k != "transform"},
                }
                for c in t["columns"]
            ],
        }
        for t in raw["tables"]
    ]
    text = json.dumps(kept, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
