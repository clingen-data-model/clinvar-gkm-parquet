"""Compile the `sections` table spec from a bundle schema set and fixed rules (ADR 0006).

`compile_sections(schema_set, overlay)` returns a spec mapping, the format of a tables.yaml (spec.py), that
`spec.parse_spec` and `spec.check_rules(..., builtin=True)` accept. The rules hold for every schema version. No
section, class or property is named here, except the `loc_` rename (LOC_RENAMES) and the structural names the rules
read (`extensions`, `id`, `schema_version`). Each schema pattern has one rule:

- A property of the bundle schema other than schema_version: a section, so one table, in schema order.
- A property of the section's class: a column of the same name, in schema order.
- The record's `id`: the key column (the bundle key); `json.record_id` says whether the class has an `id`.
- An object, or an array of objects: a nested STRUCT, or a list of them.
- A union of objects: one merged STRUCT, the first branch's fields, then each further branch's new fields.
- A string whose `pattern` is ^#/(a|b)/: a pointer, a bare id, plus `<column>_section` if it has several targets.
- An integer or Range at the top level: `<prop>`, `<prop>_min`, `<prop>_max`. A Range is known by its shape, a
  two-element array of integers or nulls.
- An `extensions` list: {name, value_<json type>} slots, one per JSON type its values can take.
- Anything else stops the compile, naming the section and position and what the schema must state: an open object,
  a scalar union, a position that may be a pointer or an inline object, a nested integer-or-Range, a float (a
  lossless table has no exact text for one), a pointer with several targets inside a value, a class that reaches
  itself.

How a position is read: `$ref` is followed. `allOf` members all hold, and a later declaration of a property
replaces an earlier one (a profile narrows its base). `oneOf` and `anyOf` branches are a union. A member or
declaration with no shape keyword (`required`, `contains`, `minItems`) says nothing about the columns.
`iriReference` alone is a plain string. A keyword the rules can't read (`if`, `patternProperties`, ...) makes the
position opaque, so only an overlay can state it.

Order: a key-sorted table (`key_order: sorted`, the schema contract's rule) has its columns and fields in schema
order, then the open positions in overlay order. A table in the producer's order (`key_order: columns`) takes every
order from the overlay, because the bundle text depends on it.

The overlay (spec/sections/overlays/<version>.yaml, ADR 0006 §5) states what a version's published schemas lack:

    schema_version: "1.1.0"
    key_order:                # the sections not written key-sorted; every other section is sorted
      <section>:              # null, or {sorted_columns: [<column>, ...]}: values the export sorts anyway
    sections:
      <section>: {<shape>}    # the record's emitted properties; `id` listed means the record has its own

A shape says what is at one position. On a position the schema declares as an array it says what each element is.

- null: what the schema says. Refused where the schema leaves it open or can't say.
- {a, b: <shape>}: an inline object, of which only these properties are emitted. Refused where the schema has no
  object, or a closed object lacks the property. A property the schema leaves open must state its shape.
- `->a` or `->a|b`: a pointer into those sections (the contract's pattern ^#/(a|b)/). Refused where the schema
  allows no string.
- string, integer, boolean: that scalar, which narrows an integer-or-Range or states an open position. Refused
  where the schema doesn't allow that type.
- [<shape>]: a list, for a position the schema leaves open. Refused where the schema has no array.
- `extensions: [<shape>, ...]`, on a property named extensions: its value shapes, one per slot, all of them.
  Refused where the schema doesn't allow the value.

An entry with no use is refused too: a section the schema lacks, a sorted column that is no column, a section with
an empty shape, a mapping that repeats the schema exactly (nothing narrowed, no new order). So is a schema_version
other than the set's.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from functools import reduce
from pathlib import Path

import yaml

from .errors import CompileError
from .schemas import SchemaSet, split_ref
from .sqltemplate import canonical_type

RULES_VERSION = "1.0"  # the version of these rules: with the schema set's sha256, the identity of a table set
LOC_RENAMES = {
    "location": {"start": "loc_start", "end": "loc_end"}
}  # a choice of this tool: `end` is an SQL keyword
EXTENSIONS, ID, SCHEMA_VERSION = "extensions", "id", "schema_version"
WRITER = {"compression": "snappy", "row_group_size": 122880}
OVERLAY_DIR = "sections/overlays"

_DUCK = {"string": "VARCHAR", "integer": "BIGINT", "boolean": "BOOLEAN"}
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
_POINTER_PREFIX = "^#/"
_POINTER_PATTERN = re.compile(r"\^#/\(?(\w+(?:\|\w+)*)\)?/.*")
_POINTER_SHAPE = re.compile(r"->(\w+(?:\|\w+)*)")
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

_Decl = tuple[dict, str]  # a schema node, and the $id of the file it sits in


@dataclass
class _Alts:
    """What a schema position allows: at most one merged object, one array, and any scalars."""

    scalars: list[str] = field(default_factory=list)  # JSON types
    pointers: list[str] = field(default_factory=list)  # the sections a string's `pattern` names
    items: list[_Decl] | None = None  # an array's element declarations; []: no `items`, so open
    fields: dict[str, list[_Decl]] | None = None  # an object's properties, each with its declarations
    closed: bool = False  # the object allows no other property
    range: bool = False  # a two-element array of integers or nulls: a VRS Range, by its shape
    opaque: str | None = None  # a keyword the rules can't read
    refs: set[str] = field(default_factory=set)  # the object classes this position went through

    def says_something(self) -> bool:
        return (
            bool(self.scalars or self.pointers or self.range) or self.items is not None or self.fields is not None
        )

    def split(self) -> list[_Alts]:
        """One _Alts per alternative: each scalar, the pointer, the array, the object."""
        out = [_Alts(scalars=[t], refs=self.refs) for t in self.scalars]
        if self.pointers:
            out.append(_Alts(pointers=self.pointers, refs=self.refs))
        if self.items is not None:
            out.append(_Alts(items=self.items, refs=self.refs))
        if self.fields is not None:
            out.append(_Alts(fields=self.fields, closed=self.closed, refs=self.refs))
        return out


def _names_sections(node: dict) -> bool:
    return isinstance(node.get("pattern"), str) and node["pattern"].startswith(_POINTER_PREFIX)


def _has_shape(node: dict) -> bool:
    return any(k in node for k in _SHAPE_KEYS) or any(k in node for k in _OPAQUE) or _names_sections(node)


def _refine(a: _Alts, b: _Alts) -> _Alts:
    """Both hold (`allOf`). A later declaration of a property replaces an earlier one."""
    refs, opaque = a.refs | b.refs, a.opaque or b.opaque
    if not a.says_something():
        return replace(b, refs=refs, opaque=opaque)
    if not b.says_something():
        return replace(a, refs=refs, opaque=opaque)
    out = _Alts(opaque=opaque, refs=refs, range=a.range and b.range)
    out.scalars = [t for t in a.scalars if t in b.scalars]
    # A pointer is a string, so a pointer pattern narrows a plain string (iriReference, say).
    pointers = (a.pointers, b.pointers) if a.pointers or b.pointers else ()
    if pointers and (a.pointers or "string" in a.scalars) and (b.pointers or "string" in b.scalars):
        out.pointers = b.pointers or a.pointers
        out.scalars = [t for t in out.scalars if t != "string"]
    if a.items is not None and b.items is not None:
        out.items = b.items or a.items
    if a.fields is not None and b.fields is not None:
        out.fields = dict(a.fields)
        for name, decls in b.fields.items():
            if name not in out.fields or any(_has_shape(n) for n, _ in decls):
                out.fields[name] = decls
        out.closed = a.closed or b.closed
    return out


def _union(a: _Alts, b: _Alts) -> _Alts:
    """Either holds (`oneOf`, `anyOf`): the union rule. The first branch's fields, then each further one's new."""
    out = _Alts(opaque=a.opaque or b.opaque, refs=a.refs | b.refs, range=a.range or b.range)
    out.scalars = a.scalars + [t for t in b.scalars if t not in a.scalars]
    out.pointers = a.pointers + [s for s in b.pointers if s not in a.pointers]
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
    slot: str  # the JSON type's name in a slot: string, boolean, integer, object, <k>_array
    pointers: dict[str, list[str]] = field(default_factory=dict)  # position inside the value -> target sections
    extension: bool = False  # holds an extension struct
    range: bool = False  # an integer-or-Range: three columns


class _Compiler:
    def __init__(self, schema_set: SchemaSet, overlay: dict | None):
        self.schema_set = schema_set
        self.overlay = overlay
        self.section_names = [n for n in schema_set.bundle.get("properties", {}) if n != SCHEMA_VERSION]
        self.section = ""
        self.ordered = False  # the table's key_order is `columns`
        self.path: frozenset[str] = frozenset()  # the classes open on the way to the current position

    # --- errors --------------------------------------------------------------------------------------------------

    def stop(self, pos: str, message: str, must: str | None = None) -> CompileError:
        where = f"schema set {self.schema_set.version}: section {self.section}: position {pos}"
        return CompileError(f"{where}: {message}" + (f"; the schema must {must}" if must else ""))

    def bad_overlay(self, pos: str, message: str) -> CompileError:
        return CompileError(
            f"overlay {self.schema_set.version}: section {self.section}: position {pos}: {message}"
        )

    # --- reading the schema --------------------------------------------------------------------------------------

    def read(self, decls: list[_Decl], pos: str) -> _Alts:
        """What the declarations of one position allow, as a union."""
        alts = [self._node(node, base, (), pos) for node, base in decls if _has_shape(node)]
        return reduce(_union, alts) if alts else _Alts()

    def _node(self, node: dict, base: str, stack: tuple[str, ...], pos: str) -> _Alts:
        out = _Alts()
        ref = node.get("$ref")
        if ref is not None:
            doc_id, pointer = split_ref(ref, base)
            key = f"{doc_id}#{pointer}"
            if key in stack:
                raise self.stop(pos, f"class {key} reaches itself", "leave no class that reaches itself")
            try:
                target, file_id = self.schema_set.resolve(ref, base)
            except Exception as e:
                raise self.stop(pos, f"unknown $ref: {e}", "name a schema of its set") from e
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
        if isinstance(extra, dict):
            out.opaque = out.opaque or "additionalProperties with a schema"
        closes = extra is False or node.get("unevaluatedProperties") is False
        types = node.get("type")
        if types is None and "const" in node:
            types = _json_type(node["const"])
        if types is None and node.get("enum"):
            types = sorted({_json_type(v) for v in node["enum"]})
        if types is None and ("properties" in node or closes):
            types = "object"
        if types is None and "items" in node:
            types = "array"
        pattern = _POINTER_PATTERN.fullmatch(node["pattern"]) if _names_sections(node) else None
        if _names_sections(node) and not pattern:
            out.opaque = out.opaque or f"pointer pattern {node['pattern']!r}, not of the form ^#/(a|b)/,"
        for t in [types] if isinstance(types, str) else (types or ()):
            if t == "object":
                out.fields = {
                    name: [(decl, base)]
                    for name, decl in node.get("properties", {}).items()
                    if isinstance(decl, dict)
                }
                out.closed = closes
            elif t == "array":
                self._own_array(out, node, base, stack, pos)
            elif t == "string" and pattern:
                out.pointers = pattern.group(1).split("|")
            elif t != "null":
                out.scalars.append(t)
        if types is None and pattern:
            out.pointers = pattern.group(1).split("|")
        return out

    def _own_array(self, out: _Alts, node: dict, base: str, stack: tuple[str, ...], pos: str) -> None:
        items = node.get("items")
        if not isinstance(items, dict):
            out.items = []
            return
        if node.get("minItems") == 2 and node.get("maxItems") == 2:
            inner = self._node(items, base, stack, pos)
            if inner.scalars == ["integer"] and not (inner.fields or inner.items or inner.pointers):
                out.range = True
                return
        out.items = [(items, base)]

    # --- a position, from the schema alone -----------------------------------------------------------------------

    def from_schema(self, a: _Alts, pos: str, top: bool) -> _Value:
        if a.opaque:
            raise self.stop(pos, f"uses {a.opaque}, which the rules can't read", "state the value without it")
        if not a.says_something():
            raise self.stop(pos, "says nothing about its value", "define it")
        if a.range and "integer" in a.scalars and not (a.pointers or a.items is not None or a.fields is not None):
            if len(a.scalars) == 1:
                if top:
                    return _Value("BIGINT", "integer", range=True)
                raise self.stop(
                    pos,
                    "an integer-or-Range inside a value has no column form",
                    "narrow it to the kind the producer writes",
                )
        kinds = len(a.scalars) + bool(a.pointers) + (a.items is not None) + (a.fields is not None) + a.range
        if kinds > 1:
            if a.fields is not None and ("string" in a.scalars or a.pointers):
                raise self.stop(
                    pos,
                    "may be a string or an inline object, so pointer or inline is open",
                    "write a pointer as a string whose pattern names its target sections (^#/(a|b)/), and drop "
                    "iriReference from a position that is always inline",
                )
            raise self.stop(pos, f"is a union of {_describe(a)}", "allow one kind of value here")
        if a.pointers:
            return self.pointer(a.pointers, pos)
        if a.scalars:
            return self.scalar(a.scalars[0], pos)
        if a.range:
            raise self.stop(pos, "is a Range with no integer form", "allow an integer here, or none of it")
        if a.items is not None:
            if not a.items:
                raise self.stop(pos, "is an array with no `items`", "define its elements")
            return self.list_of(self.value(a.items, None, pos), pos)
        if not a.closed or not a.fields:
            raise self.stop(pos, "is an open object", "define it fully, with no open object")
        names = list(a.fields)
        return self.struct(a, {n: None for n in names}, names, pos)

    def scalar(self, json_type: str, pos: str) -> _Value:
        if json_type == "number":
            raise self.stop(
                pos, "is a float, which has no exact JSON text in a lossless table", "allow integers here"
            )
        return _Value(_DUCK[json_type], json_type)

    def pointer(self, targets: list[str], pos: str) -> _Value:
        unknown = [s for s in targets if s not in self.section_names]
        if unknown:
            raise self.stop(
                pos, f"a pointer names {unknown}, not sections of the bundle schema", "name its sections"
            )
        return _Value("VARCHAR", "string", pointers={"": list(targets)})

    def list_of(self, v: _Value, pos: str) -> _Value:
        if v.slot.endswith("_array"):
            raise self.stop(pos, "is an array of arrays", "allow one list level here")
        pointers = {f"[*]{p}": ts for p, ts in v.pointers.items()}
        return _Value(f"{v.type}[]", f"{v.slot}_array", pointers, v.extension)

    def struct(self, a: _Alts, shape: dict, names: list[str], pos: str) -> _Value:
        """The object at `pos` with the properties `names`, each by its shape."""
        reenters = a.refs & self.path
        if reenters:
            raise self.stop(
                pos, f"class {sorted(reenters)[0]} reaches itself", "leave no class that reaches itself"
            )
        outer, self.path = self.path, self.path | a.refs
        try:
            fields, pointers, extension = [], {}, False
            for name in names:
                v = self.field(name, (a.fields or {}).get(name, []), shape[name], f"{pos}.{name}")
                fields.append(f'"{name}" {v.type}')
                extension = extension or v.extension
                for p, ts in v.pointers.items():
                    if len(ts) > 1:
                        raise self.stop(
                            f"{pos}.{name}",
                            f"a pointer inside a value has several targets ({', '.join(ts)}), and only a column "
                            "can hold its section",
                            "name one target section here",
                        )
                    pointers[f".{name}{p}"] = ts
        finally:
            self.path = outer
        return _Value(f"STRUCT({', '.join(fields)})", "object", pointers, extension)

    def field(self, name: str, decls: list[_Decl], shape: object, pos: str, top: bool = False) -> _Value:
        return self.extensions(decls, shape, pos) if name == EXTENSIONS else self.value(decls, shape, pos, top)

    # --- a position, with its overlay shape ----------------------------------------------------------------------

    def value(self, decls: list[_Decl], shape: object, pos: str, top: bool = False) -> _Value:
        return self.from_shape(self.read(decls, pos), shape, pos, top)

    def from_shape(self, a: _Alts, shape: object, pos: str, top: bool = False) -> _Value:
        if shape is None:
            return self.from_schema(a, pos, top)
        known = a.says_something()
        must_unwrap = a.items is not None and a.fields is None and not a.scalars and not a.pointers and not a.range
        if isinstance(shape, list):
            if len(shape) != 1:
                raise self.bad_overlay(pos, f"a list shape holds one element shape, not {shape!r}")
            if known and a.items is None:
                raise self.bad_overlay(
                    pos, f"contradicts the schema: it says a list, the schema allows {_describe(a)}"
                )
            return self.list_of(self.value(a.items or [], shape[0], pos), pos)
        if must_unwrap and a.items:  # the schema says the list; the shape says what each element is
            return self.list_of(self.value(a.items, shape, pos), pos)
        if isinstance(shape, str):
            m = _POINTER_SHAPE.fullmatch(shape)
            if m:
                targets = m.group(1).split("|")
                unknown = [t for t in targets if t not in self.section_names]
                if unknown:
                    raise self.bad_overlay(pos, f"a pointer names {unknown}, not sections of the bundle schema")
                if known and not a.pointers and "string" not in a.scalars:
                    raise self.bad_overlay(
                        pos, f"contradicts the schema: it says a pointer, the schema allows {_describe(a)}"
                    )
                if a.pointers and set(a.pointers) != set(targets):
                    raise self.bad_overlay(
                        pos, f"contradicts the schema's pointer targets {a.pointers}: {targets}"
                    )
                return self.pointer(targets, pos)
            if shape not in _DUCK:
                raise self.bad_overlay(
                    pos, f"unknown shape {shape!r} (string, integer, boolean, ->section, ->a|b)"
                )
            if known and shape not in a.scalars and not (shape == "integer" and "number" in a.scalars):
                raise self.bad_overlay(
                    pos, f"contradicts the schema: it says {shape}, the schema allows {_describe(a)}"
                )
            return self.scalar(shape, pos)
        if not isinstance(shape, dict) or not shape:
            raise self.bad_overlay(pos, f"unknown shape {shape!r}")
        if known and a.fields is None:
            raise self.bad_overlay(
                pos, f"contradicts the schema: it says an object, the schema allows {_describe(a)}"
            )
        return self.object_shape(a, shape, pos)

    def object_shape(self, a: _Alts, shape: dict, pos: str) -> _Value:
        return self.struct(a, shape, self.emitted(a, shape, pos), pos)

    def emitted(self, a: _Alts, shape: dict, pos: str) -> list[str]:
        """The properties a mapping shape emits, in table order; refuses one with no position or no use."""
        fields = a.fields or {}
        unknown = [k for k in shape if k not in fields] if a.closed else []
        if unknown:
            raise self.bad_overlay(
                pos, f"names {unknown}, which the schema doesn't have here (it has: {', '.join(fields)})"
            )
        if a.closed and all(v is None for v in shape.values()):
            same = list(shape) == list(fields) if self.ordered else set(shape) == set(fields)
            if same:
                raise self.bad_overlay(
                    pos, "repeats the schema: nothing is narrowed and no order is stated; drop it"
                )
        return self.order(fields, shape)

    def order(self, fields: dict, shape: dict) -> list[str]:
        """The overlay's order for a `columns` table; else schema order, then the open positions."""
        if self.ordered:
            return list(shape)
        return [k for k in fields if k in shape] + [k for k in shape if k not in fields]

    # --- extensions ----------------------------------------------------------------------------------------------

    def extensions(self, decls: list[_Decl], shape: object, pos: str) -> _Value:
        a = self.read(decls, pos)
        if a.says_something() and a.items is None:
            raise self.stop(pos, f"an {EXTENSIONS} property is {_describe(a)}, not a list", "make it a list")
        if shape is not None and not (isinstance(shape, list) and shape):
            raise self.bad_overlay(pos, f"an {EXTENSIONS} shape is the list of its value shapes, not {shape!r}")
        element = self.read(a.items or [], pos)
        value_pos = f"{pos}.value"
        values = self.read((element.fields or {}).get("value", []), value_pos)
        if shape is not None and element.says_something():
            if element.fields is None or (element.closed and "value" not in element.fields):
                raise self.bad_overlay(
                    pos, f"has value shapes, but the schema's elements have no value: {shape!r}"
                )
        if shape is None:
            slots = self.schema_slots(a, element, values, pos)
        else:
            slots = [self.from_shape(values, s, value_pos) for s in shape]
        return self.extension_struct(slots, value_pos)

    def schema_slots(self, a: _Alts, element: _Alts, values: _Alts, pos: str) -> list[_Value]:
        value_pos = f"{pos}.value"
        must = "define every extension value fully, with no open object"
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
            raise self.stop(value_pos, "the value is a Range", "narrow it to the kind the producer writes")
        slots = []
        for alt in values.split():
            if alt.items is None:
                slots.append(self.from_schema(alt, value_pos, False))
                continue
            for element_alt in self.read(alt.items, value_pos).split():
                slots.append(self.list_of(self.from_schema(element_alt, value_pos, False), value_pos))
        return slots

    def extension_struct(self, slots: list[_Value], pos: str) -> _Value:
        by_slot: dict[str, _Value] = {}
        for v in slots:
            if v.slot in by_slot:
                raise self.stop(pos, f"two value shapes fill the slot value_{v.slot}", None)
            by_slot[v.slot] = v
        fields, pointers = ['"name" VARCHAR'], {}
        for slot in sorted(by_slot, key=_SLOT_ORDER.index):
            v = by_slot[slot]
            fields.append(f'"value_{slot}" {v.type}')
            pointers.update({f"[*].value_{slot}{p}": ts for p, ts in v.pointers.items()})
        return _Value(f"STRUCT({', '.join(fields)})[]", "object_array", pointers, True)

    # --- a table -------------------------------------------------------------------------------------------------

    def record_node(self, name: str) -> dict:
        """The section's record schema: its class, plus any bundle-level `allOf` that narrows that section."""
        bundle = self.schema_set.bundle
        section = bundle["properties"][name]
        patterns = section.get("patternProperties") if isinstance(section, dict) else None
        if not (isinstance(patterns, dict) and len(patterns) == 1):
            raise self.stop(
                "(the section)", "isn't a map from one key pattern to a class", "use one patternProperties entry"
            )
        members = [next(iter(patterns.values()))]
        for i, member in enumerate(bundle.get("allOf", ())):
            narrowing = member.get("patternProperties")
            if not isinstance(narrowing, dict) or set(member) - {"description", "patternProperties"}:
                raise CompileError(
                    f"schema set {self.schema_set.version}: bundle schema allOf[{i}]: the rules read only "
                    "patternProperties, from a section-name pattern to {additionalProperties: <record schema>}"
                )
            for regex, inner in narrowing.items():
                if re.search(regex, name):
                    if set(inner) != {"additionalProperties"} or not isinstance(
                        inner["additionalProperties"], dict
                    ):
                        raise CompileError(
                            f"schema set {self.schema_set.version}: bundle schema allOf[{i}].{regex}: the rules "
                            "read only {additionalProperties: <record schema>} here"
                        )
                    members.append(inner["additionalProperties"])
        return {"allOf": members}

    def table(self, name: str) -> dict:
        self.section, self.path = name, frozenset()
        overlay = self.overlay or {}
        shape = overlay.get("sections", {}).get(name)
        order = overlay.get("key_order", {})
        self.ordered = name in order
        bundle_id = self.schema_set.bundle_id
        record = self.read([(self.record_node(name), bundle_id)], "(the record)")
        if record.fields is None:
            raise self.stop("(the record)", "is not an object", "give the section a class with properties")
        if shape is None and not record.closed:
            raise self.stop("(the record)", "is an open object", "define it fully, with no open object")
        if shape is None:
            shape = {k: None for k in record.fields}
            names = list(shape)
        elif isinstance(shape, dict) and shape:
            names = self.emitted(record, shape, "(the record)")
        else:
            raise self.bad_overlay("(the record)", f"a section's shape is a mapping of properties, not {shape!r}")
        outer, self.path = self.path, record.refs
        try:
            columns = [
                _column(
                    "id", "VARCHAR", "never", {"kind": "key", "path": None, "transform": None}, "The bundle key."
                )
            ]
            slots: dict[str, str] = {}
            for prop in names:
                decls = record.fields.get(prop, [])
                if prop == ID:
                    self.check_id(decls, shape[prop])
                    continue
                columns.extend(self.columns(prop, decls, shape[prop], slots))
        finally:
            self.path = outer
        columns.append(
            _column(
                "ordinal",
                "BIGINT",
                "never",
                {"kind": "ordinal", "path": None, "transform": None},
                "The record's position in its bundle section (0 = its first record line).",
            )
        )
        layout: dict = {"key_order": "columns" if self.ordered else "sorted", "record_id": ID in shape}
        if self.ordered:
            layout.update(self.sorted_columns(name, order[name], columns))
        table = {
            "name": name,
            "sections": [name],
            "row_filter": None,
            "sort_key": "id",
            "known_fields": True,
            "description": _clean(self.schema_set.bundle["properties"][name].get("description"))
            or f"Section {name}.",
            "columns": columns,
        }
        if slots:
            table["extension_slots"] = slots
        table["json"] = layout
        return table

    def check_id(self, decls: list[_Decl], shape: object) -> None:
        v = self.value(decls, shape, ID)
        if v.type != "VARCHAR" or v.pointers:
            raise self.stop(ID, "the record's id must be a string, the bundle key", "declare it as a string")

    def sorted_columns(self, name: str, entry: object, columns: list[dict]) -> dict:
        if entry is None:
            return {}
        if not (
            isinstance(entry, dict)
            and set(entry) == {"sorted_columns"}
            and isinstance(entry["sorted_columns"], list)
        ):
            raise self.bad_overlay("(key_order)", f"an entry is null or {{sorted_columns: [...]}}, not {entry!r}")
        have = {c["name"] for c in columns}
        unknown = [c for c in entry["sorted_columns"] if c not in have]
        if unknown:
            raise self.bad_overlay(
                "(key_order)", f"sorted_columns names {unknown}, which are not columns of {name}"
            )
        return {"sorted_columns": list(entry["sorted_columns"])}

    def columns(self, prop: str, decls: list[_Decl], shape: object, slots: dict[str, str]) -> list[dict]:
        v = self.field(prop, decls, shape, prop, top=True)
        path = f"$.{prop}"
        description = _clean(
            next((n["description"] for n, _ in decls if isinstance(n.get("description"), str)), "")
        )
        description = description or f"The {prop} property."
        if v.range:
            return self.range_columns(prop, path, description)
        when_absent = "when-absent"
        bare = v.pointers.get("") or v.pointers.get("[*]")
        if bare:
            many = "[*]" in v.pointers
            strip = f"{_STRIP} from each pointer, in order" if many else _STRIP
            out = [_column(prop, v.type, when_absent, _derive("bare-id", path, strip, targets=bare), description)]
            if len(bare) > 1:
                which = "the <section> of each pointer, in order" if many else "the <section> of the pointer"
                out.append(
                    _column(
                        f"{prop}_section",
                        v.type,
                        when_absent,
                        _derive("pointer-section", path, which, targets=bare),
                        f"The section each pointer of {prop} names." if many else f"The section {prop} names.",
                    )
                )
            return out
        if v.extension:
            slots[prop] = "json-type"
        if v.type in _DUCK.values():
            return [_column(prop, v.type, when_absent, _derive("scalar-path", path, None), description)]
        kind = "extension-slot" if prop == EXTENSIONS else "typed-copy"
        transform = _SLOT_TRANSFORM if prop == EXTENSIONS else _NESTED_SLOT_TRANSFORM if v.extension else None
        if v.pointers and prop != EXTENSIONS:
            transform = "pointers stripped"
        return [
            _column(prop, v.type, when_absent, _derive(kind, path, transform, pointers=v.pointers), description)
        ]

    def range_columns(self, prop: str, path: str, description: str) -> list[dict]:
        base = LOC_RENAMES.get(self.section, {}).get(prop, prop)
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

    def check_overlay(self) -> None:
        overlay = self.overlay
        if overlay is None:
            return
        version = self.schema_set.version
        if not isinstance(overlay, dict):
            raise CompileError(f"overlay {version}: not a mapping")
        unknown = set(overlay) - {"schema_version", "key_order", "sections"}
        if unknown:
            raise CompileError(f"overlay {version}: unknown key(s) {sorted(unknown)}")
        if overlay.get("schema_version") != version:
            raise CompileError(
                f"overlay {version}: says it is for schema_version {overlay.get('schema_version')!r}"
            )
        for key in ("key_order", "sections"):
            entries = overlay.get(key, {})
            if not isinstance(entries, dict):
                raise CompileError(f"overlay {version}: {key} must be a mapping")
            stray = [s for s in entries if s not in self.section_names]
            if stray:
                raise CompileError(f"overlay {version}: {key} names {stray}, not sections of the bundle schema")
        empty = [s for s, shape in overlay.get("sections", {}).items() if shape is None]
        if empty:
            raise CompileError(f"overlay {version}: sections: {empty} state nothing; drop them")

    def spec(self) -> dict:
        self.check_overlay()
        if not self.section_names:
            raise CompileError(f"schema set {self.schema_set.version}: the bundle schema has no sections")
        version = self.schema_set.version
        return {
            "name": "sections",
            "spec_version": RULES_VERSION,
            "status": "draft",
            "bundle_schema_versions": [version],
            "lossless": True,
            "sources": [
                f"Compiled by the rules {RULES_VERSION} (ADR 0006) from bundle schema set {version}, "
                f"sha256 {self.schema_set.sha256}" + (f", with overlay {version}" if self.overlay else ""),
            ],
            "writer": dict(WRITER),
            "tables": [self.table(name) for name in self.section_names],
        }


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
    if a.pointers:
        kinds.append(f"a pointer to {'|'.join(a.pointers)}")
    if a.range:
        kinds.append("a Range")
    if a.items is not None:
        kinds.append("an array")
    if a.fields is not None:
        kinds.append("an object")
    return ", ".join(kinds) or "nothing stated"


def _clean(text: object) -> str:
    return " ".join(str(text).split()) if text else ""


def _derive(kind: str, path: str, transform: str | None, *, targets=None, pointers=None) -> dict:
    d: dict = {"kind": kind, "path": path, "transform": transform}
    if targets:
        d["targets"] = list(targets)
    if pointers:
        d["pointers"] = {p: list(ts) for p, ts in pointers.items()}
    return d


def _column(name: str, type_: str, nulls: str, derivation: dict, description: str) -> dict:
    return {
        "name": name,
        "type": canonical_type(type_),
        "nulls": nulls,
        "derivation": derivation,
        "description": description,
    }


def compile_sections(schema_set: SchemaSet, overlay: dict | None = None) -> dict:
    """The sections spec of `schema_set`, as a mapping (module docstring). Raises CompileError naming the section
    and position when the schemas (and the overlay) don't determine a table."""
    return _Compiler(schema_set, overlay).spec()


def overlay_path(version: str) -> Path | None:
    """The overlay of `version`: in the wheel, or spec/ in a source tree; None: the version has none."""
    for root in (Path(__file__).parent / "_spec", Path(__file__).resolve().parents[2] / "spec"):
        path = root / OVERLAY_DIR / f"{version}.yaml"
        if path.is_file():
            return path
    return None


def load_overlay(version: str) -> dict | None:
    """The parsed overlay of `version`, or None: a version whose schemas meet the contract has none."""
    path = overlay_path(version)
    if path is None:
        return None
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as e:
        raise CompileError(f"{path}: can't read the overlay: {e}") from e
