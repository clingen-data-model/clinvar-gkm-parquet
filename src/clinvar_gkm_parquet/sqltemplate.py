"""Render a table's SQL from sql/<table>.sql and the spec (design plan §3.1), or from the spec alone.

`render(table)` renders the table's shipped sql/<table>.sql. `render_generic(table)` (the generic renderer, for
a table with no such file, e.g. in a user's own spec) writes that file's text from the spec's derivation kinds
(`generic_template`: one placeholder per column, `WHERE {{known_keys}}` for `known_fields`) and renders it the
same way, so a hand-written file of generic columns and the generic renderer give the same SQL byte for byte
(tests/test_generic.py proves it for every such built-in table). It builds key, record, refs (joined on from
baseline.refs_ctes), scalar-path, bare-id, pointer-section, typed-copy and extension-slot columns, with the path
rules in spec.py; `computed` columns and row filters need a shipped file (`generic_problems`).

Owned by the table-spec work package. Contract with build.py:

- `sql/_macros.sql` holds DuckDB macros shared by the tables (e.g. bare-id and pointer-section helpers that fail
  loudly on a non-pointer); build.py runs it once per connection before any table.
- `render(table)` returns ONE SELECT statement over the `bundle_<section>` tables (columns `key`, `record`)
  whose output columns are exactly `table.columns`, in order, with the spec types. Types come from the spec, not
  from the SQL text: the SQL files use placeholders that render() fills from the spec, so each type has one
  source. build.py wraps the result as `COPY (SELECT * FROM (<sql>) ORDER BY <sort_key>) TO ...`.
- Anything the spec can't map (an unknown extension name, an evidence-line id of no known shape, a non-pointer
  proposition object) must make the query fail with DuckDB's error(), naming the table, column and key.

Placeholders in sql/<table>.sql (`{{name args}}`; COL is a spec column, its JSON path is the spec's
`derivation.path`):

- `{{type COL}}`              the column's spec type.
- `{{ctx COL}}`               a SQL string literal 'table <t>: column <c>' for error messages.
- `{{scalar COL}}`            the scalar at the column's path, cast to its type; an object or array fails.
- `{{bare_id COL}}`           the #/<section>/<id> pointer (VARCHAR) or list of pointers (VARCHAR[]) at the
                              path, with ^#/[^/]+/ removed; a non-pointer fails.
- `{{pointer_section COL}}`   the <section> part of the pointer at the path; a non-pointer fails.
- `{{typed COL [ptr=R,R..]}}` typed copy of the value at the path (see below). `ptr` lists the positions,
                              relative to the column value, of pointer strings to strip (`[]` = list element,
                              `.f` = struct field, e.g. `[].value_gene_array[].gene`).
- `{{known_keys}}`            TRUE, or an error when the record has a top-level field no column's path names
                              (for tables whose columns copy the whole record, BigQuery's `SELECT t.*`).
- `{{json COL}}`              the JSON value at the column's path (as json_extract gives it), for hand-written
                              derivations.
- `{{walk COL}}`              the value at a path with `[*]` steps (spec.py's path rules), each leaf read by
                              the column's kind (scalar-path, bare-id or pointer-section), cast to its type.
- `{{source}}`                the FROM item: `bundle_<section>`, with each record parsed once (see below).

Typed copy (derivation kinds typed-copy, extension-slot and list bare-id) walks the spec type: every JSON object
must be an object, every list a list, and an object may hold only the fields its spec struct has, so a bundle
field the spec type lacks stops the build instead of being dropped (DuckDB's json_transform ignores unknown keys,
and CAST(JSON AS STRUCT) also rejects absent ones, so neither is used on its own). A struct with a `name` field
and `value_*` fields is an extension: the bundle's `{name, value}` is put in the slot that the table's
`extension_slots` map gives for that name, and a name the map doesn't list for that struct stops the build.
Empty versus NULL inside typed values (design plan §3.5): an absent struct or scalar is NULL; an absent list is
`[]` (BigQuery stores no NULL arrays) when its parent is present, and at the top level follows the column's
`nulls` rule.

That walk (the slow expression) parses each JSON value again at every level. With `{{source}}` the record is
parsed once instead, by one json_transform into the spec types (`_t`), and each typed copy gets a fast expression
that reads `_t`; a census (_Source) decides, per row, whether the fast expressions equal the slow ones, and a row
it can't vouch for takes the slow expression, which stops with its own message. The output is the same, byte for
byte, and so is every failure: render(table, fast=False) gives the slow SQL alone, and the tests compare the two.
"""

from __future__ import annotations

import json
import re
from dataclasses import replace
from functools import cache
from importlib import resources

import duckdb

from .errors import SpecError
from .spec import Column, Table

_PLACEHOLDER = re.compile(r"\{\{\s*(\w+)((?:\s+[^\s}]+)*)\s*\}\}")
_SCALARS = {"varchar", "bigint", "boolean", "date", "integer", "double"}


def _sql_text(name: str) -> str:
    return resources.files("clinvar_gkm_parquet").joinpath("sql", name).read_text(encoding="utf-8")


def macros_sql() -> str:
    return _sql_text("_macros.sql")


def _lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _ident(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


@cache
def _parse_type(type_str: str) -> duckdb.typing.DuckDBPyType:
    try:
        t = duckdb.sqltype(type_str)
    except Exception as e:  # duckdb raises its own exception types
        raise SpecError(f"invalid DuckDB type {type_str!r}: {e}") from e
    return t


def canonical_type(type_str: str) -> str:
    """The type string as DuckDB prints it (what DESCRIBE shows)."""
    return str(_parse_type(type_str))


class _Gen:
    """Generates the typed-copy expression for one column."""

    def __init__(self, table: Table, column: Column, pointers: set[str]):
        self.table = table
        self.column = column
        self.ctx = _lit(f"table {table.name}: column {column.name}")
        self.slots = table.extension_slots.get(column.name, {})
        self.pointers = pointers
        self.used_pointers: set[str] = set()
        self.used_slots: set[str] = set()
        self.n = 0

    def var(self, prefix: str) -> str:
        self.n += 1
        return f"{prefix}{self.n}"

    def fail(self, msg_sql: str) -> str:
        return f"cgp_fail({self.ctx}, key, {msg_sql})"

    def bind(self, x: str, body) -> str:
        """Evaluate x once: (list_transform([x], lambda v: body(v)))[1]."""
        v = self.var("v")
        return f"(list_transform([{x}], lambda {v}: {body(v)}))[1]"

    def expr(self, x: str, t, rel: str, top: bool = False) -> str:
        tid = t.id
        if tid == "struct":
            return self.struct(x, t, rel)
        if tid == "list":
            return self.list(x, t, rel, top)
        if tid not in _SCALARS:
            raise SpecError(f"{self.ctx}: unsupported type {t} at {rel or '(column)'}")
        where = rel or "(column)"
        if rel in self.pointers:
            self.used_pointers.add(rel)
            if tid != "varchar":
                raise SpecError(f"{self.ctx}: pointer at {where} must be VARCHAR")
            return f"cgp_bare_id({x}, {self.ctx}, key, {_lit(where)})"
        if tid == "varchar":
            return f"cgp_str({x}, {self.ctx}, key, {_lit(where)})"
        return f"cgp_scalar({x}, {self.ctx}, key, {_lit(where)})::{t}"

    def list(self, x: str, t, rel: str, top: bool) -> str:
        child = t.children[0][1]
        crel = f"{rel}[]"
        absent = "NULL" if (top and self.column.nulls == "when-absent") else f"CAST([] AS {t})"

        e = self.var("e")
        not_list = self.fail(_lit(f"expected a list at {rel or '(column)'}, got ") + f" || {x}")
        # x is a lambda variable or a struct field (cheap to repeat); the column root is bound once.
        return (
            f"CASE coalesce(json_type({x}), 'NULL') WHEN 'NULL' THEN {absent} "
            f"WHEN 'ARRAY' THEN list_transform(CAST({x} AS JSON[]), lambda {e}: {self.expr(e, child, crel)}) "
            f"ELSE {not_list} END"
        )

    def struct(self, x: str, t, rel: str) -> str:
        fields = list(t.children)
        names = [f for f, _ in fields]
        slot_fields = [f for f in names if f.startswith("value_")]
        is_ext = "name" in names and bool(slot_fields)
        if is_ext and not self.slots:
            raise SpecError(f"{self.ctx}: extension struct at {rel or '(column)'} but no extension_slots entry")
        plain = [f for f in names if f not in slot_fields]
        json_keys = plain + (["value"] if is_ext else [])
        where = rel or "(column)"
        # Fields come out as JSON for the per-field checks; an extension's name comes out as VARCHAR, read once
        # (a non-string name gives text that is no known name, so it still fails).
        structure = _lit(json.dumps({k: ("VARCHAR" if is_ext and k == "name" else "JSON") for k in json_keys}))
        allowed = "[" + ", ".join(_lit(k) for k in json_keys) + "]"

        k = self.var("k")
        s = self.var("s")
        unknown = f"list_filter(json_keys({x}), lambda {k}: NOT list_contains({allowed}, {k}))"
        packed = self.pack(s, fields, rel, is_ext)
        not_object = self.fail(_lit(f"expected an object at {where}, got ") + f" || {x}")
        extra = self.fail(_lit(f"field(s) not in the spec type at {where}: ") + f" || {unknown}::VARCHAR")
        # One json_transform per object, bound to s so the fields are read from it without re-parsing.
        return (
            f"CASE coalesce(json_type({x}), 'NULL') WHEN 'NULL' THEN NULL "
            f"WHEN 'OBJECT' THEN CASE WHEN NOT list_has_all({allowed}, json_keys({x})) THEN {extra} "
            f"ELSE (list_transform([json_transform({x}, {structure})], lambda {s}: {packed}))[1] END "
            f"ELSE {not_object} END"
        )

    def pack(self, s: str, fields, rel: str, is_ext: bool) -> str:
        parts = []
        if is_ext:
            here = {f for f, _ in fields if f.startswith("value_")}
            known = sorted(n for n, slot in self.slots.items() if slot in here)
            self.used_slots.update(known)
            name_sql = f'{s}."name"'
        for f, ft in fields:
            frel = f"{rel}.{f}"
            if is_ext and f == "name":
                val = name_sql
            elif is_ext and f.startswith("value_"):
                names = sorted(n for n, slot in self.slots.items() if slot == f)
                absent = f"CAST([] AS {ft})" if ft.id == "list" else "NULL"
                if names:
                    cond = f"list_contains([{', '.join(_lit(n) for n in names)}], {name_sql})"
                    value = self.expr(s + '."value"', ft, frel)
                    val = f"CASE WHEN {cond} THEN {value} ELSE {absent} END"
                else:
                    val = absent
            else:
                val = self.expr(f"{s}.{_ident(f)}", ft, frel)
            parts.append(f"{_ident(f)} := {val}")
        packed = f"struct_pack({', '.join(parts)})"
        if not is_ext:
            return packed
        unknown = self.unknown_name(name_sql, known)
        return f"CASE WHEN {unknown} THEN {self.name_fail(name_sql, rel)} ELSE {packed} END"

    def unknown_name(self, name_sql: str, known: list[str]) -> str:
        known_sql = "[" + ", ".join(_lit(n) for n in known) + "]"
        return f"NOT list_contains({known_sql}, coalesce({name_sql}, ''))"

    def name_fail(self, name_sql: str, rel: str) -> str:
        msg = _lit(f"unknown extension name at {rel or '(column)'}: ") + f" || coalesce({name_sql}, '(none)')"
        return self.fail(msg)


def typed_expr(table: Table, column: Column, pointers: set[str] | None = None) -> tuple[str, _Gen]:
    path = column.derivation.path
    if not path:
        raise SpecError(f"table {table.name}: column {column.name}: typed copy needs a derivation path")
    gen = _Gen(table, column, set(pointers or ()))
    t = _parse_type(column.type)
    body = gen.bind(f"json_extract(record, {_lit(path)})", lambda v: gen.expr(v, t, "", top=True))
    missing = gen.pointers - gen.used_pointers
    if missing:
        raise SpecError(
            f"table {table.name}: column {column.name}: ptr position(s) not in the type: {sorted(missing)}"
        )
    return f"CAST({body} AS {column.type})", gen


# ---------------------------------------------------------------- {{source}}: parse once, check by census

_TOP_PATH = re.compile(r"\$\.(\w+)")
_JSON_PATH = re.compile(r"\$(\.\w+)+")
_SOURCE = "\x00source\x00"
_KNOWN = "\x00known_keys\x00"
# The census counts nodes, plus _FLAG for each node caught in a way the count alone can't vouch for. Records of
# _MAX_RECORD bytes or more take the slow expressions, so a record has fewer than 2^23 nodes and a flag can't be
# balanced (nor the BIGINT sum overflow).
_FLAG = str(2**24)
_MAX_RECORD = str(2**23)


def _scalar_census(x: str) -> str:
    """A leaf: an empty object or array read as text ('{}' or '[]', which a string can also be) is flagged, as
    the count alone doesn't see that it isn't a scalar."""
    return f"CASE WHEN {x} IS NULL THEN 0 WHEN {x} IN ('{{}}', '[]') THEN {_FLAG} ELSE 1 END"


def _pointer_census(x: str) -> str:
    """A pointer (read as VARCHAR) counts only in cgp_pointer's own shape, so the fast strips below hold."""
    return f"CASE WHEN {x} IS NULL THEN 0 WHEN regexp_matches({x}, '^#/[^/]+/.') THEN 1 ELSE {_FLAG} END"


def _bare_id(x: str) -> str:
    """regexp_replace(x, '^#/[^/]+/', ''), for x of cgp_pointer's shape."""
    return f"substr({x}, 3 + strpos(substr({x}, 3), '/'))"


def _section(x: str) -> str:
    """regexp_extract(x, '^#/([^/]+)/', 1), for x of cgp_pointer's shape."""
    return f"substr({x}, 3, strpos(substr({x}, 3), '/') - 1)"


class _Unsupported(Exception):
    """A typed column the fast expression doesn't cover; it keeps the slow expression alone."""


class _Fast:
    """The fast expression for one typed column: read from the column's value in the record's one json_transform
    (`_t`), with the census of what that json_transform caught. See _Source."""

    def __init__(self, gen: _Gen, key: str):
        self.gen = gen
        self.key = key
        self.slots: dict[str, tuple[list[str], object]] = {}  # slot -> (names, structure): see _Source.parsed

    def walk(self, x: str, t, rel: str, top: bool = False) -> tuple[object, str, str]:
        """(json_transform structure, value SQL, census SQL) for x, the json_transform of a value of spec type t;
        rel is the slow generator's position, for its leaves and messages."""
        gen = self.gen
        if t.id == "list":
            child = t.children[0][1]
            absent = "NULL" if (top and gen.column.nulls == "when-absent") else f"CAST([] AS {t})"
            e = gen.var("e")
            if top and self.name_dependent(child):
                # The column's own list of name-dependent extensions: read through _Source.parsed.
                x = _ident(f"_p_{self.key}")
                s, val, census = self.ext(f"{e}.e", child, f"{rel}[]", e)
            else:
                s, val, census = self.walk(e, child, f"{rel}[]")
            body = x if val == e else f"list_transform({x}, lambda {e}: {val})"
            if census == _scalar_census(e):
                # (Two list_contains: list_has_any in a lambda is quadratic in DuckDB 1.5.4.)
                empty = f"list_contains({x}, '{{}}') OR list_contains({x}, '[]')"
                inner = f"list_count({x}) + CASE WHEN {empty} THEN {_FLAG} ELSE 0 END"
            else:
                inner = f"coalesce(list_sum(list_transform({x}, lambda {e}: {census})), 0)"
            return (
                [s],
                f"CASE WHEN {x} IS NULL THEN {absent} ELSE {body} END",
                f"CASE WHEN {x} IS NULL THEN 0 ELSE 1 + {inner} END",
            )
        if t.id == "struct":
            if self.is_ext(t):
                return self.ext(x, t, rel)
            structure: dict[str, object] = {}
            vals, counts, same = [], [], True
            for f, ft in t.children:
                fx = f"{x}.{_ident(f)}"
                structure[f], val, census = self.walk(fx, ft, f"{rel}.{f}")
                same = same and val == fx
                vals.append(f"{_ident(f)} := {val}")
                counts.append(census)
            value = x if same else f"CASE WHEN {x} IS NULL THEN NULL ELSE struct_pack({', '.join(vals)}) END"
            return structure, value, self.struct_census(x, list(structure), counts)
        return self.leaf(x, t, rel)

    def leaf(self, x: str, t, rel: str, json_value: bool = False) -> tuple[object, str, str]:
        """A scalar the census has seen as a scalar (or, for a pointer, in pointer shape), so the slow generator's
        leaf reduces to a read. json_value: x is JSON text already (an extension value), not a json_transform
        field this leaf types."""
        gen = self.gen
        if t.id not in _SCALARS:
            raise SpecError(f"{gen.ctx}: unsupported type {t} at {rel or '(column)'}")
        if rel in gen.pointers:
            if json_value:
                return "JSON", gen.expr(x, t, rel), _scalar_census(x)
            if t.id != "varchar":
                raise SpecError(f"{gen.ctx}: pointer at {rel or '(column)'} must be VARCHAR")
            gen.used_pointers.add(rel)
            return "VARCHAR", _bare_id(x), _pointer_census(x)
        if t.id == "varchar":  # cgp_str of a scalar
            return "VARCHAR", f"json_extract_string({x}, '$')" if json_value else x, _scalar_census(x)
        return "JSON", f"CAST({x} AS {t})", _scalar_census(x)  # cgp_scalar of a scalar, cast

    @staticmethod
    def is_ext(t) -> bool:
        names = [f for f, _ in t.children]
        return "name" in names and any(f.startswith("value_") for f in names)

    def used_slots(self, t) -> dict[str, list[str]]:
        """{slot: its names} for the slots of extension struct t that some name maps to."""
        here = [f for f, _ in t.children if f.startswith("value_")]
        names = {f: sorted(n for n, slot in self.gen.slots.items() if slot == f) for f in here}
        return {f: ns for f, ns in names.items() if ns}

    def name_dependent(self, t) -> bool:
        """An extension struct whose value's shape depends on its name (two slots, one nested or more)."""
        if t.id != "struct" or not self.is_ext(t):
            return False
        types = dict(t.children)
        used = self.used_slots(t)
        return len(used) > 1 and any(types[f].id in ("list", "struct") for f in used)

    def struct_census(self, x: str, fields: list[str], counts: list[str]) -> str:
        # A non-NULL struct with every field NULL is flagged: json_transform makes one out of a scalar or an array
        # (and out of {}, which the bundle doesn't write), and the count alone can't tell it from an object.
        empty = " AND ".join(f"{x}.{_ident(f)} IS NULL" for f in fields)
        return f"CASE WHEN {x} IS NULL THEN 0 WHEN {empty} THEN {_FLAG} ELSE 1 + {' + '.join(counts)} END"

    def ext(self, x: str, t, rel: str, parsed: str | None = None) -> tuple[object, str, str]:
        gen = self.gen
        types = dict(t.children)
        known = sorted(n for n, slot in gen.slots.items() if slot in types)
        used = self.used_slots(t)
        nested = [f for f in used if types[f].id in ("list", "struct")]
        if self.name_dependent(t) and parsed is None:
            raise _Unsupported(rel)  # name-dependent below the column's own list
        name_sql = f'{x}."name"'
        v = f'{x}."value"'
        structure: dict[str, object] = {"name": "VARCHAR", "value": "JSON"}
        vals, counts, value_counts = [], [], []
        for f, ft in t.children:
            frel = f"{rel}.{f}"
            if f == "name":
                vals.append(f"{_ident(f)} := {name_sql}")
                counts.append(_scalar_census(name_sql))
            elif f in types and f.startswith("value_"):
                absent = f"CAST([] AS {ft})" if ft.id == "list" else "NULL"
                if f not in used:
                    vals.append(f"{_ident(f)} := {absent}")
                    continue
                if f in nested and parsed is not None:  # parsed by its name's slot type, in _Source.parsed
                    s, val, census = self.walk(f"{parsed}.{_ident(f)}", ft, frel)
                    self.slots[f] = (used[f], s)
                elif f in nested:  # the one slot: read in the record's json_transform
                    structure["value"], val, census = self.walk(v, ft, frel)
                else:
                    _, val, census = self.leaf(v, ft, frel, json_value=True)
                cond = f"list_contains([{', '.join(_lit(n) for n in used[f])}], {name_sql})"
                vals.append(f"{_ident(f)} := CASE WHEN {cond} THEN {val} ELSE {absent} END")
                value_counts.append(f"WHEN {cond} THEN {census}")
            else:
                fx = f"{x}.{_ident(f)}"
                structure[f], val, census = self.walk(fx, ft, frel)
                vals.append(f"{_ident(f)} := {val}")
                counts.append(census)
        # The value is caught only under a known name; any other name leaves it out of the census.
        counts.append(f"CASE {' '.join(value_counts)} ELSE 0 END" if value_counts else "0")
        value = (
            f"CASE WHEN {x} IS NULL THEN NULL WHEN {gen.unknown_name(name_sql, known)} "
            f"THEN {gen.name_fail(name_sql, rel)} ELSE struct_pack({', '.join(vals)}) END"
        )
        return structure, value, self.struct_census(x, list(structure), counts)


class _Source:
    """{{source}}: the bundle table (after the table's own row filter) with each record parsed once, into `_t`
    (one json_transform for every placeholder that reads its path) and, when the table has a typed copy of
    nested type, `_ok`: the row's fast expressions equal its slow ones.

    A fast expression reads its value from `_t`, where json_transform has typed it the spec's way. That drops what
    the slow expression stops on: a field the spec type lacks, and a JSON kind (object, array, scalar) other than
    the spec's, which json_transform turns into NULL, a struct of NULLs or the value's JSON text; and it can't tell
    JSON null from absent. So `_ok` is a census: the record's nodes (json_array_length over `$.**`, one parse)
    equal the nodes `_t` caught, with none of them flagged. Each caught node is a distinct node of the record, so
    the counts agree only if every node was caught, JSON null included (a null is never caught). A node caught as
    the wrong kind is flagged where the count can't see it: a struct of NULLs (what a scalar or an array becomes)
    and '{}' or '[]' read as a scalar (an empty container where a scalar goes). So is a pointer not of
    cgp_pointer's shape, so that its fast strip equals the regex. Where the census agrees, every slow leaf check
    has passed, so a leaf reduces to a read, and the checks that remain (extension names, casts) are the slow
    generator's own, with its messages. Where it differs, the row takes the slow expressions."""

    def __init__(self, table: Table, census: bool):
        self.table = table
        self.has_census = census  # the table has nested typed columns: _ok exists
        self.structure: dict[str, object] = {}
        self.census: dict[str, str] = {}  # top-level key -> census SQL of what _t caught there
        self.fast: list[_Fast] = []
        self.known_keys = False

    def _put(self, key: str, s: object, census: str) -> None:
        if self.structure.setdefault(key, s) != s or self.census.setdefault(key, census) != census:
            raise SpecError(f"table {self.table.name}: path $.{key} is read with two shapes")

    def json(self, c: Column) -> str:
        """The JSON value at the column's path, as json_extract gives it (or NULL for JSON null, which the leaf
        macros also take as absent); json_extract itself where _t can't hold the path."""
        x = self._json_path(c.derivation.path or "")
        return x or f"json_extract(record, {_path(self.table, c)})"

    def _json_path(self, path: str) -> str | None:
        if not _JSON_PATH.fullmatch(path):
            return None
        keys = path[2:].split(".")
        x = "_t." + ".".join(_ident(k) for k in keys)
        if len(keys) == 1:
            if self.structure.get(keys[0], "JSON") != "JSON":
                return None
            self._put(keys[0], "JSON", _scalar_census(x))
            return x
        if self.has_census:  # the census reads whole top-level fields
            return None
        node = self.structure
        for k in keys[:-1]:
            node = node.setdefault(k, {})
            if not isinstance(node, dict):
                return None
        return x if node.setdefault(keys[-1], "JSON") == "JSON" else None

    def scalar(self, c: Column, slow_of, pointer: bool) -> str:
        """{{scalar}}, or {{pointer_section}} (pointer); slow_of(j) is its macro on the JSON value j."""
        m = _TOP_PATH.fullmatch(c.derivation.path or "")
        slow = slow_of(f"json_extract(record, {_path(self.table, c)})")
        if not self.has_census:  # the macro on the same JSON value is the whole check
            return slow_of(self.json(c))
        if not m:
            return slow
        x = f"_t.{_ident(m.group(1))}"
        t = _parse_type(c.type)
        if pointer:
            s, fast, census = "VARCHAR", _section(x), _pointer_census(x)
        elif t.id == "varchar":
            s, fast, census = "VARCHAR", x, _scalar_census(x)
        else:
            s, fast, census = "JSON", f"CAST({x} AS {c.type})", _scalar_census(x)
        self._put(m.group(1), s, census)
        return f"CASE WHEN _ok THEN {fast} ELSE {slow} END"

    def typed(self, c: Column, gen: _Gen, slow: str) -> str:
        m = _TOP_PATH.fullmatch(c.derivation.path or "")
        if not m:
            return slow
        x = f"_t.{_ident(m.group(1))}"
        t = _parse_type(c.type)
        if not self.has_census:  # the slow expression, on the same JSON value
            return f"CAST({gen.expr(self.json(c), t, '', top=True)} AS {c.type})"
        fast = _Fast(gen, m.group(1))
        try:
            s, expr, census = fast.walk(x, t, "", top=True)
        except _Unsupported:
            return slow
        self._put(m.group(1), s, census)
        self.fast.append(fast)
        return f"CASE WHEN _ok THEN CAST({expr} AS {c.type}) ELSE {slow} END"

    def sql(self, row_filter: str | None) -> str:
        """row_filter: the table's own row filter (its WHERE without the known_keys check), applied first, so
        the parse and the census see only the table's rows."""
        bundle = _ident(f"bundle_{self.table.sections[0]}")
        if row_filter:
            bundle = f"(SELECT key, record FROM {bundle} WHERE {row_filter})"
        if not self.structure:
            return bundle
        if self.has_census and self.known_keys:
            # _ok also stands for the known_keys check: a top-level field _t doesn't read is a node it misses.
            for k in _known_key_set(self.table):
                if k not in self.structure:
                    self._put(k, "JSON", _scalar_census(f"_t.{_ident(k)}"))
        sql = f"SELECT key, record, json_transform(record, {_lit(json.dumps(self.structure))}) AS _t FROM {bundle}"
        if not self.has_census:
            return f"({sql}) AS _src"
        parsed = [self.parsed(f) for f in self.fast if f.slots]
        if parsed:
            sql = f"SELECT *, {', '.join(parsed)} FROM ({sql})"
        record = "len(json_array_length(record, '$.**'))"  # the record's nodes but itself: the cheapest list
        caught = " + ".join(self.census.values())
        return f"(SELECT *, strlen(record) < {_MAX_RECORD} AND {record} = {caught} AS _ok FROM ({sql})) AS _src"

    @staticmethod
    def parsed(fast: _Fast) -> str:
        """`_p_<col>`: the column's name-dependent extensions, each as {e: the element in _t, <slot>: its value
        parsed by the slot's type, for a name of that slot}."""
        x = f"_t.{_ident(fast.key)}"
        slots = [
            f'{_ident(f)} := CASE WHEN list_contains([{", ".join(map(_lit, names))}], e."name") '
            f'THEN json_transform(e."value", {_lit(json.dumps(s))}) END'
            for f, (names, s) in fast.slots.items()
        ]
        return (
            f"list_transform({x}, lambda e: struct_pack(e := e, {', '.join(slots)})) AS {_ident('_p_' + fast.key)}"
        )


def _census(table: Table, text: str) -> bool:
    """{{source}} takes a census when the table has a typed copy of nested type at a top-level path, and checks
    known_keys (else a top-level field no column reads would keep every row on the slow path)."""
    if not re.search(r"\{\{\s*known_keys\s*\}\}", text):
        return False
    for m in _PLACEHOLDER.finditer(text):
        if m.group(1) in ("typed", "bare_id"):
            c = table.column(m.group(2).split()[0])
            if _TOP_PATH.fullmatch(c.derivation.path or "") and _parse_type(c.type).id in ("list", "struct"):
                return True
    return False


def _col(table: Table, name: str) -> Column:
    return table.column(name)


def _path(table: Table, c: Column) -> str:
    if not c.derivation.path:
        raise SpecError(f"table {table.name}: column {c.name}: placeholder needs a derivation path")
    return _lit(c.derivation.path)


def _known_key_set(table: Table) -> list[str]:
    keys = {"id"}
    for c in table.columns:
        p = c.derivation.path
        if p and c.derivation.kind != "computed":
            m = re.fullmatch(r"\$\.(\w+)", p)
            if not m:
                raise SpecError(f"table {table.name}: column {c.name}: known_keys needs a top-level path, got {p}")
            keys.add(m.group(1))
    return sorted(keys)


def _known_keys(table: Table) -> str:
    allowed = "[" + ", ".join(_lit(k) for k in _known_key_set(table)) + "]"
    return f"cgp_known_keys(record, {allowed}, {_lit('table ' + table.name)}, key)"


def _substitute(
    table: Table, text: str, src: _Source | None, slot_cols: dict[str, _Gen], known: str | None = None
) -> str:
    def sub(m: re.Match) -> str:
        name, args = m.group(1), m.group(2).split()
        opts = dict(a.split("=", 1) for a in args if "=" in a)
        pos = [a for a in args if "=" not in a]
        if name == "source":
            if len(table.sections) != 1:
                raise SpecError(f"table {table.name}: {m.group(0)} needs exactly one section")
            return _SOURCE if src else _ident(f"bundle_{table.sections[0]}")
        if name == "known_keys":
            if known is not None:
                return known
            if src is None:
                return _known_keys(table)
            src.known_keys = True
            return _KNOWN
        if len(pos) != 1:
            raise SpecError(f"{table.name}.sql: {m.group(0)}: expected one column argument")
        c = _col(table, pos[0])
        ctx = _lit(f"table {table.name}: column {c.name}")
        if name == "type":
            return c.type
        if name == "json":
            return src.json(c) if src else f"json_extract(record, {_path(table, c)})"
        if name == "ctx":
            return ctx
        if name in ("scalar", "pointer_section"):
            t = _parse_type(c.type)

            def slow_of(j: str) -> str:
                if name == "pointer_section":
                    return f"cgp_pointer_section({j}, {ctx}, key)"
                if t.id == "varchar":
                    return f"cgp_str({j}, {ctx}, key, '(column)')"
                return f"CAST(cgp_scalar({j}, {ctx}, key, '(column)') AS {c.type})"

            if src:
                return src.scalar(c, slow_of, pointer=name == "pointer_section")
            return slow_of(f"json_extract(record, {_path(table, c)})")
        if name == "walk":
            return _walk_expr(table, c)
        if name in ("bare_id", "typed"):
            if name == "bare_id":
                ptr = {""} if _parse_type(c.type).id == "varchar" else {"[]"}
            else:
                ptr = set(opts.get("ptr", "").split(",")) - {""}
            expr, gen = typed_expr(table, c, ptr)
            if name == "typed" and c.name in table.extension_slots:
                slot_cols[c.name] = gen
            return src.typed(c, gen, expr) if src else expr
        raise SpecError(f"{table.name}.sql: unknown placeholder {m.group(0)}")

    return _PLACEHOLDER.sub(sub, text)


def has_template(table_name: str) -> bool:
    """The tool ships sql/<table_name>.sql."""
    return resources.files("clinvar_gkm_parquet").joinpath("sql", f"{table_name}.sql").is_file()


def render(table: Table, fast: bool = True) -> str:
    """The table's SELECT. fast=False leaves {{source}} out (every placeholder reads the record with json_extract
    and every typed copy takes the slow expression): the reference the fast SQL must equal, for tests."""
    return _render_text(table, _sql_text(f"{table.name}.sql"), fast)


def _render_text(table: Table, text: str, fast: bool) -> str:
    """render() of a template text: sql/<table>.sql, or generic_template's."""
    slot_cols: dict[str, _Gen] = {}
    src = _Source(table, _census(table, text)) if fast and re.search(r"\{\{\s*source\s*\}\}", text) else None
    sql = _substitute(table, text, src, slot_cols)
    if src:
        if src.known_keys:
            known = _known_keys(table)
            sql = sql.replace(_KNOWN, f"CASE WHEN _ok THEN TRUE ELSE {known} END" if src.has_census else known)
        m = re.search(r"^WHERE (.*)$", text, re.M)
        row_filter = _substitute(table, m.group(1), None, {}, known="TRUE") if m else "TRUE"
        sql = sql.replace(_SOURCE, src.sql(None if row_filter == "TRUE" else row_filter))
    rest = _PLACEHOLDER.sub("", text)  # the template, not the SQL: JSON structure literals hold "}}"
    if "{{" in rest or "}}" in rest:
        raise SpecError(f"{table.name}.sql: unparsed placeholder")
    for col, mapping in table.extension_slots.items():
        gen = slot_cols.get(col)
        if gen is None:
            raise SpecError(f"table {table.name}: extension_slots.{col} but the SQL has no typed copy of it")
        unused = set(mapping) - gen.used_slots
        if unused:
            raise SpecError(
                f"table {table.name}: extension_slots.{col}: slot(s) not in the column type: {sorted(unused)}"
            )
    sql = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--")).strip()
    return sql.rstrip(";")


# ---------------------------------------------------------------- the generic renderer (no sql/<table>.sql)

GENERIC_KINDS = (
    "key",
    "record",
    "refs",
    "scalar-path",
    "bare-id",
    "pointer-section",
    "typed-copy",
    "extension-slot",
)
_RECORD_TYPES = ("JSON", "VARCHAR")
_GENERIC_NAME = re.compile(r"[^\s{}=]+")  # a column name a placeholder argument can hold
_TOP_FIELD_PATH = re.compile(r"\$\.\w+(?:\[\*\])*")
_LEAF_PLACEHOLDER = {"scalar-path": "scalar", "bare-id": "bare_id", "pointer-section": "pointer_section"}


def _list_depth(t) -> tuple[int, object]:
    """(list levels, the type below them)."""
    d = 0
    while t.id == "list":
        t = t.children[0][1]
        d += 1
    return d, t


def _leaf_path(table: Table, c: Column) -> tuple[str, str]:
    """(placeholder, path) for a scalar-path, bare-id or pointer-section column. The path gets the implicit
    trailing [*] when the type has one list level more than the path (spec.py's path rules). A path whose [*]s
    are all trailing is a plain path read by the template placeholders: scalar / bare_id / pointer_section, or
    typed for a list of scalars (a typed copy of it is the same rule, with the fast path). Any other takes walk."""
    kind, path = c.derivation.kind, c.derivation.path
    where = f"table {table.name}: column {c.name}"
    d, leaf = _list_depth(_parse_type(c.type))
    k = path.count("[*]")
    if d == k + 1:
        path, k = path + "[*]", k + 1
    if d != k:
        raise SpecError(f"{where}: path {c.derivation.path} has {k} list level(s) ([*]); type {c.type} has {d}")
    if kind == "scalar-path" and leaf.id not in _SCALARS:
        raise SpecError(
            f"{where}: a scalar-path type is a scalar or a list of scalars ({', '.join(sorted(_SCALARS))}), "
            f"not {c.type}"
        )
    if kind != "scalar-path" and leaf.id != "varchar":
        raise SpecError(f"{where}: a {kind} type is VARCHAR or a list of VARCHAR, not {c.type}")
    plain = path.replace("[*]", "")
    if k == 0:
        return _LEAF_PLACEHOLDER[kind], plain
    if path == plain + "[*]" * k and (kind == "scalar-path" or (kind == "bare-id" and k == 1)):
        return ("typed" if kind == "scalar-path" else "bare_id"), plain
    return "walk", path


def _walk_expr(table: Table, c: Column) -> str:
    """{{walk COL}}: the value at a path with [*] steps (spec.py's path rules). The fields before the first [*]
    are one json_extract; from there each [*] must be a list and each field step an object (absent or JSON null
    is absent: [] for a list inside the value, the column's nulls rule at its top, NULL for a scalar). Each leaf
    is read by the column's kind with the macros' own checks."""
    path = c.derivation.path
    t = _parse_type(c.type)
    if _list_depth(t)[0] == path.count("[*]") + 1:
        path += "[*]"
    kind = c.derivation.kind
    ctx = _lit(f"table {table.name}: column {c.name}")
    i = path.index("[*]")
    prefix, steps = path[:i], re.findall(r"\[\*\]|\.\w+", path[i:])
    n = [0]

    def var(p: str) -> str:
        n[0] += 1
        return f"{p}{n[0]}"

    def fail(msg: str, x: str) -> str:
        return f"cgp_fail({ctx}, key, {_lit(msg)} || CAST({x} AS VARCHAR))"

    def leaf(x: str, lt, pos: str) -> str:
        p = _lit(pos)
        if kind == "bare-id":
            return f"cgp_bare_id({x}, {ctx}, key, {p})"
        if kind == "pointer-section":
            return f"regexp_extract(cgp_pointer({x}, {ctx}, key, {p}), '^#/([^/]+)/', 1)"
        if lt.id == "varchar":
            return f"cgp_str({x}, {ctx}, key, {p})"
        return f"CAST(cgp_scalar({x}, {ctx}, key, {p}) AS {lt})"

    def go(x: str, rest: list[str], vt, pos: str, top: bool) -> str:
        if not rest:
            return leaf(x, vt, pos)
        step, rest = rest[0], rest[1:]
        if step == "[*]":
            absent = "NULL" if top and c.nulls == "when-absent" else f"CAST([] AS {vt})"
            e = var("e")
            body = go(e, rest, vt.children[0][1], pos + "[]", False)
            return (
                f"CASE coalesce(json_type({x}), 'NULL') WHEN 'NULL' THEN {absent} "
                f"WHEN 'ARRAY' THEN list_transform(CAST({x} AS JSON[]), lambda {e}: {body}) "
                f"ELSE {fail(f'expected a list at {pos}, got ', x)} END"
            )
        # A field step: only below a [*] (the prefix is the json_extract), so never at the column's top.
        absent = f"CAST([] AS {vt})" if vt.id == "list" else "NULL"
        v = var("v")
        inner = go(v, rest, vt, pos + step, top)
        field = f"json_extract({x}, {_lit('$' + step)})"
        return (
            f"CASE coalesce(json_type({x}), 'NULL') WHEN 'NULL' THEN {absent} "
            f"WHEN 'OBJECT' THEN (list_transform([{field}], lambda {v}: {inner}))[1] "
            f"ELSE {fail(f'expected an object at {pos}, got ', x)} END"
        )

    root = var("v")
    body = go(root, steps, t, prefix[2:], True)
    return f"CAST((list_transform([json_extract(record, {_lit(prefix)})], lambda {root}: {body}))[1] AS {c.type})"


def _generic_columns(table: Table) -> tuple[Table, list[str]]:
    """(the table with each plainly read leaf column's path made plain, its SELECT list lines). Refs columns are
    left out of the lines (render_generic joins them on)."""
    cols, lines = [], []
    for c in table.columns:
        kind, alias = c.derivation.kind, _ident(c.name)
        if kind == "key":
            lines.append(f"key AS {alias}")
        elif kind == "record":
            lines.append(f"record AS {alias}" if c.type == "VARCHAR" else f"CAST(record AS {c.type}) AS {alias}")
        elif kind in _LEAF_PLACEHOLDER:
            placeholder, path = _leaf_path(table, c)
            if placeholder != "walk" and path != c.derivation.path:
                c = replace(c, derivation=replace(c.derivation, path=path))
            lines.append(f"{{{{{placeholder} {c.name}}}}} AS {alias}")
        elif kind in ("typed-copy", "extension-slot"):
            ptr = ",".join(p.replace("[*]", "[]") for p in c.derivation.pointers)
            lines.append(f"{{{{typed {c.name}{' ptr=' + ptr if ptr else ''}}}}} AS {alias}")
        elif kind != "refs":
            raise SpecError(f"table {table.name}: column {c.name}: the generic renderer has no kind {kind!r}")
        cols.append(c)
    return replace(table, columns=tuple(cols)), lines


def generic_template(table: Table) -> tuple[Table, str]:
    """(the table with plain paths, the sql/<table>.sql text the generic renderer renders for one section): the
    same placeholders a hand-written file uses, so a hand-written file of generic columns renders the same SQL."""
    table, lines = _generic_columns(table)
    text = "SELECT\n" + ",\n".join(f"  {line}" for line in lines) + "\nFROM {{source}}\n"
    if table.known_fields:
        text += "WHERE {{known_keys}}\n"
    return table, text


def generic_problems(table: Table) -> list[str]:
    """Why render_generic can't build `table` (empty: it can). Renders the SQL to find out, so a bad type, an
    unknown pointer position or an unused extension slot is found when the spec loads."""
    from .baseline import REFS_TYPE

    out = []
    if len(table.sections) != 1:
        out.append(f"it reads {len(table.sections)} sections; the build reads one section per table")
    if table.row_filter is not None:
        out.append("it has a row filter (only a shipped SQL file implements one)")
    other = [c for c in table.columns if c.derivation.kind not in GENERIC_KINDS]
    if other:
        out.append(
            f"column(s) {', '.join(f'{c.name} ({c.derivation.kind})' for c in other)}: only a shipped SQL file "
            f"implements that kind (the generic renderer builds {', '.join(GENERIC_KINDS)})"
        )
    for c in table.columns:
        kind = c.derivation.kind
        if kind == "key" and c.type != "VARCHAR":
            out.append(f"column {c.name} (key) must be VARCHAR, not {c.type}")
        elif kind == "record" and c.type not in _RECORD_TYPES:
            out.append(f"column {c.name} (record) must be one of {_RECORD_TYPES}, not {c.type}")
        elif kind == "refs":
            if c.type != REFS_TYPE:
                out.append(f"column {c.name} (refs) must be {REFS_TYPE}, not {c.type}")
            if c.nulls != "empty-list":
                out.append(f"column {c.name} (refs) must have nulls: empty-list")
        if not _GENERIC_NAME.fullmatch(c.name):
            out.append(f"column name {c.name!r} holds a space, '{{', '}}' or '='")
        if table.known_fields and kind not in ("key", "record", "refs"):
            if not _TOP_FIELD_PATH.fullmatch(c.derivation.path or ""):
                out.append(
                    f"known_fields needs every column to read a top-level field ($.<field>); column {c.name} "
                    f"reads {c.derivation.path}"
                )
    if not out:
        try:
            render_generic(table)
        except SpecError as e:
            out.append(str(e).removeprefix(f"table {table.name}: "))
    return out


def render_generic(table: Table, fast: bool = True) -> str:
    """The table's SELECT built from its spec alone, under render()'s contract: generic_template, rendered as a
    sql/<table>.sql would be (fast=False: the slow reference). A refs column comes from baseline.refs_ctes,
    joined on the key."""
    from .baseline import REFS_TYPE, refs_ctes

    plain, text = generic_template(table)
    sql = _render_text(plain, text, fast)
    if not table.columns_of("refs"):
        return sql
    key = _ident(table.columns_of("key")[0].name)
    outer = ", ".join(
        f"coalesce(r.refs, CAST([] AS {REFS_TYPE})) AS {_ident(c.name)}"
        if c.derivation.kind == "refs"
        else f"g.{_ident(c.name)}"
        for c in table.columns
    )
    return (
        f"{refs_ctes(table, table.sections[0])}\nSELECT {outer}\nFROM (\n{sql}\n) g LEFT JOIN r ON r.k = g.{key}"
    )
