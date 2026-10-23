"""Render a table's SQL from sql/<table>.sql and the spec (design plan §3.1).

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

Typed copy (derivation kinds typed-copy, extension-slot and list bare-id) walks the spec type: every JSON object
must be an object, every list a list, and an object may hold only the fields its spec struct has, so a bundle
field the spec type lacks stops the build instead of being dropped (DuckDB's json_transform ignores unknown keys,
and CAST(JSON AS STRUCT) also rejects absent ones, so neither is used on its own). A struct with a `name` field
and `value_*` fields is an extension: the bundle's `{name, value}` is put in the slot that the table's
`extension_slots` map gives for that name, and a name the map doesn't list for that struct stops the build.
Empty versus NULL inside typed values (design plan §3.5): an absent struct or scalar is NULL; an absent list is
`[]` (BigQuery stores no NULL arrays) when its parent is present, and at the top level follows the column's
`nulls` rule.
"""

from __future__ import annotations

import json
import re
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
        known_sql = "[" + ", ".join(_lit(n) for n in known) + "]"
        msg = _lit(f"unknown extension name at {rel or '(column)'}: ") + f" || coalesce({name_sql}, '(none)')"
        unknown = f"NOT list_contains({known_sql}, coalesce({name_sql}, ''))"
        return f"CASE WHEN {unknown} THEN {self.fail(msg)} ELSE {packed} END"


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


def _col(table: Table, name: str) -> Column:
    return table.column(name)


def _path(table: Table, c: Column) -> str:
    if not c.derivation.path:
        raise SpecError(f"table {table.name}: column {c.name}: placeholder needs a derivation path")
    return _lit(c.derivation.path)


def _known_keys(table: Table) -> str:
    keys = {"id"}
    for c in table.columns:
        p = c.derivation.path
        if p and c.derivation.kind != "computed":
            m = re.fullmatch(r"\$\.(\w+)", p)
            if not m:
                raise SpecError(f"table {table.name}: column {c.name}: known_keys needs a top-level path, got {p}")
            keys.add(m.group(1))
    allowed = "[" + ", ".join(_lit(k) for k in sorted(keys)) + "]"
    return f"cgp_known_keys(record, {allowed}, {_lit('table ' + table.name)}, key)"


def render(table: Table) -> str:
    text = _sql_text(f"{table.name}.sql")
    slot_cols: dict[str, _Gen] = {}

    def sub(m: re.Match) -> str:
        name, args = m.group(1), m.group(2).split()
        opts = dict(a.split("=", 1) for a in args if "=" in a)
        pos = [a for a in args if "=" not in a]
        if name == "known_keys":
            return _known_keys(table)
        if len(pos) != 1:
            raise SpecError(f"{table.name}.sql: {m.group(0)}: expected one column argument")
        c = _col(table, pos[0])
        ctx = _lit(f"table {table.name}: column {c.name}")
        if name == "type":
            return c.type
        if name == "ctx":
            return ctx
        if name == "scalar":
            t = _parse_type(c.type)
            if t.id == "varchar":
                return f"cgp_str(json_extract(record, {_path(table, c)}), {ctx}, key, '(column)')"
            return f"CAST(cgp_scalar(json_extract(record, {_path(table, c)}), {ctx}, key, '(column)') AS {c.type})"
        if name == "pointer_section":
            return f"cgp_pointer_section(json_extract(record, {_path(table, c)}), {ctx}, key)"
        if name == "bare_id":
            t = _parse_type(c.type)
            ptr = {""} if t.id == "varchar" else {"[]"}
            expr, _ = typed_expr(table, c, ptr)
            return expr
        if name == "typed":
            ptr = set(opts.get("ptr", "").split(",")) - {""}
            expr, gen = typed_expr(table, c, ptr)
            if c.name in table.extension_slots:
                slot_cols[c.name] = gen
            return expr
        raise SpecError(f"{table.name}.sql: unknown placeholder {m.group(0)}")

    sql = _PLACEHOLDER.sub(sub, text)
    if "{{" in sql or "}}" in sql:
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
