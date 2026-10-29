r"""Render the SQL of a baseline-style table: `id`, `refs`, `data` per bundle record (ADR 0003).

Contract with build.py (the same as `sqltemplate.render`): `render(table)` returns ONE SELECT over the
`bundle_<section>` tables (columns `key VARCHAR`, `record VARCHAR`), with no trailing `;`, whose output columns
are exactly `table.columns`, in order, with the spec types. No macros are needed.

A table can use this renderer when it has no row filter and every column is one of these derivation kinds
(`can_render`); the column names and their order come from the spec:

- `key`: the bundle key, VARCHAR.
- `record`: the bundle record, byte for byte (`record::JSON` keeps the bytes; JSON or VARCHAR).
- `refs`: every `#/<section>/<id>` pointer string in the record, as `STRUCT(path, section, id)[]`, in document
  order, `[]` (never NULL) when there are none.

`refs` rules (DuckDB 1.5.4, measured 2026-10-26):

- **Which strings.** Every JSON string value (not object keys) whose decoded text starts `#/`. A string that
  starts `#/` but does not match `^#/[^/]+/.` (no section, or no id) stops the query with an error naming the
  table, the record key and the path. `section` is the text between the first two `/`, `id` everything after
  the second `/` (it may hold more `/`).
- **Order.** `json_tree` numbers the nodes of a record in pre-order (its `id` column), which is document order;
  the rows themselves come out in no useful order, so the list is aggregated `ORDER BY` that id. Duplicate
  object keys are kept, each in its place.
- **Path.** The position of the string in the record, from the record root, with no `$`: object keys joined by
  `.`, and every array position written `[]` (`contributions[].contributor`, `members[]`,
  `extensions[].value[].gene`). A key is written bare unless it is empty or holds one of `.` `[` `]` `"` `\`;
  such a key is written as a JSON string (`extensions[]."a.b"`, `x.""`), so every path parses back to one list
  of keys. `json_tree`'s own `fullkey` does not quote keys (`$.a.b` for both `{"a.b":…}` and `{"a":{"b":…}}`),
  so it is used only for records with no such key (the fast path); a record that has one gets its path built
  key by key from `json_tree`'s parent links (a recursive walk, the slow path). No 1.1.0 record seen so far has
  such a key (the real fixture has none), so the slow path is for correctness, not speed.
"""

from __future__ import annotations

from collections.abc import Callable

from .errors import SpecError
from .spec import Table

KINDS = frozenset({"key", "record", "refs"})
REFS_TYPE = "STRUCT(path VARCHAR, section VARCHAR, id VARCHAR)[]"
RECORD_TYPES = ("JSON", "VARCHAR")
# A key that is empty or holds one of these characters is written quoted (as a JSON string) in a refs path.
_ODD_KEY_RE = r'[.\[\]"\\]'


def _lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _ident(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def problems(table: Table) -> list[str]:
    """Why this renderer can't build `table` (empty: it can)."""
    out = []
    if table.row_filter is not None:
        out.append("has a row filter")
    for c in table.columns:
        k = c.derivation.kind
        if k not in KINDS:
            out.append(f"column {c.name} has derivation kind {k!r} (this renderer has {sorted(KINDS)})")
        elif k == "key" and c.type != "VARCHAR":
            out.append(f"column {c.name} (key) must be VARCHAR, not {c.type}")
        elif k == "record" and c.type not in RECORD_TYPES:
            out.append(f"column {c.name} (record) must be one of {RECORD_TYPES}, not {c.type}")
        elif k == "refs":
            if c.type != REFS_TYPE:
                out.append(f"column {c.name} (refs) must be {REFS_TYPE}, not {c.type}")
            if c.derivation.path != "$":
                out.append(f"column {c.name} (refs) must have path $ (the whole record)")
            if c.nulls != "empty-list":
                out.append(f"column {c.name} (refs) must have nulls: empty-list")
    return out


def can_render(table: Table) -> bool:
    return not problems(table)


def renderer_for(table: Table) -> Callable[[Table], str]:
    """The renderer build.py should use for `table`: this module's for key/record/refs tables; else the shipped
    `sql/<table>.sql` (sqltemplate.render) when the table is the clinvar-gkm table of that name
    (spec.builtin_sql_applies); else the generic renderer, which builds the SELECT from the spec's derivation
    kinds alone (sqltemplate.render_generic; spec.check_rules has checked that it can)."""
    if can_render(table):
        return render
    from . import sqltemplate
    from .spec import builtin_sql_applies

    return sqltemplate.render if builtin_sql_applies(table) else sqltemplate.render_generic


def _section_sql(table: Table, section: str) -> str:
    src = _ident(f"bundle_{section}")
    refs_col = next((c for c in table.columns if c.derivation.kind == "refs"), None)
    cols = []
    for c in table.columns:
        k = c.derivation.kind
        if k == "key":
            expr = "b.key"
        elif k == "record":
            expr = f"CAST(b.record AS {c.type})"
        else:
            expr = f"coalesce(r.refs, CAST([] AS {REFS_TYPE}))"
        cols.append(f"{expr} AS {_ident(c.name)}")
    select = ",\n       ".join(cols)
    if refs_col is None:
        return f"SELECT {select}\nFROM {src} b"
    return f"{refs_ctes(table, section)}\nSELECT {select}\nFROM {src} b LEFT JOIN r ON r.k = b.key"


def refs_ctes(table: Table, section: str) -> str:
    """`WITH RECURSIVE ..., r AS (...)`: the CTEs that give `r(k, refs)`, each record's refs list (module
    docstring), for the records of bundle_<section> that hold a pointer. A query that follows them joins `r` on
    the key and takes `coalesce(r.refs, [])` (sqltemplate.render_generic does too)."""
    src = _ident(f"bundle_{section}")
    ctx = _lit(f"table {table.name}: section {section}: record ")
    # nodes: the pointer strings of every record, plus every node whose key would need quoting (to know which
    # records take the slow path). json_tree's `key` of an array element is its index (digits), never odd.
    return f"""WITH RECURSIVE
nodes AS (
    SELECT b.key AS k, j.id, j.fullkey,
           CASE WHEN j.type = 'VARCHAR' THEN json_extract_string(j.atom, '$') END AS str,
           (j.key = '' OR regexp_matches(j.key, {_lit(_ODD_KEY_RE)})) AS odd
    FROM {src} b, json_tree(b.record) j
    WHERE (j.type = 'VARCHAR' AND starts_with(json_extract_string(j.atom, '$'), '#/'))
       OR j.key = '' OR regexp_matches(j.key, {_lit(_ODD_KEY_RE)})
),
odd_records AS (SELECT DISTINCT k FROM nodes WHERE odd),
fast AS (
    SELECT n.k, n.id, n.str,
           regexp_replace(regexp_replace(n.fullkey, '\\[[0-9]+\\]', '[]', 'g'), '^\\$\\.?', '') AS path
    FROM nodes n ANTI JOIN odd_records o ON o.k = n.k
    WHERE n.str IS NOT NULL
),
tree AS (
    SELECT b.key AS k, j.id, j.parent, j.key AS name, j.type,
           CASE WHEN j.type = 'VARCHAR' THEN json_extract_string(j.atom, '$') END AS str
    FROM (SELECT b.key, b.record FROM {src} b SEMI JOIN odd_records o ON o.k = b.key) b, json_tree(b.record) j
),
seg AS (
    SELECT n.k, n.id, n.parent,
           CASE WHEN p.type = 'ARRAY' THEN '[]'
                WHEN n.name = '' OR regexp_matches(n.name, {_lit(_ODD_KEY_RE)})
                     THEN '.' || CAST(to_json(n.name) AS VARCHAR)
                ELSE '.' || n.name END AS seg
    FROM tree n JOIN tree p ON p.k = n.k AND p.id = n.parent
),
walk AS (
    SELECT t.k, t.id AS leaf, t.str, s.parent AS cur, s.seg AS acc
    FROM tree t JOIN seg s ON s.k = t.k AND s.id = t.id
    WHERE starts_with(t.str, '#/')
    UNION ALL
    SELECT w.k, w.leaf, w.str, s.parent, s.seg || w.acc
    FROM walk w JOIN seg s ON s.k = w.k AND s.id = w.cur
),
slow AS (
    SELECT w.k, w.leaf AS id, w.str, regexp_replace(w.acc, '^\\.', '') AS path
    FROM walk w JOIN tree root ON root.k = w.k AND root.id = w.cur AND root.parent IS NULL
),
ptrs AS (SELECT * FROM fast UNION ALL SELECT * FROM slow),
r AS (
    SELECT k, list(
               CASE WHEN regexp_matches(str, '^#/[^/]+/.', 's')
                    THEN struct_pack(path := path,
                                     section := regexp_extract(str, '^#/([^/]+)/', 1),
                                     id := regexp_extract(str, '^#/[^/]+/(.*)$', 1, 's'))
                    ELSE error({ctx} || k || ': malformed pointer ' || to_json(str)::VARCHAR || ' at ' ||
                               CASE WHEN path = '' THEN '(record root)' ELSE path END ||
                               ' (a string that starts #/ must be #/<section>/<id>)')
               END ORDER BY id) AS refs
    FROM ptrs GROUP BY k
)"""


def render(table: Table) -> str:
    """ONE SELECT over bundle_<section> (UNION ALL over the table's sections) giving `table.columns`."""
    bad = problems(table)
    if bad:
        raise SpecError(f"table {table.name}: the baseline renderer can't build it: {'; '.join(bad)}")
    parts = [_section_sql(table, s) for s in table.sections]
    if len(parts) == 1:
        return parts[0]
    return "\nUNION ALL\n".join(f"SELECT * FROM (\n{p}\n)" for p in parts)
