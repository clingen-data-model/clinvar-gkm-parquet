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
"""

from __future__ import annotations

from .spec import Table


def macros_sql() -> str:
    raise NotImplementedError


def render(table: Table) -> str:
    raise NotImplementedError
