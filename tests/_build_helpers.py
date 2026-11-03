"""A small spec, a render function and `bundle_<section>` tables for testing the table stage and verify.

These are tool-level test data for the build machinery (COPY, self-check, metadata, comparisons), not bundle
records: they test the stage contracts, not the table SQL. Real-bundle fixtures test the SQL
(tests/fixtures/, .claude/rules/python.md).
"""

from __future__ import annotations

import json

import duckdb

from clinvar_gkm_parquet.bundle import SECTIONS
from clinvar_gkm_parquet.spec import Spec, Table, parse_spec


def _col(name, type_, kind, path=None, nulls="when-absent"):
    return {
        "name": name,
        "type": type_,
        "nulls": nulls,
        "derivation": {"kind": kind, "path": path, "transform": None},
        "description": "",
    }


INFO_TYPE = "STRUCT(a VARCHAR, b BIGINT[])"


def raw_spec() -> dict:
    return {
        "spec_version": "9.9",
        "status": "draft",
        "bundle_schema_versions": ["1.1.0"],
        "sources": [],
        "writer": {"compression": "snappy", "row_group_size": 2048},
        "tables": [
            {
                "name": "thing",
                "sections": ["thing"],
                "row_filter": None,
                "sort_key": "id",
                "description": "",
                "columns": [
                    _col("id", "VARCHAR", "key", nulls="never"),
                    _col("name", "VARCHAR", "scalar-path", "$.name"),
                    _col("n", "BIGINT", "scalar-path", "$.n"),
                    _col("tags", "VARCHAR[]", "typed-copy", "$.tags", nulls="empty-list"),
                    _col("info", INFO_TYPE, "typed-copy", "$.info"),
                    _col("data", "VARCHAR", "record", nulls="never"),
                ],
            },
            {
                "name": "kv",
                "sections": ["kv"],
                "row_filter": None,
                "sort_key": "key",
                "description": "",
                "columns": [
                    _col("key", "VARCHAR", "key", nulls="never"),
                    _col("value", "VARCHAR", "record", nulls="never"),
                    _col("name", "VARCHAR", "scalar-path", "$.name"),
                ],
            },
            {
                "name": "evA",
                "sections": ["ev"],
                "row_filter": "ids starting A",
                "sort_key": "id",
                "description": "",
                "columns": [_col("id", "VARCHAR", "key", nulls="never"), _col("data", "VARCHAR", "record")],
            },
            {
                "name": "evB",
                "sections": ["ev"],
                "row_filter": "ids starting B",
                "sort_key": "id",
                "description": "",
                "columns": [_col("id", "VARCHAR", "key", nulls="never"), _col("data", "VARCHAR", "record")],
            },
        ],
    }


def make_spec() -> Spec:
    return parse_spec(raw_spec(), where="test spec")


SQL = {
    "thing": f"""
        SELECT key AS id,
               json_extract_string(record, '$.name') AS name,
               CAST(json_extract(record, '$.n') AS BIGINT) AS n,
               coalesce(CAST(json_extract(record, '$.tags') AS VARCHAR[]), []::VARCHAR[]) AS tags,
               CAST(json_extract(record, '$.info') AS {INFO_TYPE}) AS info,
               record AS data
        FROM bundle_thing""",
    "kv": "SELECT key, record AS value, json_extract_string(record, '$.name') AS name FROM bundle_kv",
    "evA": "SELECT key AS id, record AS data FROM bundle_ev WHERE key LIKE 'A%'",
    "evB": "SELECT key AS id, record AS data FROM bundle_ev WHERE key LIKE 'B%'",
}


def render(table: Table) -> str:
    return SQL[table.name]


def render_with(**overrides):
    def _render(table: Table) -> str:
        return overrides.get(table.name, SQL[table.name])

    return _render


def thing_record(i: int) -> str:
    rec = {"id": f"T{i:04d}", "name": f"thing {i}", "n": i}
    if i % 3:
        rec["tags"] = [f"t{i}", "x"]
    if i % 2:
        rec["info"] = {"a": f"a{i}", "b": [i, i + 1]}
    return json.dumps(rec, separators=(",", ":"))


def section_rows(n_thing: int = 30) -> dict[str, list[tuple[str, str]]]:
    things = [(f"T{i:04d}", thing_record(i)) for i in range(n_thing)]
    kv = [(f"K{i}", json.dumps({"name": f"kv {i}"}, separators=(",", ":"))) for i in range(5)]
    ev = [(f"{p}{i:03d}", json.dumps({"id": f"{p}{i:03d}", "w": i})) for p in "AB" for i in range(7)]
    return {"thing": things, "kv": kv, "ev": ev}


def load_sections(con: duckdb.DuckDBPyConnection, rows: dict[str, list[tuple[str, str]]]) -> dict[str, int]:
    for section, pairs in rows.items():
        con.execute(f'CREATE OR REPLACE TABLE "bundle_{section}" (key VARCHAR, record VARCHAR)')
        if pairs:
            con.executemany(f'INSERT INTO "bundle_{section}" VALUES (?, ?)', pairs)
    return {s: len(p) for s, p in rows.items()}


def make_con(rows=None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    load_sections(con, rows if rows is not None else section_rows())
    return con


KV = {
    "tool_version": "0.0.0-test",
    "table_spec_version": "9.9",
    "bundle_schema_version": "1.1.0",
    "source_bundle_sha256": "ab" * 32,
    "source_deltas": "[]",
    "source_release": "2026-10-18",
    "duckdb_version": duckdb.__version__,
}


def passthrough_spec(drop: tuple[str, ...] = ()):
    """One table per bundle section: id = key, data = record. Exercises the real reader, replay, pipeline,
    build and verify on a real bundle without depending on the table SQL (tests/test_tables*.py)."""
    col = lambda n, k: {  # noqa: E731
        "name": n,
        "type": "VARCHAR",
        "nulls": "never",
        "derivation": {"kind": k, "path": None, "transform": None},
        "description": "",
    }
    raw = {
        "spec_version": "9.9",
        "status": "draft",
        "bundle_schema_versions": ["1.1.0"],
        "sources": [],
        "writer": {"compression": "snappy", "row_group_size": 122880},
        "tables": [
            {
                "name": s,
                "sections": [s],
                "row_filter": None,
                "sort_key": "id",
                "description": "",
                "columns": [col("id", "key"), col("data", "record")],
            }
            for s in SECTIONS
            if s not in drop
        ],
    }
    return parse_spec(raw)


def passthrough_sql(t) -> str:
    return f'SELECT key AS id, record AS data FROM "bundle_{t.name}"'
