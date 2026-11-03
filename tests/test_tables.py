"""Every spec table built on the bundle fixture: DESCRIBE equals the spec, and every value equals what the
spec's derivation gives when computed here, in Python, from the bundle record JSON (not from the SQL).
"""

from __future__ import annotations

import datetime
import json
import re

import duckdb
import pytest
from _tables_helpers import SPEC, build, connect, describe, read_fixture, rows

from clinvar_gkm_parquet.sqltemplate import canonical_type, render

RECORDS = read_fixture()
TABLES = [t.name for t in SPEC.tables]
# Pointer positions the typed copies strip (as in the SQL files' ptr=; bare-id columns are the whole value).
POINTERS = {
    ("variation", "extensions"): {"[].value_gene_array[].gene"},
    ("variation", "constraints"): {"[].allele", "[].location"},
    ("scv", "contributions"): {"[].contributor"},
}
EVIDENCE_LINE_SHAPES = {
    "evidenceLine": r"clinvar\.submission:SCV\d+\.\d+",
    "vcv_evidenceLine": r"VCV\d+\.\d+-.*",
    "rcv_evidenceLine": r"RCV\d+\.\d+-.*",
}


@pytest.fixture(scope="module")
def con():
    c = connect(RECORDS)
    for t in SPEC.tables:
        build(c, t)
    return c


# ---------------------------------------------------------------- expected values, from the record JSON


def _get(rec, path):
    assert path.startswith("$.")
    v = rec
    for part in path[2:].split("."):
        if not isinstance(v, dict):
            return None
        v = v.get(part)
    return v


def _strip(p):
    assert isinstance(p, str) and re.match(r"^#/[^/]+/.", p), p
    return re.sub(r"^#/[^/]+/", "", p)


def _typed(v, t, rel, slots, pointers, top_nulls=None):
    if t.id == "list":
        if v is None:
            return None if top_nulls == "when-absent" else []
        return [_typed(e, t.children[0][1], rel + "[]", slots, pointers) for e in v]
    if v is None:
        return None
    if t.id == "struct":
        names = [f for f, _ in t.children]
        is_ext = "name" in names and any(f.startswith("value_") for f in names)
        allowed = {f for f in names if not (is_ext and f.startswith("value_"))} | ({"value"} if is_ext else set())
        assert set(v) <= allowed, (rel, set(v) - allowed)
        out = {}
        for f, ft in t.children:
            if is_ext and f.startswith("value_"):
                hit = slots[v["name"]] == f
                out[f] = _typed(v.get("value") if hit else None, ft, f"{rel}.{f}", slots, pointers)
            else:
                out[f] = _typed(v.get(f), ft, f"{rel}.{f}", slots, pointers)
        return out
    if rel in pointers:
        return _strip(v)
    return {
        "varchar": str,
        "bigint": int,
        "boolean": bool,
        "date": datetime.date.fromisoformat,
    }[t.id](v)


def _computed(table, col, rec):
    if table == "location":
        start, end = rec.get("start"), rec.get("end")

        def bound(x, i):
            return x[i] if isinstance(x, list) else x

        return {
            "loc_start": None if isinstance(start, list) else start,
            "loc_end": None if isinstance(end, list) else end,
            "loc_start_min": bound(start, 0),
            "loc_start_max": bound(start, 1),
            "loc_end_min": bound(end, 0),
            "loc_end_max": bound(end, 1),
        }[col]
    if table.startswith("proposition"):
        gcq = rec.get("geneContextQualifier") or {}
        exts = gcq.get("extensions", [])
        if col == "gene_context_id":
            code = (gcq.get("primaryCoding") or {}).get("code")
            return None if code is None else f"ncbigene:{code}"
        if col == "submitted_gene_symbols":
            return [s for e in exts if e["name"] == "submittedGeneSymbols" for s in e["value"]]
        if col == "gene_context_source":
            vals = [e["value"] for e in exts if e["name"] == "geneContextSource"]
            assert len(vals) <= 1
            return vals[0] if vals else None
    raise AssertionError(f"no expectation for computed column {table}.{col}")


def expected_row(table, key, text):
    rec = json.loads(text)
    out = {}
    for c in table.columns:
        d = c.derivation
        t = duckdb.sqltype(c.type)
        if d.kind == "key":
            v = key
        elif d.kind == "record":
            v = text
        elif d.kind == "scalar-path":
            v = _get(rec, d.path)
            v = None if v is None else (int(v) if t.id == "bigint" else str(v))
        elif d.kind == "bare-id":
            v = _get(rec, d.path)
            if t.id == "list":
                v = [] if v is None else [_strip(p) for p in v]
            else:
                v = None if v is None else _strip(v)
        elif d.kind == "pointer-section":
            v = _get(rec, d.path)
            v = None if v is None else re.match(r"^#/([^/]+)/", v).group(1)
        elif d.kind in ("typed-copy", "extension-slot"):
            v = _typed(
                _get(rec, d.path),
                t,
                "",
                table.extension_slots.get(c.name, {}),
                POINTERS.get((table.name, c.name), set()),
                top_nulls=c.nulls,
            )
        else:
            v = _computed(table.name, c.name, rec)
        out[c.name] = v
    return out


def source_rows(table):
    (section,) = table.sections
    recs = RECORDS[section]
    if table.name in EVIDENCE_LINE_SHAPES:
        recs = [(k, r) for k, r in recs if re.fullmatch(EVIDENCE_LINE_SHAPES[table.name], k)]
    return recs


# ---------------------------------------------------------------- tests


def test_spec_types_are_canonical():
    """Each spec type is written exactly as DuckDB prints it, so DESCRIBE can be compared as text."""
    for t in SPEC.tables:
        for c in t.columns:
            assert canonical_type(c.type) == c.type, (t.name, c.name)


def test_spec_has_22_tables():
    assert len(TABLES) == 22


@pytest.mark.parametrize("name", TABLES)
def test_describe_equals_spec(con, name):
    table = SPEC.table(name)
    assert describe(con, f"out_{name}") == table.describe()


@pytest.mark.parametrize("name", TABLES)
def test_values_follow_the_derivations(con, name):
    table = SPEC.table(name)
    got = {r[table.sort_key]: r for r in rows(con, f"out_{name}")}
    want = {k: expected_row(table, k, text) for k, text in source_rows(table)}
    assert got.keys() == want.keys()
    for k in want:
        assert got[k] == want[k], k


def test_evidence_line_split_covers_the_section(con):
    n = sum(con.execute(f'SELECT count(*) FROM "out_{t}"').fetchone()[0] for t in EVIDENCE_LINE_SHAPES)
    assert n == len(RECORDS["evidenceLine"])
    assert n > 0 and all(source_rows(SPEC.table(t)) for t in EVIDENCE_LINE_SHAPES)


def test_fixture_exercises_the_typed_paths(con):
    """Guard against a vacuous value test: the fixture fills the slot and pointer paths it is meant to check."""
    (ext,) = con.execute(
        "SELECT list_filter(extensions, lambda e: e.name = 'clinvarGeneList') FROM out_variation "
        "WHERE len(list_filter(extensions, lambda e: e.name = 'clinvarGeneList')) > 0 LIMIT 1"
    ).fetchone()
    assert ext[0]["value_gene_array"][0]["gene"].startswith("ncbigene:")
    assert ext[0]["value_string"] is None and ext[0]["value_hgvs_array"] == []
    (contribs,) = con.execute("SELECT contributions FROM out_scv LIMIT 1").fetchone()
    assert isinstance(contribs[0]["date"], datetime.date)
    assert contribs[0]["contributor"].startswith("clinvar.submitter:")
    slots = con.execute(
        "SELECT DISTINCT e.name FROM (SELECT unnest(extensions) AS e FROM out_scv) "
        "WHERE e.value_submitted_condition IS NOT NULL"
    ).fetchall()
    assert slots == [("submittedCondition",)]


@pytest.mark.parametrize("name", sorted(TABLES))
def test_fixture_builds_each_table_alone(name):
    c = connect(RECORDS)
    build(c, SPEC.table(name))


def test_render_is_one_select():
    for t in SPEC.tables:
        sql = render(t)
        assert sql.lstrip().upper().startswith("SELECT"), t.name
        assert "{{" not in sql and ";" not in sql.replace("';'", "")
