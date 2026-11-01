"""The baseline spec and renderer (ADR 0003) on the real 1.1.0 excerpt.

Expectations come from an independent Python walk of each parsed record (not from the SQL under test), and
`data` is compared byte for byte with the bundle line. The fixture is read with a small stdlib loader here (the
bundle line format of design plan §4.2), so these tests share no helper with the table-SQL tests.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from pathlib import Path

import duckdb
import pytest

from clinvar_gkm_parquet import baseline, build, sqltemplate
from clinvar_gkm_parquet.compat import SCHEMA_SECTIONS
from clinvar_gkm_parquet.spec import resolve_spec

FIXTURE = Path(__file__).parent / "fixtures" / "real-1.1.0" / "clinvar-gkm_excerpt.json.gz"
SECTIONS = SCHEMA_SECTIONS["1.1.0"]
SPEC, SOURCE = resolve_spec("baseline")
_SECTION = re.compile(r'^  "(\w+)": \{$')
_RECORD = re.compile(r'^    ("(?:[^"\\]|\\.)*"): (.*?),?$')


def read_fixture(path: Path = FIXTURE) -> dict[str, list[tuple[str, str]]]:
    """{section: [(key, record text)]} in bundle order; every 1.1.0 section, empty when the excerpt has none."""
    out: dict[str, list[tuple[str, str]]] = {s: [] for s in SECTIONS}
    section = None
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if m := _SECTION.match(line):
                section = m.group(1)
                assert section in out, f"unknown section {section}"
            elif section and (m := _RECORD.match(line)):
                out[section].append((json.loads(m.group(1)), m.group(2)))
            elif line.strip() in ("}", "},"):
                section = None
    return out


BUNDLE = read_fixture()


def connect(sections: dict[str, list[tuple[str, str]]]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    for s, rows in sections.items():
        con.execute(f'CREATE TABLE "bundle_{s}" (key VARCHAR, record VARCHAR)')
        if rows:
            con.executemany(f'INSERT INTO "bundle_{s}" VALUES (?, ?)', rows)
    return con


# --- the independent recompute ---------------------------------------------------------------------------------

_ODD = re.compile(r'[.\[\]"\\]')


def _seg(key: str) -> str:
    return json.dumps(key, ensure_ascii=False) if key == "" or _ODD.search(key) else key


class _Obj(list):
    """A JSON object as its (key, value) pairs in text order, so duplicate keys stay in place."""


def expected_refs(record_text: str) -> list[dict]:
    """Every string starting #/ in document order, with its normalised path."""
    out: list[dict] = []

    def walk(v, path: str) -> None:
        if isinstance(v, _Obj):
            for k, x in v:
                walk(x, f"{path}.{_seg(k)}" if path else _seg(k))
        elif isinstance(v, list):
            for x in v:
                walk(x, path + "[]")
        elif isinstance(v, str) and v.startswith("#/"):
            m = re.fullmatch(r"#/([^/]+)/(.+)", v, re.S)
            assert m, f"malformed pointer {v!r} in the fixture"
            out.append({"path": path, "section": m[1], "id": m[2]})

    walk(json.loads(record_text, object_pairs_hook=_Obj), "")
    return out


def built(con, table) -> list[tuple[str, list[dict], str, str]]:
    """(id, refs, data as text, typeof(data)) rows of the rendered SELECT, by id."""
    sql = baseline.render(table)
    return con.execute(f"SELECT id, refs, CAST(data AS VARCHAR), typeof(data) FROM ({sql}) ORDER BY id").fetchall()


# --- every section ---------------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def con():
    c = connect(BUNDLE)
    yield c
    c.close()


def test_baseline_spec_shape():
    assert (SOURCE.name, SOURCE.version, SOURCE.builtin) == ("baseline", "1.0", True)
    assert [t.name for t in SPEC.tables] == list(SECTIONS)
    for t in SPEC.tables:
        assert t.sections == (t.name,) and t.row_filter is None and t.sort_key == "id"
        assert t.describe() == [("id", "VARCHAR"), ("refs", baseline.REFS_TYPE), ("data", "JSON")]
        assert baseline.renderer_for(t) is baseline.render
    assert SPEC.writer == {"compression": "snappy", "row_group_size": 122880}


@pytest.mark.parametrize("section", SECTIONS)
def test_section(con, section):
    table = SPEC.table(section)
    sql = baseline.render(table)
    assert not sql.rstrip().endswith(";")
    describe = [(r[0], r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM ({sql})").fetchall()]
    assert describe == table.describe()
    rows = built(con, table)
    records = sorted(BUNDLE[section])
    assert [r[0] for r in rows] == [k for k, _ in records], "one row per bundle key"
    for (key, refs, data, typ), (_, record) in zip(rows, records, strict=True):
        assert data == record, f"{section} {key}: data is not the bundle record byte for byte"
        assert typ == "JSON"
        assert refs == expected_refs(record), f"{section} {key}: refs differ from the recompute"


def test_fixture_covers_the_shapes(con):
    """The excerpt exercises every kind of position the issue names; if a new excerpt lost one, this says so."""
    seen: dict[tuple[str, str], set[str]] = {}
    for t in SPEC.tables:
        for _, refs, _, _ in built(con, t):
            for r in refs:
                seen.setdefault((t.name, r["path"]), set()).add(r["section"])
    assert seen[("allele", "location")] == {"location"}  # a single field
    assert seen[("vcv", "hasEvidenceLines[]")] == {"evidenceLine"}  # a list
    assert seen[("scv", "contributions[].contributor")] == {"submitter"}  # a field inside list elements
    assert seen[("variation", "extensions[].value[].gene")] == {"gene"}  # deep, under an extension
    assert len(seen[("variation", "members[]")]) > 1  # polymorphic list
    assert len(seen[("propositionPathogenicity", "object")]) > 1  # polymorphic field
    assert len(seen[("scv", "proposition")]) > 1


def test_record_without_pointers_has_empty_refs(con):
    rows = [r for t in SPEC.tables for r in built(con, t) if not expected_refs(r[2])]
    assert rows, "the excerpt has no record without pointers"
    assert all(r[1] == [] for r in rows)


def test_refs_document_order_within_a_record(con):
    """A variation with several members: the list follows the record text, not section or id order."""
    rows = built(con, SPEC.table("variation"))
    multi = [r for r in rows if len(r[1]) > 2]
    assert multi
    for _, refs, data, _ in multi:
        start = 0
        for r in refs:  # each pointer is found in the text after the previous one (str.index raises if not)
            start = data.index(json.dumps(f"#/{r['section']}/{r['id']}"), start) + 1


# --- one record changed ----------------------------------------------------------------------------------------


def _one_changed(section: str, key_pred, change) -> tuple[dict, str, str]:
    rows = list(BUNDLE[section])
    i = next(i for i, (k, rec) in enumerate(rows) if key_pred(k, rec))
    k, rec = rows[i]
    new = change(rec)
    assert new != rec
    rows[i] = (k, new)
    return {**BUNDLE, section: rows}, k, new


@pytest.mark.parametrize("bad", ["#/allele", "#/allele/", "#//x", "#/"])
def test_malformed_pointer_fails_naming_key_and_path(bad):
    sections, key, _ = _one_changed(
        "variation",
        lambda k, rec: '"members":["#/allele/' in rec,
        lambda rec: rec.replace('"members":["#/allele/', f'"members":[{json.dumps(bad)},"#/allele/', 1),
    )
    c = connect(sections)
    with pytest.raises(duckdb.Error) as e:
        built(c, SPEC.table("variation"))
    msg = str(e.value)
    assert "table variation" in msg and key in msg and "members[]" in msg and "malformed pointer" in msg


def test_quoted_keys_take_the_slow_path():
    """A key with '.' (and one with '"') on a pointer's path is written as a JSON string; the other records of
    the table, which have no such key, keep the fast path. Expected paths come from the Python walk."""
    sections, key, new = _one_changed(
        "scv",
        lambda k, rec: '"contributions":[' in rec and '"contributor":"#/submitter/' in rec,
        lambda rec: rec.replace('"contributor":', '"contri.butor":', 1).replace(
            '"proposition":', '"pro\\"position":', 1
        ),
    )
    want = expected_refs(new)
    paths = {r["path"] for r in want}
    assert 'contributions[]."contri.butor"' in paths and '"pro\\"position"' in paths
    c = connect(sections)
    rows = {r[0]: r for r in built(c, SPEC.table("scv"))}
    assert rows[key][1] == want and rows[key][2] == new
    for k, rec in sections["scv"]:
        assert rows[k][1] == expected_refs(rec)


def test_renderer_for_clinvar_gkm_tables_is_the_template():
    spec, _ = resolve_spec("clinvar-gkm")
    for t in spec.tables:
        assert baseline.renderer_for(t) is sqltemplate.render


# --- Parquet -------------------------------------------------------------------------------------------------


def test_parquet_round_trip(con, tmp_path):
    """COPY with spec baseline 1.0's writer settings, read back: JSON stays a distinct type (Parquet logical
    type JSON), data keeps its bytes, refs keeps its type and values."""
    table = SPEC.table("scv")
    out = tmp_path / table.file_name
    w = SPEC.writer
    con.execute(
        f"COPY (SELECT * FROM ({baseline.render(table)}) ORDER BY id) TO '{out}' "
        f"(FORMAT parquet, COMPRESSION {w['compression']}, ROW_GROUP_SIZE {w['row_group_size']})"
    )
    describe = [(r[0], r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{out}')").fetchall()]
    assert describe == table.describe()
    logical = dict(
        con.execute(f"SELECT name, converted_type FROM parquet_schema('{out}') WHERE name = 'data'").fetchall()
    )
    assert logical == {"data": "JSON"}
    back = con.execute(f"SELECT id, refs, CAST(data AS VARCHAR) FROM read_parquet('{out}') ORDER BY id").fetchall()
    assert back == [(k, refs, data) for k, refs, data, _ in built(con, table)]
    assert [d for _, _, d in back] == [rec for _, rec in sorted(BUNDLE["scv"])]


def _kv() -> dict[str, str]:
    return {
        **build.kv_metadata(
            table_spec_version=SPEC.spec_version,
            bundle_schema_version="1.1.0",
            source_bundle_sha256="0" * 64,
            source_deltas=[],
            source_release="2026-10-18",
        )
    }


def test_build_tables_self_checks_pass_with_baseline(tmp_path):
    """build.build_tables (unchanged) writes and self-checks every baseline table with this renderer: DESCRIBE,
    a unique sort key, and the (id, data) fingerprint against the bundle's (key, record), with data typed JSON.
    Two builds give byte-identical files."""
    names = [t.name for t in SPEC.tables]
    shas = []
    for run in ("a", "b"):
        c = connect(BUNDLE)
        results = build.build_tables(c, SPEC, names, tmp_path / run, _kv(), render=baseline.render)
        c.close()
        assert {r.table: r.rows for r in results} == {s: len(BUNDLE[s]) for s in SECTIONS}
        shas.append({r.table: r.sha256 for r in results})
    assert shas[0] == shas[1]
    for name in names:
        p = tmp_path / "a" / f"{name}.parquet"
        assert hashlib.sha256(p.read_bytes()).hexdigest() == shas[0][name]
