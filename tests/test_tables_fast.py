"""The fast table SQL against the slow reference (sqltemplate.py, {{source}}).

render(table) reads each record once (one json_transform) and checks the row by a census before it trusts the
fast expressions; render(table, fast=False) is the reference: the typed copies alone, as the spec was first
implemented. The two must write the same bytes, take the same rows, and stop with the same message. Each record
here is a real fixture record (current_shape(), see test_tables.py) changed minimally, as the test says.
"""

from __future__ import annotations

import copy
import hashlib
import json

import duckdb
import pytest
from _tables_helpers import SPEC, build, connect, current_shape, read_fixture, rows

from clinvar_gkm_parquet.sqltemplate import render

BASE = current_shape(read_fixture())
CENSUS = [t.name for t in SPEC.tables if " AS _ok FROM " in render(t)]  # the tables whose source takes a census


def _parquet_sha256(con, table, fast, path):
    sql = render(table, fast=fast)
    con.execute(
        f"COPY (SELECT * FROM ({sql}) ORDER BY \"{table.sort_key}\") TO '{path}' "
        f"(FORMAT parquet, COMPRESSION {SPEC.writer['compression']}, "
        f"ROW_GROUP_SIZE {SPEC.writer['row_group_size']})"
    )
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize("table", [t.name for t in SPEC.tables])
def test_fast_writes_the_same_parquet_bytes(table, tmp_path):
    con = connect(BASE)
    t = SPEC.table(table)
    assert _parquet_sha256(con, t, True, tmp_path / "fast.parquet") == _parquet_sha256(
        con, t, False, tmp_path / "slow.parquet"
    )


def _ok(sections, table, key):
    """The census's verdict on one row: True when it takes the fast expressions."""
    sql = render(SPEC.table(table))
    source = sql.split("\nFROM ", 1)[1].split("\nWHERE ", 1)[0]
    return connect(sections).execute(f"SELECT _ok FROM {source} WHERE key = ?", [key]).fetchone()[0]


def test_the_typed_tables_take_a_census():
    assert set(CENSUS) == {
        "variation",
        "condition",
        "conditionSet",
        "evidenceLine",
        "scv",
        "rcv",
        "rcv_evidenceLine",
        "vcv",
        "vcv_evidenceLine",
    }


@pytest.mark.parametrize("table", CENSUS)
def test_the_fixture_takes_the_fast_path(table):
    sql = render(SPEC.table(table))
    source = sql.split("\nFROM ", 1)[1].split("\nWHERE ", 1)[0]
    where = sql.split("\nWHERE ", 1)[1] if "\nWHERE " in sql else "TRUE"
    con = connect(BASE)
    n, slow = con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE NOT _ok) FROM {source} WHERE {where}"
    ).fetchone()
    assert n > 0 and slow == 0


def _mutated(section, pick, change):
    """A copy of the fixture with the first record of `section` for which pick(key, rec) holds changed by
    change(rec) (it may return replacement record text). Returns (sections, key)."""
    sections = copy.deepcopy(BASE)
    for i, (k, text) in enumerate(sections[section]):
        rec = json.loads(text)
        if pick(k, rec):
            out = change(rec)
            sections[section][i] = (k, out if isinstance(out, str) else json.dumps(rec, separators=(",", ":")))
            return sections, k
    raise AssertionError(f"no {section} record to mutate")


def _outcome(sections, table, fast):
    con = connect(sections)
    try:
        return "rows", rows(con, build(con, SPEC.table(table), fast=fast))
    except duckdb.Error as e:
        return "error", f"{type(e).__name__}: {e}"


def _hgvs(rec):
    return next(e for e in rec["extensions"] if e["name"] == "clinvarHgvsList")["value"]


def _scv_condition_set(rec):
    return next(e for e in rec["extensions"] if e["name"] == "submittedConditionSet")


def _set(path, value):
    def change(rec):
        node = rec
        for p in path[:-1]:
            node = node[p]
        node[path[-1]] = value

    return change


def _duplicate_quality(rec):
    # The record's text with its quality object written twice (json.dumps can't write a duplicate key).
    text = json.dumps(rec, separators=(",", ":"))
    q = json.dumps(rec["quality"], separators=(",", ":"))
    return text.replace(f'"quality":{q}', f'"quality":{q},"quality":{{"type":"MappableConcept","name":"x"}}', 1)


def _has(*path):
    def pick(k, rec):
        node = rec
        for p in path:
            if not isinstance(node, (dict, list)) or (isinstance(node, dict) and p not in node):
                return False
            node = node[p]
        return True

    return pick


MUTATIONS = {
    # a field the spec type lacks, deep inside a name-dependent extension value
    "unknown field in an hgvs element": (
        "variation",
        lambda k, rec: any(e["name"] == "clinvarHgvsList" for e in rec.get("extensions", [])),
        lambda rec: _hgvs(rec)[0].__setitem__("addedField", "x"),
    ),
    "array where a struct goes": ("scv", _has("quality"), _set(["quality"], [])),
    "scalar where a struct goes": ("scv", _has("quality"), _set(["quality"], "x")),
    "empty object where a scalar goes": (
        "scv",
        _has("classification", "name"),
        _set(["classification", "name"], {}),
    ),
    "empty array where a scalar goes": (
        "scv",
        _has("classification", "name"),
        _set(["classification", "name"], []),
    ),
    "object where a list goes": ("scv", _has("contributions"), _set(["contributions"], {})),
    "scalar where a list goes": ("scv", _has("contributions"), _set(["contributions"], "x")),
    "object for a top-level scalar": ("scv", _has("type"), _set(["type"], {"x": 1})),
    "number for a pointer": ("variation", _has("members", 0), _set(["members", 0], 5)),
    "pointer of another shape": ("scv", _has("proposition"), _set(["proposition"], "#//x")),
    "string for a nested extension value": (
        "scv",
        lambda k, rec: any(e["name"] == "submittedConditionSet" for e in rec.get("extensions", [])),
        lambda rec: _scv_condition_set(rec).__setitem__("value", "x"),
    ),
    "extension without a name": (
        "scv",
        _has("classification", "extensions", 0),
        lambda rec: rec["classification"]["extensions"][0].pop("name") and None,
    ),
    "top-level field no column covers": ("vcv", lambda k, rec: True, _set(["addedField"], "x")),
    # no error: the slow expressions take JSON null as absent and the first of two equal keys
    "JSON null": ("scv", _has("strength"), _set(["strength"], None)),
    "duplicate key": ("scv", _has("quality"), _duplicate_quality),
}


@pytest.mark.parametrize("case", list(MUTATIONS))
def test_the_census_turns_away_what_the_fast_path_would_miss(case):
    table, pick, change = MUTATIONS[case]
    sections, key = _mutated(table, pick, change)
    assert _ok(sections, table, key) is False
    fast, slow = _outcome(sections, table, True), _outcome(sections, table, False)
    assert fast == slow
    if case in ("JSON null", "duplicate key"):
        assert fast[0] == "rows"
    else:
        assert fast[0] == "error" and f"key {key}" in fast[1], fast


def test_a_check_the_census_leaves_to_the_fast_path_stops_the_same_way():
    # An extension of unknown name with no value: every node is caught, so the row is fast, and its fast
    # expression stops with the slow generator's message.
    def change(rec):
        rec["classification"]["extensions"] = [{"name": "noSuchName"}]

    sections, key = _mutated("scv", _has("classification"), change)
    assert _ok(sections, "scv", key) is True
    fast, slow = _outcome(sections, "scv", True), _outcome(sections, "scv", False)
    assert fast == slow and fast[0] == "error" and "unknown extension name at .extensions[]: noSuchName" in fast[1]


def test_duckdb_behaviour_the_census_relies_on():
    """json_transform (DuckDB 1.5.4, not strict) catches a mistyped value as NULL, a struct of NULLs, or its JSON
    text; json_array_length over $.** lists every node but the root, null and repeated keys too; the pointer
    strips equal the regexes for cgp_pointer's shape."""
    con = duckdb.connect()
    one = con.execute
    assert one("""SELECT json_transform('[{"a":1}, 5, [1], null]', '[{"a":"VARCHAR"}]')""").fetchone()[0] == [
        {"a": "1"},
        {"a": None},
        {"a": None},
        None,
    ]
    assert one("""SELECT json_transform('{"x":{}, "y":5}', '{"x":["VARCHAR"], "y":["VARCHAR"]}')""").fetchone()[
        0
    ] == {
        "x": None,
        "y": None,
    }
    assert one("""SELECT json_transform('{"x":{}, "y":[]}', '{"x":"VARCHAR", "y":"JSON"}')""").fetchone()[0] == {
        "x": "{}",
        "y": "[]",
    }
    assert one("""SELECT len(json_array_length('{"a":{"b":null,"c":[1,{}]},"a":2}', '$.**'))""").fetchone()[0] == 6
    for p in ("#/a/b", "#/sec/x/y", "#/é/ü/z", "#/a//b"):
        bare, section = one(
            "SELECT substr(?, 3 + strpos(substr(?, 3), '/')) = regexp_replace(?, '^#/[^/]+/', ''), "
            "substr(?, 3, strpos(substr(?, 3), '/') - 1) = regexp_extract(?, '^#/([^/]+)/', 1)",
            [p] * 6,
        ).fetchone()
        assert bare and section, p
