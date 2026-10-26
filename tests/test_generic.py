"""The generic renderer (sqltemplate.render_generic): a table's SELECT built from its spec's derivation kinds
alone, for any table the tool ships no sql/<table>.sql for (ADR 0003, spec.py's path rules).

Two kinds of evidence:

- **One source of truth.** Every clinvar-gkm 1.0 table whose columns are all generic kinds and that has no row
  filter renders, through the generic renderer, to exactly the SQL its hand-written file renders (fast and slow);
  the three evidence-line tables, whose row filter only a SQL file implements, have the generic SELECT list. The
  spec's `pointers` and `known_fields` agree with every shipped file's `ptr=` and `{{known_keys}}`.
- **A custom typed spec on the real excerpt** (tests/fixtures/custom-spec/tables.yaml): built through the CLI,
  checked value by value against the bundle records parsed here with stdlib json, verified, used as the driver
  spec of a selection, and the fail-loud cases, at load time and at build time.
"""

from __future__ import annotations

import copy
import dataclasses
import json
import re
from pathlib import Path

import duckdb
import pytest
import yaml
from _fixture import FIXTURE, SOURCE_RELEASE
from _tables_helpers import SPEC, connect, read_fixture

from clinvar_gkm_parquet import baseline, sqltemplate
from clinvar_gkm_parquet.cli import main
from clinvar_gkm_parquet.errors import SpecError
from clinvar_gkm_parquet.spec import parse_spec, resolve_spec

REAL = FIXTURE.parent.name == "real-1.1.0"
real_only = pytest.mark.skipif(not REAL, reason="reads the real 1.1.0 excerpt")
CUSTOM = Path(__file__).parent / "fixtures" / "custom-spec" / "tables.yaml"
FULL = ["--full", FIXTURE, "--full-source-release", SOURCE_RELEASE]

# The clinvar-gkm 1.0 tables the generic renderer builds: no computed column, no row filter.
GENERIC_BUILTIN = [
    "sequenceReference",
    "allele",
    "copyNumberCount",
    "copyNumberChange",
    "gene",
    "variation",
    "condition",
    "conditionSet",
    "therapy",
    "therapyGroup",
    "submitter",
    "scv",
    "rcv",
    "vcv",
]
ROW_FILTER_TABLES = ["evidenceLine", "rcv_evidenceLine", "vcv_evidenceLine"]


def cli(capsys, *args) -> tuple[int, str, str]:
    rc = main([str(a) for a in args])
    got = capsys.readouterr()
    return rc, got.out, got.err


# --- one source of truth: the built-in tables ------------------------------------------------------------------


def test_which_builtin_tables_are_generic():
    generic = [t.name for t in SPEC.tables if not sqltemplate.generic_problems(t)]
    assert generic == [n for n in (t.name for t in SPEC.tables) if n in GENERIC_BUILTIN]
    for t in SPEC.tables:
        if t.name in GENERIC_BUILTIN:
            continue
        problems = " ".join(sqltemplate.generic_problems(t))
        assert ("(computed)" in problems) or ("row filter" in problems), (t.name, problems)


@pytest.mark.parametrize("name", GENERIC_BUILTIN)
def test_generic_renders_the_hand_written_sql(name):
    """Byte for byte, the fast SQL and the slow reference."""
    t = SPEC.table(name)
    assert sqltemplate.render_generic(t) == sqltemplate.render(t)
    assert sqltemplate.render_generic(t, fast=False) == sqltemplate.render(t, fast=False)


def _select_list(text: str) -> list[str]:
    lines = [ln for ln in text.splitlines() if not ln.lstrip().startswith("--")]
    return lines[lines.index("SELECT") + 1 : lines.index("FROM {{source}}")]


@pytest.mark.parametrize("name", ROW_FILTER_TABLES)
def test_row_filter_tables_have_the_generic_select_list(name):
    """Their columns are all generic kinds; only the row filter (the evidence-line split) is hand-written."""
    t = SPEC.table(name)
    _, text = sqltemplate.generic_template(dataclasses.replace(t, row_filter=None))
    assert _select_list(sqltemplate._sql_text(f"{name}.sql")) == _select_list(text)


def test_shipped_sql_agrees_with_the_spec():
    """Every shipped file's typed-copy pointer positions are its column's `pointers`, and it checks the top-level
    fields ({{known_keys}}) exactly when the table says known_fields."""
    for t in SPEC.tables:
        text = sqltemplate._sql_text(f"{t.name}.sql")
        assert ("{{known_keys}}" in text) == t.known_fields, t.name
        for m in re.finditer(r"\{\{typed (\w+)((?: [^}]+)?)\}\}", text):
            c = t.column(m.group(1))
            opts = dict(a.split("=", 1) for a in m.group(2).split())
            got = set(opts.get("ptr", "").split(",")) - {""}
            assert got == {p.replace("[*]", "[]") for p in c.derivation.pointers}, (t.name, c.name)
        for c in t.columns:
            if c.derivation.pointers:
                assert f"{{{{typed {c.name} ptr=" in text, (t.name, c.name)


def test_renderer_for():
    for t in SPEC.tables:
        assert baseline.renderer_for(t) is sqltemplate.render  # the shipped file wins
    spec, _ = resolve_spec(str(CUSTOM))
    for t in spec.tables:
        assert baseline.renderer_for(t) is sqltemplate.render_generic


# --- the custom spec: expectations from the records, parsed with stdlib json ----------------------------------


def _records(section: str) -> dict[str, dict]:
    return {k: json.loads(r) for k, r in read_fixture()[section]}


def _bare(p: str | None) -> str | None:
    assert p is None or re.match(r"^#/[^/]+/.", p), p
    return None if p is None else p.split("/", 2)[2]


def _section_of(p: str | None) -> str | None:
    return None if p is None else p.split("/", 2)[1]


def _refs(node, path: str = ""):
    if isinstance(node, dict):
        for k, v in node.items():
            yield from _refs(v, f"{path}.{k}" if path else k)
    elif isinstance(node, list):
        for v in node:
            yield from _refs(v, path + "[]")
    elif isinstance(node, str) and node.startswith("#/"):
        section, _, key = node[2:].partition("/")
        yield {"path": path, "section": section, "id": key}


def _want_variation(rec: dict) -> dict:
    return {
        "name": rec.get("name"),
        "members": [_bare(m) for m in rec.get("members", [])],
        "mappings": [
            {
                "coding": {
                    "system": m["coding"].get("system"),
                    "code": m["coding"].get("code"),
                    "iris": m["coding"].get("iris", []),
                },
                "relation": m.get("relation"),
            }
            for m in rec.get("mappings", [])
        ],
        "mapping_codes": [m.get("coding", {}).get("code") for m in rec.get("mappings", [])],
        "refs": list(_refs(rec)),
    }


def _want_scv(rec: dict) -> dict:
    c = rec.get("classification")
    classification = None
    if c is not None:
        assert all(e["name"] == "description" for e in c.get("extensions", []))
        classification = {
            "type": c.get("type"),
            "conceptType": c.get("conceptType"),
            "name": c.get("name"),
            "primaryCoding": None
            if "primaryCoding" not in c
            else {"code": c["primaryCoding"].get("code"), "system": c["primaryCoding"].get("system")},
            "extensions": [{"name": e["name"], "value_string": e["value"]} for e in c.get("extensions", [])],
        }
    contributions = rec.get("contributions", [])
    return {
        "classification": classification,
        "proposition_id": _bare(rec.get("proposition")),
        "proposition_section": _section_of(rec.get("proposition")),
        "contributor_ids": [_bare(x.get("contributor")) for x in contributions],
        "contribution_dates": [x.get("date") for x in contributions],
        "evidence_line_sections": [_section_of(p) for p in rec.get("hasEvidenceLines", [])],
    }


def _rows(path: Path, columns: list[str]) -> dict[str, dict]:
    con = duckdb.connect()
    cols = ", ".join(f'"{c}"' for c in columns)
    out = {}
    for row in con.execute(f"SELECT id, {cols} FROM read_parquet('{path}') ORDER BY id").fetchall():
        out[row[0]] = dict(zip(columns, row[1:], strict=True))
    return out


def _dates_as_text(rows: dict[str, dict]) -> dict[str, dict]:
    for r in rows.values():
        r["contribution_dates"] = [None if d is None else d.isoformat() for d in r["contribution_dates"]]
    return rows


@real_only
def test_custom_spec_build_verify(tmp_path, capsys):
    out = tmp_path / "out"
    rc, stdout, err = cli(capsys, "build", *FULL, "--spec", CUSTOM, "--out", out)
    assert rc == 0, err
    assert "built 2 table(s) of spec summary-example 0.1 (custom" in stdout
    variations, scvs = _records("variation"), _records("scv")
    got = _rows(out / "variation_summary.parquet", ["name", "members", "mappings", "mapping_codes", "refs"])
    assert got == {k: _want_variation(r) for k, r in variations.items()}
    assert any(r["members"] for r in got.values()) and any(r["refs"] for r in got.values())
    cols = list(_want_scv(next(iter(scvs.values()))))
    got = _dates_as_text(_rows(out / "scv_summary.parquet", cols))
    assert got == {k: _want_scv(r) for k, r in scvs.items()}
    assert any(r["evidence_line_sections"] for r in got.values())  # the excerpt has one such SCV
    # data is the record, byte for byte (JSON-typed in variation_summary)
    con = duckdb.connect()
    for table, section in (("variation_summary", "variation"), ("scv_summary", "scv")):
        data = con.execute(
            f"SELECT id, CAST(data AS VARCHAR) FROM read_parquet('{out / table}.parquet')"
        ).fetchall()
        assert sorted(data) == sorted(read_fixture()[section])
    assert cli(capsys, "verify", out, "--spec", CUSTOM)[0] == 0  # DESCRIBE equals the spec, among others
    rc = cli(capsys, "verify", out, "--spec", CUSTOM, "--bundle-recompute", *FULL, "--work-dir", tmp_path / "w")[0]
    assert rc == 0


@real_only
def test_custom_spec_fast_equals_slow():
    spec, _ = resolve_spec(str(CUSTOM))
    con = connect()
    for t in spec.tables:
        fast = con.execute(f"SELECT * FROM ({sqltemplate.render_generic(t)}) ORDER BY id").fetchall()
        slow = con.execute(f"SELECT * FROM ({sqltemplate.render_generic(t, fast=False)}) ORDER BY id").fetchall()
        assert fast == slow and len(fast) == len(read_fixture()[t.sections[0]])


def _bfs(seeds: dict[str, set[str]], sections: set[str]) -> dict[str, set[str]]:
    """Records reached from the seeds by outgoing pointers within `sections` (ADR 0004 `out`, no depth limit)."""
    recs = {s: dict(read_fixture()[s]) for s in sections}
    got = {s: set(seeds.get(s, ())) for s in sections}
    todo = [(s, k) for s, ks in seeds.items() for k in ks]
    while todo:
        s, k = todo.pop()
        for r in _refs(json.loads(recs[s][k])):
            if r["section"] in sections and r["id"] not in got[r["section"]]:
                got[r["section"]].add(r["id"])
                todo.append((r["section"], r["id"]))
    return got


@real_only
def test_custom_spec_as_the_driver_spec(tmp_path, capsys):
    """--where over the generic table as the spec builds it, then the normal build of the mini-bundle."""
    out = tmp_path / "out"
    where = (
        "proposition_section = 'propositionPathogenicity' AND "
        "list_contains(contributor_ids, 'clinvar.submitter:3')"
    )
    sections = "scv,propositionPathogenicity,variation"
    rc, stdout, err = cli(
        capsys, "build", *FULL, "--spec", CUSTOM, "--driver", "scv_summary", "--where", where,
        "--sections", sections, "--out", out,
    )  # fmt: skip
    assert rc == 0, err
    seeds = {
        k
        for k, r in _records("scv").items()
        if r["proposition"].startswith("#/propositionPathogenicity/")
        and "#/submitter/clinvar.submitter:3" in [c["contributor"] for c in r["contributions"]]
    }
    assert seeds
    want = _bfs({"scv": seeds}, set(sections.split(",")))
    assert set(_rows(out / "scv_summary.parquet", ["proposition_id"])) == want["scv"] == seeds
    assert set(_rows(out / "variation_summary.parquet", ["name"])) == want["variation"] != set()
    con = duckdb.connect()
    kv = dict(con.execute(f"SELECT key, value FROM parquet_kv_metadata('{out}/scv_summary.parquet')").fetchall())
    selection = json.loads(kv[b"clinvar_gkm_parquet.selection"])
    assert (selection["driver"], selection["where"]) == ("scv_summary", where)
    assert cli(capsys, "verify", out, "--spec", CUSTOM)[0] == 0


# --- fail loud: when the spec loads ----------------------------------------------------------------------------


def _raw() -> dict:
    return yaml.safe_load(CUSTOM.read_text(encoding="utf-8"))


def _col(raw: dict, table: str, column: str) -> dict:
    t = next(t for t in raw["tables"] if t["name"] == table)
    return next(c for c in t["columns"] if c["name"] == column)


def _table(raw: dict, table: str) -> dict:
    return next(t for t in raw["tables"] if t["name"] == table)


def _set(table, column, **changes):
    def mutate(raw):
        c = _col(raw, table, column)
        for k, v in changes.items():
            if k in ("kind", "path", "pointers", "transform"):
                c["derivation"][k] = v
            else:
                c[k] = v
        return raw

    return mutate


def _add_column(table, column):
    def mutate(raw):
        t = _table(raw, table)
        t["columns"].insert(1, column)
        return raw

    return mutate


def _table_set(table, **changes):
    def mutate(raw):
        _table(raw, table).update(changes)
        return raw

    return mutate


def _unknown_derivation_key(raw):
    _col(raw, "scv_summary", "proposition_id")["derivation"]["via"] = "x"
    return raw


def _slots(raw):
    raw["tables"][1]["extension_slots"]["classification"] = {"description": "value_nope"}
    return raw


def _column(name, type_, kind, path, nulls="when-absent", transform=None) -> dict:
    return {
        "name": name,
        "type": type_,
        "nulls": nulls,
        "derivation": {"kind": kind, "path": path, "transform": transform},
        "description": "test",
    }


LOAD_FAILS = [
    (
        "a [] path",
        _set("scv_summary", "contributor_ids", path="$.contributions[].contributor"),
        "is not $.<field>",
    ),
    ("a path without $", _set("scv_summary", "proposition_id", path="proposition"), "is not $.<field>"),
    ("a path into a key", _set("scv_summary", "proposition_id", path="$.a..b"), "is not $.<field>"),
    ("a list path on a typed copy", _set("variation_summary", "mappings", path="$.mappings[*]"), "fields only"),
    ("a path on a key", _set("scv_summary", "id", path="$.id"), "takes no path"),
    ("too few list levels", _set("scv_summary", "contributor_ids", type="VARCHAR"), "list level(s)"),
    ("too many list levels", _set("scv_summary", "proposition_id", type="VARCHAR[][]"), "list level(s)"),
    ("a bare id that is not text", _set("scv_summary", "proposition_id", type="BIGINT"), "VARCHAR or a list"),
    (
        "a scalar path of a struct",
        _set("variation_summary", "name", type="STRUCT(a VARCHAR)"),
        "a scalar or a list",
    ),
    (
        "a computed column with no SQL",
        _add_column("scv_summary", _column("n", "BIGINT", "computed", "$.contributions", transform="count")),
        "n (computed): only a shipped SQL file implements",
    ),
    ("a row filter with no SQL", _table_set("scv_summary", row_filter="only some rows"), "row filter"),
    (
        "a pointer position not in the type",
        _set("variation_summary", "mappings", pointers=["[*].nope"]),
        "ptr position(s) not in the type",
    ),
    (
        "a pointer position of bad syntax",
        _set("variation_summary", "mappings", pointers=["[].x"]),
        "pointer position",
    ),
    ("pointers on a scalar path", _set("variation_summary", "name", pointers=["[*]"]), "pointers is for"),
    (
        "known_fields with a nested path",
        _table_set("variation_summary", known_fields=True),
        "known_fields needs every column to read a top-level field",
    ),
    ("an extension slot not in the type", _slots, "slot(s) not in the column type"),
    ("an unknown derivation key", _unknown_derivation_key, "unknown key"),
    ("a bad type", _set("variation_summary", "mappings", type="STRUCT(oops"), "invalid DuckDB type"),
]


@pytest.mark.parametrize("label, mutate, match", LOAD_FAILS, ids=[c[0] for c in LOAD_FAILS])
def test_custom_spec_fails_when_it_loads(tmp_path, label, mutate, match):
    raw = mutate(_raw())
    p = tmp_path / "tables.yaml"
    p.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    with pytest.raises(SpecError, match=re.escape(match)):
        resolve_spec(str(p))


def test_the_example_spec_loads():
    spec, src = resolve_spec(str(CUSTOM))
    assert [t.name for t in spec.tables] == ["variation_summary", "scv_summary"] and not src.builtin
    assert parse_spec(_raw()) == spec


# --- fail loud: when the build reads a record ------------------------------------------------------------------


def _fails(raw: dict, table: str, *needles: str) -> None:
    """Rendering the table and running it on the real records stops with every needle in the message; the slow
    reference stops with the same message."""
    spec = parse_spec(raw)
    t = spec.table(table)
    assert not sqltemplate.generic_problems(t)
    msgs = []
    for fast in (True, False):
        con = connect()
        with pytest.raises(duckdb.InvalidInputException) as exc:
            con.execute(f"SELECT * FROM ({sqltemplate.render_generic(t, fast=fast)}) ORDER BY 1").fetchall()
        msgs.append(str(exc.value))
    for n in needles:
        assert n in msgs[0], msgs[0]
    assert msgs[1] == msgs[0]


@real_only
def test_unknown_field_under_a_typed_copy_stops_the_build(tmp_path, capsys):
    """The spec's classification type lacks primaryCoding, which the records have: the build stops, naming the
    table, column, key and field (no field is dropped), through the CLI too."""
    raw = _raw()
    c = _col(raw, "scv_summary", "classification")
    c["type"] = c["type"].replace('primaryCoding STRUCT(code VARCHAR, "system" VARCHAR), ', "")
    _fails(
        raw,
        "scv_summary",
        "table scv_summary: column classification",
        "field(s) not in the spec type",
        "primaryCoding",
    )
    p = tmp_path / "tables.yaml"
    p.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    rc, _, err = cli(capsys, "build", *FULL, "--spec", p, "--out", tmp_path / "out", "--tables", "scv_summary")
    assert rc == 2 and "field(s) not in the spec type at (column): [primaryCoding]" in err
    assert "table scv_summary: column classification: key clinvar.submission:SCV" in err
    assert not list((tmp_path / "out").glob("*.parquet"))


@real_only
def test_non_pointer_at_a_bare_id_path_stops_the_build():
    raw = _add_column("variation_summary", _column("name_id", "VARCHAR", "bare-id", "$.name"))(_raw())
    _fails(raw, "variation_summary", "column name_id", "not a #/<section>/<id> pointer at (column)")


@real_only
def test_non_pointer_in_a_list_path_stops_the_build():
    raw = _add_column(
        "scv_summary", _column("dates", "VARCHAR[]", "bare-id", "$.contributions[*].date", "empty-list")
    )(_raw())
    _fails(raw, "scv_summary", "column dates", "not a #/<section>/<id> pointer at contributions[].date")


@real_only
def test_field_step_on_a_non_object_stops_the_build():
    raw = _add_column(
        "variation_summary", _column("x", "VARCHAR[]", "scalar-path", "$.members[*].x", "empty-list")
    )(_raw())
    _fails(raw, "variation_summary", "column x", "expected an object at members[], got ")


@real_only
def test_list_step_on_a_non_list_stops_the_build():
    raw = _add_column(
        "scv_summary", _column("names", "VARCHAR[]", "scalar-path", "$.classification[*].name", "empty-list")
    )(_raw())
    _fails(raw, "scv_summary", "column names", "expected a list at classification, got ")


@real_only
def test_object_at_a_scalar_path_stops_the_build():
    raw = _add_column("scv_summary", _column("c", "VARCHAR", "scalar-path", "$.classification"))(_raw())
    _fails(raw, "scv_summary", "column c", "expected a scalar")


@real_only
def test_malformed_pointer_in_refs_stops_the_build():
    """The refs column of a generic table keeps the baseline rule (ADR 0003 item 4)."""
    sections = copy.deepcopy(read_fixture())
    key, text = sections["variation"][0]
    rec = json.loads(text)
    rec["name"] = "#/oops"
    sections["variation"][0] = (key, json.dumps(rec, separators=(",", ":")))
    spec, _ = resolve_spec(str(CUSTOM))
    con = connect(sections)
    with pytest.raises(duckdb.InvalidInputException, match="malformed pointer"):
        con.execute(f"SELECT * FROM ({sqltemplate.render_generic(spec.table('variation_summary'))})").fetchall()


@real_only
def test_known_fields_with_refs_and_a_list_path():
    """known_fields on a table with a refs column and a $.<field>[*] path renders; a record field no column reads
    stops the build."""
    raw = _raw()
    raw["tables"] = [
        {
            "name": "scv_known",
            "sections": ["scv"],
            "row_filter": None,
            "sort_key": "id",
            "known_fields": True,
            "description": "test",
            "columns": [
                _column("id", "VARCHAR", "key", None, "never"),
                _column("el", "VARCHAR[]", "pointer-section", "$.hasEvidenceLines[*]", "empty-list"),
                _column("refs", "STRUCT(path VARCHAR, section VARCHAR, id VARCHAR)[]", "refs", "$", "empty-list"),
                _column("data", "VARCHAR", "record", None, "never"),
            ],
        }
    ]
    _fails(raw, "scv_known", "table scv_known: record: key clinvar.submission:SCV", "top-level field(s) no column")
