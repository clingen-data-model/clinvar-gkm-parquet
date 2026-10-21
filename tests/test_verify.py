import json
import shutil
from argparse import Namespace
from pathlib import Path

import duckdb
import pytest
from _build_helpers import KV, make_con, make_spec, render

from clinvar_gkm_parquet import verify as verify_mod
from clinvar_gkm_parquet.build import KV_KEYS, KV_PREFIX, LoadedState, build_tables
from clinvar_gkm_parquet.cli import main
from clinvar_gkm_parquet.verify import (
    _json_first_diff,
    compare_dirs,
    recompute,
    recompute_failed,
    table_differences,
    verify_dir,
)

ALL = ["thing", "kv", "evA", "evB"]


@pytest.fixture
def built(tmp_path):
    spec, con = make_spec(), make_con()
    out = tmp_path / "out"
    build_tables(con, spec, ALL, out, KV, render=render)
    return spec, con, out


def _rewrite(con, path: Path, select: str, kv: dict | None = None) -> None:
    """Rewrite a Parquet file from a SELECT over `src` (the file's current content)."""
    tmp = path.with_suffix(".rewrite")
    shutil.move(path, tmp)
    meta = ""
    if kv is not None:
        items = ", ".join(f"'{KV_PREFIX}{k}': '{v}'" for k, v in kv.items())
        meta = f", KV_METADATA {{{items}}}"
    con.execute(
        f"COPY (WITH src AS (SELECT * FROM read_parquet('{tmp}')) {select}) TO '{path}' (FORMAT parquet{meta})"
    )
    tmp.unlink()


# --- verify_dir ------------------------------------------------------------------------------------------------


def test_verify_dir_passes_on_a_build(built):
    spec, _, out = built
    assert verify_dir(out, spec) == []


def test_verify_dir_finds_each_problem(built):
    spec, con, out = built
    (out / "evB.parquet").unlink()
    shutil.copy(out / "kv.parquet", out / "stray.parquet")
    _rewrite(con, out / "kv.parquet", "SELECT key, value, 1 AS name FROM src", kv={**KV, "table": "kv"})
    _rewrite(
        con,
        out / "evA.parquet",
        "SELECT * FROM src",
        kv={**KV, "table": "evA", "source_release": "2026-10-11"},
    )
    (out / "manifest.json").write_text(json.dumps({"tables": {"thing": {"rows": 31, "sha256": "00"}}}))
    problems = verify_dir(out, spec)
    text = "\n".join(problems)
    assert "evB: evB.parquet is missing" in text
    assert "stray: stray.parquet is not a table of spec 9.9" in text
    assert "kv: schema: column name: file INTEGER != spec VARCHAR" in text
    assert "clinvar_gkm_parquet.source_release differs across files" in text
    assert "manifest.json: thing rows 31 != file 30" in text
    assert "manifest.json: thing sha256 00 != file" in text
    assert len(problems) == 6, problems


def test_verify_dir_missing_metadata(built):
    spec, con, out = built
    _rewrite(con, out / "kv.parquet", "SELECT * FROM src")
    (problem,) = verify_dir(out, spec)
    assert problem.startswith("kv: key-value metadata lacks clinvar_gkm_parquet.tool_version")
    assert all(KV_PREFIX + k in problem for k in KV_KEYS)


# --- bundle recompute ------------------------------------------------------------------------------------------


def test_recompute_passes(built):
    spec, con, out = built
    groups = recompute(con, spec, out)
    assert [g["tables"] for g in groups] == [["thing"], ["kv"], ["evA", "evB"]]
    assert not any(recompute_failed(g) for g in groups)
    ev = groups[2]
    assert ev["bundle_keys"] == 14 and ev["rows"] == {"evA": 7, "evB": 7}


def test_recompute_finds_each_difference(built):
    spec, con, out = built
    con.execute("DELETE FROM bundle_kv WHERE key = 'K1'")  # a row the bundle doesn't have
    con.execute("""INSERT INTO bundle_thing VALUES ('T5000', '{"id":"T5000"}')""")  # a key no row has
    con.execute("""UPDATE bundle_ev SET record = '{"id":"B002","w":99}' WHERE key = 'B002'""")  # data differs
    _rewrite(
        con, out / "evB.parquet", "SELECT * FROM src UNION ALL SELECT 'A001', data FROM src WHERE id = 'B001'"
    )
    by = {tuple(g["tables"]): g for g in recompute(con, spec, out)}
    kv, thing, ev = by[("kv",)], by[("thing",)], by[("evA", "evB")]
    assert kv["not_in_bundle"] == {"count": 1, "samples": [{"key": "K1", "table": "kv"}]}
    assert kv["missing"]["count"] == 0 and kv["data_differs"]["count"] == 0
    assert thing["missing"] == {"count": 1, "samples": [{"key": "T5000", "table": None}]}
    assert thing["not_in_bundle"]["count"] == thing["data_differs"]["count"] == 0
    assert ev["data_differs"]["count"] == 2  # B002, and the duplicate A001 in evB (its data is B001's)
    assert {s["key"] for s in ev["data_differs"]["samples"]} == {"A001", "B002"}
    assert ev["duplicates"] == {"count": 1, "samples": [{"key": "A001", "tables": ["evA", "evB"]}]}
    assert all(recompute_failed(g) for g in (kv, thing, ev))


def test_recompute_reports_missing_file(built):
    spec, con, out = built
    (out / "evA.parquet").unlink()
    g = [g for g in recompute(con, spec, out) if "evA" in g["tables"]][0]
    assert g["problems"] == ["not checked: evA missing"] and recompute_failed(g)


# --- the oracle ------------------------------------------------------------------------------------------------


def _reference(con, tool: Path, ref: Path) -> None:
    """A reference set with one planted difference of every kind (see test_oracle_finds_each_difference)."""
    ref.mkdir()
    for name in ("kv", "evB"):
        shutil.copy(tool / f"{name}.parquet", ref / f"{name}.parquet")
    t5 = json.dumps(
        {"info": {"b": [5, 6], "a": "a5"}, "tags": ["t5", "x"], "n": 5, "name": "thing 5", "id": "T0005"}
    )
    info_t = "STRUCT(a VARCHAR, b BIGINT[])"
    con.execute(
        f"""COPY (
            SELECT id,
                   CASE WHEN id = 'T0001' THEN 'renamed' ELSE name END AS name,
                   n::INTEGER AS n,
                   CASE WHEN id = 'T0003' THEN NULL ELSE tags END AS tags,
                   CASE WHEN id = 'T0011' THEN {{'a': info.a, 'b': [11, 99]}}::{info_t}
                        WHEN id = 'T0000' THEN {{'a': NULL, 'b': NULL}}::{info_t}
                        ELSE info END AS info,
                   CASE WHEN id = 'T0005' THEN '{t5}'
                        WHEN id = 'T0007' THEN replace(data, 'thing 7', 'thing seven')
                        ELSE data END AS data,
                   'x' AS extra
            FROM read_parquet('{tool / "thing.parquet"}') WHERE id <> 'T0009'
            UNION ALL SELECT 'T9999', 'new', 1, []::VARCHAR[], NULL, '{{}}', 'x'
        ) TO '{ref / "thing.parquet"}' (FORMAT parquet)"""
    )
    con.execute(f"COPY (SELECT 1 AS id) TO '{ref / 'zzz.parquet'}' (FORMAT parquet)")


def test_oracle_finds_each_difference(built, tmp_path):
    spec, con, out = built
    ref = tmp_path / "ref"
    _reference(con, out, ref)
    o = compare_dirs(duckdb.connect(), out, ref, spec)
    t = o["tables"]
    assert t["kv"]["differences"] == [] and t["evB"]["differences"] == []
    assert t["evA"]["differences"] == ["missing in reference"]
    assert t["zzz"]["differences"] == ["missing in tool"]

    th = t["thing"]
    assert th["schema"]["differences"] == [
        "column n: tool BIGINT != reference INTEGER",
        "column extra only in reference",
    ]
    assert th["rows"] == {"tool": 30, "reference": 30, "equal": True}
    assert th["ids"]["only_tool"] == {"count": 1, "samples": ["T0009"]}
    assert th["ids"]["only_reference"] == {"count": 1, "samples": ["T9999"]}
    assert th["ids"]["duplicates_tool"] == th["ids"]["duplicates_reference"] == 0
    cols = th["columns"]
    assert {c: e["distinct"] for c, e in cols.items()} == {"name": 1, "n": 0, "tags": 1, "info": 2}
    assert cols["n"]["compared_as"] == "reference cast to BIGINT"
    assert cols["name"]["samples"] == [{"id": "T0001", "tool": "thing 1", "reference": "renamed"}]
    assert cols["info"]["paths"] == {"info (null)": 1, "info.b[]": 1}
    assert [s["id"] for s in cols["info"]["samples"]] == ["T0000", "T0011"]
    assert cols["tags"]["paths"] == {"tags (null)": 1, "tags (length)": 1, "tags[]": 1}
    rec = th["records"]["data"]
    assert (rec["bytes_differ"], rec["json_differ"], rec["format_only"]) == (2, 1, 1)
    assert [s["id"] for s in rec["samples_format"]] == ["T0005"]
    assert [(s["id"], s["path"]) for s in rec["samples_content"]] == [("T0007", "$.name")]
    assert th["differences"] == [
        "schema (2 difference(s))",
        "1 id(s) only in tool",
        "1 id(s) only in reference",
        "name: 1 row(s) differ",
        "tags: 1 row(s) differ",
        "info: 2 row(s) differ",
        "data: 2 byte difference(s) (1 content, 1 format only)",
    ]


def test_oracle_identical_dirs_match(built, tmp_path):
    spec, _, out = built
    ref = tmp_path / "ref"
    shutil.copytree(out, ref)
    o = compare_dirs(duckdb.connect(), out, ref, spec)
    assert all(table_differences(t) == [] for t in o["tables"].values())


def test_json_first_diff():
    assert _json_first_diff({"a": 1, "b": [1, {"c": 2}]}, {"b": [1, {"c": 2}], "a": 1.0}) is None
    assert _json_first_diff({"a": True}, {"a": 1}) == "$.a"
    assert _json_first_diff({"a": [1]}, {"a": [1, 2]}) == "$.a (length 1 vs 2)"
    assert _json_first_diff({"a": 1}, {"b": 1}) == "$.a"


# --- the command -----------------------------------------------------------------------------------------------


def _vargs(d, **kw):
    base = dict(
        dir=Path(d),
        bundle_recompute=False,
        full=None,
        delta=[],
        manifest=[],
        index=None,
        full_source_release=None,
        sha256=None,
        against=None,
        report=None,
        work_dir=None,
        memory_limit=None,
        threads=None,
    )
    base.update(kw)
    return Namespace(**base)


@pytest.fixture
def spec_patched(monkeypatch):
    spec = make_spec()
    monkeypatch.setattr(verify_mod, "load_spec", lambda v: spec)
    return spec


def test_command_exit_codes_and_report(built, tmp_path, spec_patched, capsys):
    _, con, out = built
    same = tmp_path / "same"
    shutil.copytree(out, same)
    assert verify_mod.run_verify_command(_vargs(out, against=same)) == 0
    ref = tmp_path / "ref"
    _reference(con, out, ref)
    report = tmp_path / "rep" / "oracle.md"
    assert verify_mod.run_verify_command(_vargs(out, against=ref, report=report, work_dir=tmp_path)) == 1
    md = report.read_text()
    assert "Result: **differences**" in md and "`info.b[]`: 1" in md and "| kv | match |" in md
    js = json.loads(report.with_suffix(".json").read_text())
    assert js["passed"] is False and js["oracle"]["tables"]["thing"]["rows"]["tool"] == 30
    assert "oracle: 3 table(s) differ" in capsys.readouterr().out


def test_command_usage_errors(built, tmp_path, spec_patched):
    _, _, out = built
    assert verify_mod.run_verify_command(_vargs(tmp_path / "nope")) == 2
    assert verify_mod.run_verify_command(_vargs(out, bundle_recompute=True)) == 2
    assert verify_mod.run_verify_command(_vargs(out, full="x.json.gz")) == 2
    assert verify_mod.run_verify_command(_vargs(out, against=tmp_path / "nope")) == 2
    assert main(["verify", str(tmp_path / "nope")]) == 2


def test_command_spec_problems_exit_1(built, spec_patched):
    _, _, out = built
    (out / "kv.parquet").unlink()
    assert verify_mod.run_verify_command(_vargs(out)) == 1


def test_command_bundle_recompute(built, tmp_path, spec_patched, monkeypatch):
    _, con, out = built
    monkeypatch.setattr(verify_mod, "resolve_build_inputs", lambda args: "INPUTS")

    def load_inputs(inputs, work, spec, **kw):
        assert inputs == "INPUTS" and work.parent == tmp_path / "w"
        state_con = make_con()
        state_con.execute("DELETE FROM bundle_kv WHERE key = 'K3'")
        return LoadedState(state_con, None, [], {})

    monkeypatch.setattr(verify_mod, "load_inputs", load_inputs)
    report = tmp_path / "rc.md"
    args = _vargs(out, bundle_recompute=True, full="f.json.gz", report=report, work_dir=tmp_path / "w")
    assert verify_mod.run_verify_command(args) == 1
    js = json.loads(report.with_suffix(".json").read_text())
    kv = [g for g in js["bundle_recompute"] if g["tables"] == ["kv"]][0]
    assert kv["not_in_bundle"]["samples"] == [{"key": "K3", "table": "kv"}]
    assert not list((tmp_path / "w").iterdir())  # the verify work dir is removed
