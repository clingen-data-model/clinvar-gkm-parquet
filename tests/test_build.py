import gzip
import hashlib
import json
from argparse import Namespace
from pathlib import Path

import duckdb
import pytest
from _build_helpers import KV, SQL, load_sections, make_con, make_spec, render, render_with, section_rows

from clinvar_gkm_parquet import build as build_mod
from clinvar_gkm_parquet.build import (
    KV_KEYS,
    KV_PREFIX,
    build,
    build_order,
    build_tables,
    kv_metadata,
    manifest_source_for,
    resolve_build_inputs,
    run_build_command,
)
from clinvar_gkm_parquet.errors import InputError, SpecError
from clinvar_gkm_parquet.model import BuildInputs, DeltaManifest, InputFile, StagedBundle, StagedSection

FIXTURE = Path(__file__).parent / "fixtures" / "examples-1.1.0" / "clinvar-gkm_examples.json.gz"


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _kv(con, path):
    rows = con.execute(f"SELECT key, value FROM parquet_kv_metadata('{path}')").fetchall()
    return {k.decode(): v.decode() for k, v in rows}


# --- the table stage -------------------------------------------------------------------------------------------


def test_build_tables_writes_every_table(tmp_path):
    spec, con = make_spec(), make_con()
    results = build_tables(con, spec, [t.name for t in spec.tables], tmp_path, KV, render=render)
    assert {r.table for r in results} == {"thing", "kv", "evA", "evB"}
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "evA.parquet",
        "evB.parquet",
        "kv.parquet",
        "thing.parquet",
    ]
    by = {r.table: r for r in results}
    assert by["thing"].rows == 30 and by["kv"].rows == 5 and by["evA"].rows == 7 and by["evB"].rows == 7
    for r in results:
        assert r.sha256 == _sha(r.file) and r.size == r.file.stat().st_size
        got = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{r.file}')").fetchall()
        assert [(g[0], g[1]) for g in got] == spec.table(r.table).describe()
        kv = _kv(con, r.file)
        assert list(kv) == [KV_PREFIX + k for k in KV_KEYS]
        assert kv[KV_PREFIX + "table"] == r.table
        assert kv[KV_PREFIX + "source_release"] == "2026-10-18"


def test_build_tables_values_order_and_writer_settings(tmp_path):
    spec, con = make_spec(), make_con(section_rows(5000))
    build_tables(con, spec, ["thing"], tmp_path, KV, render=render)
    f = tmp_path / "thing.parquet"
    ids = [r[0] for r in con.execute(f"SELECT id FROM read_parquet('{f}')").fetchall()]
    assert ids == sorted(ids)
    meta = con.execute(
        f"SELECT DISTINCT row_group_id, row_group_num_rows, compression FROM parquet_metadata('{f}') ORDER BY 1"
    ).fetchall()
    assert [m[1] for m in meta] == [2048, 2048, 904]
    assert {m[2] for m in meta} == {"SNAPPY"}
    row = con.execute(f"SELECT * FROM read_parquet('{f}') WHERE id = 'T0001'").fetchone()
    assert row[:4] == ("T0001", "thing 1", 1, ["t1", "x"])
    assert row[4] == {"a": "a1", "b": [1, 2]}
    assert row[5] == section_rows(2)["thing"][1][1]
    # empty-list rule and absent struct
    row0 = con.execute(f"SELECT tags, info FROM read_parquet('{f}') WHERE id = 'T0000'").fetchone()
    assert row0 == ([], None)


def test_build_order_largest_last_and_sections_released():
    spec = make_spec()
    order = build_order(spec, [t.name for t in spec.tables], {"thing": 30, "kv": 5, "ev": 14})
    assert [t.name for t in order] == ["kv", "evA", "evB", "thing"]


def test_on_section_done_after_last_reader(tmp_path):
    spec, con = make_spec(), make_con()
    events = []
    build_tables(
        con,
        spec,
        [t.name for t in spec.tables],
        tmp_path,
        KV,
        render=render,
        on_section_done=events.append,
        log=lambda m: events.append(m) if m.endswith("writing") else None,
    )
    assert events == [
        "table kv: writing",
        "kv",
        "table evA: writing",
        "table evB: writing",
        "ev",
        "table thing: writing",
        "thing",
    ]


def test_describe_mismatch_fails_and_leaves_no_file(tmp_path):
    spec, con = make_spec(), make_con()
    bad = SQL["thing"].replace("AS BIGINT) AS n", "AS INTEGER) AS n")
    with pytest.raises(SpecError, match=r"thing: DESCRIBE differs.*'n', 'INTEGER'"):
        build_tables(con, spec, ["thing"], tmp_path, KV, render=render_with(thing=bad))
    assert list(tmp_path.iterdir()) == []


def test_row_count_mismatch_fails(tmp_path):
    spec, con = make_spec(), make_con()
    bad = SQL["thing"] + " WHERE key <> 'T0003'"
    with pytest.raises(SpecError, match=r"29 rows .* hold 30 keys.*T0003 \(missing\)"):
        build_tables(con, spec, ["thing"], tmp_path, KV, render=render_with(thing=bad))
    assert not (tmp_path / "thing.parquet").exists()


def test_data_for_wrong_key_fails(tmp_path):
    spec, con = make_spec(), make_con()
    # data of T0001 and T0002 swapped: counts match, (id, data) pairs don't
    bad = SQL["kv"].replace(
        "record AS value",
        "CASE key WHEN 'K1' THEN (SELECT record FROM bundle_kv WHERE key = 'K2') "
        "WHEN 'K2' THEN (SELECT record FROM bundle_kv WHERE key = 'K1') ELSE record END AS value",
    )
    with pytest.raises(SpecError, match=r"kv: \(id, data\) pairs differ.*K1 \(data differs\); K2"):
        build_tables(con, spec, ["kv"], tmp_path, KV, render=render_with(kv=bad))


def test_duplicate_sort_key_fails(tmp_path):
    spec, con = make_spec(), make_con()
    bad = SQL["kv"] + " UNION ALL SELECT 'K1', '{}', NULL"
    with pytest.raises(SpecError, match="sort key key is not unique"):
        build_tables(con, spec, ["kv"], tmp_path, KV, render=render_with(kv=bad))


def test_evidence_line_split_must_cover_the_section(tmp_path):
    spec, con = make_spec(), make_con()
    con.execute("""INSERT INTO bundle_ev VALUES ('C001', '{"id":"C001"}')""")
    with pytest.raises(SpecError, match=r"evA, evB: 14 rows \(evA=7, evB=7\) but section\(s\) ev hold 15 keys"):
        build_tables(con, spec, ["evA", "evB"], tmp_path, KV, render=render)
    assert list(tmp_path.iterdir()) == []


def test_evidence_line_split_overlap_fails(tmp_path):
    spec, con = make_spec(), make_con()
    bad = "SELECT key AS id, record AS data FROM bundle_ev WHERE key LIKE 'B%' OR key = 'A001'"
    with pytest.raises(SpecError, match="15 rows"):
        build_tables(con, spec, ["evA", "evB"], tmp_path, KV, render=render_with(evB=bad))


def test_subset_of_a_split_checks_rows_against_the_section(tmp_path):
    spec, con = make_spec(), make_con()
    (r,) = build_tables(con, spec, ["evA"], tmp_path, KV, render=render)
    assert r.rows == 7
    bad = "SELECT key AS id, record || ' ' AS data FROM bundle_ev WHERE key LIKE 'A%'"
    with pytest.raises(SpecError, match=r"evA: rows whose \(id, data\) is not .*A000"):
        build_tables(con, spec, ["evA"], tmp_path / "bad", KV, render=render_with(evA=bad))


def test_unknown_table_and_query_error(tmp_path):
    spec, con = make_spec(), make_con()
    with pytest.raises(SpecError, match="unknown table"):
        build_tables(con, spec, ["nope"], tmp_path, KV, render=render)
    with pytest.raises(SpecError, match="table kv: query failed"):
        build_tables(con, spec, ["kv"], tmp_path, KV, render=render_with(kv="SELECT error('unknown extension x')"))


def test_query_error_mid_copy_leaves_no_file(tmp_path):
    spec, con = make_spec(), make_con()
    bad = SQL["kv"].replace(
        "record AS value", "CASE WHEN key = 'K2' THEN error('bad record K2') ELSE record END AS value"
    )
    with pytest.raises(SpecError, match="table kv: query failed.*bad record K2"):
        build_tables(con, spec, ["kv"], tmp_path, KV, render=render_with(kv=bad))
    assert list(tmp_path.iterdir()) == []


def test_empty_section_gives_an_empty_table(tmp_path):
    spec = make_spec()
    rows = section_rows()
    rows["kv"] = []
    con = make_con(rows)
    (r,) = build_tables(con, spec, ["kv"], tmp_path, KV, render=render)
    assert r.rows == 0
    assert con.execute(f"SELECT count(*) FROM read_parquet('{r.file}')").fetchone()[0] == 0


def _build_once(out: Path, threads: int, n: int = 30_000) -> dict[str, str]:
    con = duckdb.connect()
    con.execute(f"SET threads = {threads}")
    load_sections(con, {"kv": [], "ev": []})
    # thing: generated in SQL, in scrambled key order, so the sort does real work
    con.execute(
        f"""CREATE OR REPLACE TABLE bundle_thing AS
            SELECT printf('T%07d', (i * 7919) % {n}) AS key,
                   '{{"name":"n' || i || '","n":' || i || ',"tags":["a","b' || (i % 5) || '"],'
                   || '"info":{{"a":"x' || i || '","b":[' || i || ']}}}}' AS record
            FROM range({n}) t(i)"""
    )
    spec = make_spec()
    results = build_tables(con, spec, ["thing", "kv", "evA", "evB"], out, KV, render=render)
    con.close()
    return {r.table: r.sha256 for r in results}


def test_determinism_across_runs_and_threads(tmp_path):
    a = _build_once(tmp_path / "a", threads=1)
    b = _build_once(tmp_path / "b", threads=4)
    c = _build_once(tmp_path / "c", threads=8)
    assert a == b == c
    assert duckdb.__version__ == "1.5.4"


def test_kv_metadata_has_no_timestamp_and_deltas_json():
    kv = kv_metadata(
        table_spec_version="1.0",
        bundle_schema_version="1.1.0",
        source_bundle_sha256="f" * 64,
        source_deltas=[
            {"release": "2026-10-04", "sha256": "a" * 64},
            {"release": "2026-10-11", "sha256": "b" * 64},
        ],
        source_release="2026-10-11",
    )
    assert set(kv) == set(KV_KEYS) - {"table"}
    assert json.loads(kv["source_deltas"]) == [
        {"release": "2026-10-04", "sha256": "a" * 64},
        {"release": "2026-10-11", "sha256": "b" * 64},
    ]
    assert kv["source_deltas"].startswith('[{"release":"2026-10-04","sha256":')


# --- the whole build, with the input stages stubbed -------------------------------------------------------------


def _inp(name, release):
    return InputFile(
        source=f"/x/{name}", name=name, release=release, expected_sha256=None, sha256_source=None, size=10
    )


@pytest.fixture
def stubbed(monkeypatch, tmp_path):
    """build() with stage_bundle / check_chain / load_state / sqltemplate replaced by test doubles."""
    spec = make_spec()
    calls: dict = {"staged": [], "chain": None}
    full_rows = section_rows()

    def stage_bundle(inp, work_dir):
        work_dir.mkdir(parents=True, exist_ok=True)
        sections = {}
        for s in ("thing", "kv", "ev"):
            p = work_dir / f"{s}.tsv.gz"
            with gzip.open(p, "wt") as fh:
                fh.write("staged\n")
            sections[s] = StagedSection(s, p, 1)
        calls["staged"].append((inp.name, work_dir))
        sha = hashlib.sha256(inp.name.encode()).hexdigest()
        return StagedBundle(input=inp, sha256=sha, size=123, schema_version="1.1.0", sections=sections)

    def load_state(con, full, deltas):
        calls["deltas"] = deltas
        return load_sections(con, full_rows)

    def check_chain(full_source_release, manifests):
        calls["chain"] = (full_source_release, [m.release for m in manifests])

    def load_manifest(inp):
        rel = {"d1.json.gz.manifest": "2026-10-04", "d2.manifest": "2026-10-11"}[inp.name]
        base = {"2026-10-04": "2026-09-28", "2026-10-11": "2026-10-04"}[rel]
        return DeltaManifest(release=rel, baseline_release=base, deleted={}, checkpoint_full=None)

    monkeypatch.setattr(build_mod.bundle, "stage_bundle", stage_bundle)
    monkeypatch.setattr(build_mod.replay, "load_state", load_state)
    monkeypatch.setattr(build_mod.replay, "check_chain", check_chain)
    monkeypatch.setattr(build_mod.sources, "load_manifest", load_manifest)
    monkeypatch.setattr(build_mod.sqltemplate, "macros_sql", lambda: "CREATE MACRO bare_id(p) AS p")
    monkeypatch.setattr(build_mod.sqltemplate, "render", render)
    inputs = BuildInputs(
        full=_inp("clinvar-gkm_2026-10.json.gz", "2026-09-28"),
        full_source_release="2026-09-28",
        deltas=(
            (_inp("d1.json.gz", "2026-10-04"), _inp("d1.json.gz.manifest", "2026-10-04")),
            (_inp("d2.json.gz", "2026-10-11"), _inp("d2.manifest", "2026-10-11")),
        ),
    )
    return spec, inputs, calls


def test_build_end_to_end_with_stubs(stubbed, tmp_path):
    spec, inputs, calls = stubbed
    out = tmp_path / "out"
    res = build(inputs, out, spec=spec, threads=2, memory_limit="1GB")
    assert calls["chain"] == ("2026-09-28", ["2026-10-04", "2026-10-11"])
    assert [n for n, _ in calls["staged"]] == ["clinvar-gkm_2026-10.json.gz", "d1.json.gz", "d2.json.gz"]
    assert res.source_release == "2026-10-11"
    assert sorted(p.name for p in out.iterdir()) == [
        "evA.parquet",
        "evB.parquet",
        "kv.parquet",
        "manifest.json",
        "thing.parquet",
    ]
    # the work dir (a sibling, created by the build) is gone
    assert not (tmp_path / "out.work").exists()
    con = duckdb.connect()
    kv = _kv(con, out / "thing.parquet")
    full_sha = hashlib.sha256(b"clinvar-gkm_2026-10.json.gz").hexdigest()
    assert kv[KV_PREFIX + "source_bundle_sha256"] == full_sha
    assert kv[KV_PREFIX + "source_release"] == "2026-10-11"
    assert kv[KV_PREFIX + "table_spec_version"] == "9.9"
    assert json.loads(kv[KV_PREFIX + "source_deltas"]) == [
        {"release": "2026-10-04", "sha256": hashlib.sha256(b"d1.json.gz").hexdigest()},
        {"release": "2026-10-11", "sha256": hashlib.sha256(b"d2.json.gz").hexdigest()},
    ]
    m = json.loads((out / "manifest.json").read_text())
    assert m["label"] == "derived from the bundle; may be regenerated" and m["derived"] is True
    assert m["source_release"] == m["release"] == "2026-10-11"
    assert m["build_date"] == m["ended_at"][:10]
    assert m["schema_version"] == m["bundle_schema_version"] == "1.1.0"
    assert m["sections"] == {"ev": 14, "kv": 5, "thing": 30}
    assert m["tables"]["thing"] == {
        "file": "thing.parquet",
        "rows": 30,
        "sha256": _sha(out / "thing.parquet"),
        "size": (out / "thing.parquet").stat().st_size,
    }
    assert m["inputs"]["full"]["sha256"] == full_sha and m["inputs"]["full"]["source_release"] == "2026-09-28"
    assert [d["release"] for d in m["inputs"]["deltas"]] == ["2026-10-04", "2026-10-11"]
    for k in ("generated_at", "started_at", "tool_version", "duckdb_version", "note"):
        assert m[k]
    # the fields ClinVar-GKM's r2_layout.build_index reads (its own parquet-manifest shape)
    assert m["builder"] == f"clinvar-gkm-parquet {m['tool_version']}" and m["dataset"] is None
    assert m["files"] == [
        {"name": n, "size": (out / n).stat().st_size, "sha256": _sha(out / n)}
        for n in ("evA.parquet", "evB.parquet", "kv.parquet", "thing.parquet")
    ]


def test_build_refuses_an_output_dir_holding_a_set(stubbed, tmp_path):
    spec, inputs, calls = stubbed
    out = tmp_path / "out"
    build(inputs, out, spec=spec)
    calls["staged"].clear()
    with pytest.raises(InputError, match="already holds .*manifest.json"):
        build(inputs, out, spec=spec, tables=["kv"])
    assert calls["staged"] == []  # refused before any staging


def test_failed_build_leaves_no_partial_set(stubbed, tmp_path, monkeypatch):
    spec, inputs, _ = stubbed
    monkeypatch.setattr(build_mod.sqltemplate, "render", render_with(thing="SELECT error('boom')"))
    out = tmp_path / "out"
    with pytest.raises(SpecError, match="table thing: query failed"):
        build(inputs, out, spec=spec)  # thing is built last, after evA, evB and kv were written
    assert list(out.iterdir()) == []


def test_build_keep_work_keeps_staging(stubbed, tmp_path):
    spec, inputs, calls = stubbed
    work = tmp_path / "w"
    build(inputs, tmp_path / "out", spec=spec, work_dir=work, keep_work=True, tables=["kv"])
    staged = [d for _, d in calls["staged"]]
    assert all((d / "kv.tsv.gz").exists() for d in staged)
    assert list(work.glob("build-*/state.duckdb"))


def test_build_deletes_staging_per_section(stubbed, tmp_path, monkeypatch):
    spec, inputs, calls = stubbed
    seen = {}
    real = build_mod.build_tables

    def spy(con, spec_, names, out_dir, kv, **kw):
        done = kw["on_section_done"]

        def wrapped(section):
            done(section)
            seen[section] = [(d / f"{section}.tsv.gz").exists() for _, d in calls["staged"]]
            seen[section + "_dropped"] = build_mod.relation_kind(con, f"bundle_{section}") is None

        kw["on_section_done"] = wrapped
        return real(con, spec_, names, out_dir, kv, **kw)

    monkeypatch.setattr(build_mod, "build_tables", spy)
    work = tmp_path / "w"
    work.mkdir()
    build(inputs, tmp_path / "out", spec=spec, work_dir=work)
    assert seen["kv"] == [False, False, False] and seen["kv_dropped"]
    assert seen["ev"] == [False, False, False] and seen["thing_dropped"]
    assert work.exists() and not list(work.iterdir())  # a given work dir stays, emptied


def test_build_refuses_unknown_section(stubbed, tmp_path, monkeypatch):
    spec, inputs, _ = stubbed

    def load_state(con, full, deltas):
        return load_sections(con, {**section_rows(), "mystery": [("k", "{}")]})

    monkeypatch.setattr(build_mod.replay, "load_state", load_state)
    with pytest.raises(SpecError, match=r"section\(s\) \['mystery'\] are not read by any table"):
        build(inputs, tmp_path / "out", spec=spec)
    assert not (tmp_path / "out.work").exists()

    def load_state_empty(con, full, deltas):  # an empty section no table reads is fine
        return load_sections(con, {**section_rows(), "mystery": []})

    monkeypatch.setattr(build_mod.replay, "load_state", load_state_empty)
    build(inputs, tmp_path / "out", spec=spec, tables=["kv"])


def test_build_source_release_without_deltas(stubbed, tmp_path):
    spec, inputs, _ = stubbed
    full_only = BuildInputs(full=inputs.full, full_source_release="2026-09-28")
    res = build(full_only, tmp_path / "out", spec=spec, tables=["kv"])
    assert res.source_release == "2026-09-28"
    con = duckdb.connect()
    assert _kv(con, tmp_path / "out" / "kv.parquet")[KV_PREFIX + "source_deltas"] == "[]"


# --- CLI mapping -----------------------------------------------------------------------------------------------


def test_manifest_source_for():
    assert manifest_source_for("https://h/x/clinvar-gkm-delta_2026-1011.json.gz") == (
        "https://h/x/clinvar-gkm-delta_2026-1011.manifest.json"
    )
    with pytest.raises(InputError):
        manifest_source_for("delta.bin")


def _args(**kw):
    base = dict(
        full=None,
        release=None,
        delta=[],
        manifest=[],
        index=None,
        full_source_release=None,
        sha256=None,
        out=Path("out"),
        tables=None,
        work_dir=None,
        keep_work=False,
        memory_limit=None,
        threads=None,
    )
    base.update(kw)
    return Namespace(**base)


def test_resolve_build_inputs_full_and_deltas(monkeypatch):
    seen = []

    def resolve_input(source, *, sha256=None, index=None):
        seen.append((source, sha256, index))
        return _inp(source.rsplit("/", 1)[-1], "2026-09-28" if "2026-10.json" in source else None)

    monkeypatch.setattr(build_mod.sources, "resolve_input", resolve_input)
    monkeypatch.setattr(build_mod.sources, "load_index", lambda src: {"index": src})
    inputs = resolve_build_inputs(
        _args(
            full="f/clinvar-gkm_2026-10.json.gz",
            sha256="ab",
            delta=["d/clinvar-gkm-delta_2026-1004.json.gz"],
            index="idx.json",
        )
    )
    assert inputs.full_source_release == "2026-09-28"
    assert seen[0] == ("f/clinvar-gkm_2026-10.json.gz", "ab", {"index": "idx.json"})
    assert [s for s, _, _ in seen[1:]] == [
        "d/clinvar-gkm-delta_2026-1004.json.gz",
        "d/clinvar-gkm-delta_2026-1004.manifest.json",
    ]
    with pytest.raises(InputError, match="one per delta"):
        resolve_build_inputs(_args(full="f", delta=["a.json.gz", "b.json.gz"], manifest=["m"]))
    with pytest.raises(InputError, match="don't combine"):
        resolve_build_inputs(_args(release="latest", delta=["a.json.gz"]))
    with pytest.raises(InputError, match="don't combine"):
        resolve_build_inputs(_args(release="latest", sha256="ab" * 32))


def test_resolve_build_inputs_release(monkeypatch):
    got = {}

    def resolve_release(index, release, *, base_url):
        got.update(index=index, release=release, base_url=base_url)
        return "INPUTS"

    monkeypatch.setattr(build_mod.sources, "load_index", lambda src: {"from": src})
    monkeypatch.setattr(build_mod.sources, "resolve_release", resolve_release)
    assert resolve_build_inputs(_args(release="latest")) == "INPUTS"
    assert got == {
        "index": {"from": build_mod.sources.DEFAULT_INDEX_URL},
        "release": "latest",
        "base_url": "https://data.clinvar-gkm.org",
    }
    monkeypatch.setattr(build_mod.sources, "load_index", lambda src: {"base_url": "https://mirror.example"})
    resolve_build_inputs(_args(release="2026-10-18", index="idx.json"))
    assert got["base_url"] == "https://mirror.example" and got["release"] == "2026-10-18"


def test_run_build_command(monkeypatch, capsys, stubbed, tmp_path):
    spec, inputs, _ = stubbed
    monkeypatch.setattr(build_mod, "resolve_build_inputs", lambda args: inputs)
    monkeypatch.setattr(build_mod, "load_spec", lambda v: spec)
    rc = run_build_command(_args(full="x", out=tmp_path / "o", tables="kv, evA,evB"))
    assert rc == 0
    out = capsys.readouterr().out
    assert "built 3 table(s), 19 rows, source release 2026-10-11" in out


# --- end to end on the real fixture ----------------------------------------------------------------------------


def _passthrough_spec():
    """One table per bundle section: id = key, data = record. Exercises the real stage and replay, the build
    and verify on a real bundle without depending on the table SQL."""
    from clinvar_gkm_parquet.bundle import SECTIONS

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
        ],
    }
    from clinvar_gkm_parquet.spec import parse_spec

    return parse_spec(raw)


def _fixture_inputs() -> BuildInputs:
    sha = (FIXTURE.parent / (FIXTURE.name + ".sha256")).read_text().split()[0]
    inp = InputFile(
        source=str(FIXTURE),
        name=FIXTURE.name,
        release="2026-10-18",
        expected_sha256=sha,
        sha256_source="sidecar",
        size=FIXTURE.stat().st_size,
    )
    return BuildInputs(full=inp, full_source_release="2026-10-18")


def test_build_and_verify_real_fixture(tmp_path, monkeypatch):
    """Real stage + replay on the examples-1.1.0 fixture, a pass-through spec, then verify and recompute."""
    from clinvar_gkm_parquet.verify import recompute, recompute_failed, verify_dir

    spec = _passthrough_spec()
    monkeypatch.setattr(build_mod.sqltemplate, "macros_sql", lambda: "SELECT 1")
    monkeypatch.setattr(
        build_mod.sqltemplate, "render", lambda t: f'SELECT key AS id, record AS data FROM "bundle_{t.name}"'
    )
    a = build(_fixture_inputs(), tmp_path / "a", spec=spec, threads=1)
    b = build(_fixture_inputs(), tmp_path / "b", spec=spec, threads=4, work_dir=tmp_path / "w")
    assert [(t.table, t.sha256) for t in a.tables] == [(t.table, t.sha256) for t in b.tables]
    rows = {t.table: t.rows for t in a.tables}
    assert sum(rows.values()) == 346 and rows["copyNumberCount"] == rows["therapyGroup"] == 0
    assert verify_dir(tmp_path / "a", spec) == []
    m = json.loads((tmp_path / "a" / "manifest.json").read_text())
    assert m["source_bundle_sha256"] == _fixture_inputs().full.expected_sha256
    assert m["source_release"] == "2026-10-18" and m["source_deltas"] == []
    state = build_mod.load_inputs(_fixture_inputs(), tmp_path / "rc", spec)
    try:
        assert not any(recompute_failed(g) for g in recompute(state.con, spec, tmp_path / "a"))
    finally:
        state.con.close()


@pytest.mark.xfail(
    strict=True,
    raises=SpecError,
    reason="fixture's 2026-10-04 shape: bare condition CURIEs (VCV/RCV proposition objects, minted condition-set "
    "members) where the current 1.0 code writes #/ pointers; the pointer check stays strict. The same pipeline on "
    "the fixture moved to the current shape passes: tests/test_tables_build.py",
)
def test_build_fixture_end_to_end(tmp_path):
    """The real spec and table SQL on the raw fixture."""
    from clinvar_gkm_parquet.bundle import SECTIONS
    from clinvar_gkm_parquet.spec import load_spec

    spec = load_spec("1.0")
    if not set(SECTIONS) <= spec.sections:
        pytest.skip("needs spec/1.0 to cover every bundle section (table-spec work package)")
    try:
        res = build(_fixture_inputs(), tmp_path / "out")
    except NotImplementedError as e:
        pytest.skip(f"needs the table-spec work package: {e!r}")
    from clinvar_gkm_parquet.verify import verify_dir

    assert verify_dir(tmp_path / "out") == []
    res2 = build(_fixture_inputs(), tmp_path / "out2", threads=4)
    assert [t.sha256 for t in res.tables] == [t.sha256 for t in res2.tables]
