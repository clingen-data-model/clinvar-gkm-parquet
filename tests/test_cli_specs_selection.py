"""The CLI end to end on the real 1.1.0 excerpt: `--spec` (the baseline default, clinvar-gkm, a custom file),
selective builds (`--driver` with `--where`, `--ids`, `--ids-file`, the follow modes, `--max-depth`, `--sections`,
deltas), the `select` command, and `verify` of each result (the directory check, the bundle recompute, a subset
against its mini-bundle, the oracle's refusals).

Expectations come from the bundle records parsed here with stdlib json, independently of the tool; the follow and
depth semantics themselves are checked against a breadth-first search in tests/test_select.py.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import duckdb
import pytest
import yaml
from _fixture import FIXTURE, SOURCE_RELEASE
from _input_helpers import D1, D2, bundle_bytes, fixture_sections, local_input, two_synthetic_deltas, write_gz

from clinvar_gkm_parquet import select as sel
from clinvar_gkm_parquet.bundle import SECTIONS
from clinvar_gkm_parquet.cli import main
from clinvar_gkm_parquet.model import BuildInputs
from clinvar_gkm_parquet.replay import ReplayedSource, load_chain
from clinvar_gkm_parquet.spec import builtin_dir

REAL = FIXTURE.parent.name == "real-1.1.0"
pytestmark = pytest.mark.skipif(not REAL, reason="reads the real 1.1.0 excerpt")

FULL = ["--full", FIXTURE, "--full-source-release", SOURCE_RELEASE]
FIXTURE_SHA = (FIXTURE.parent / (FIXTURE.name + ".sha256")).read_text().split()[0] if REAL else None
SOMATIC_VCV = "VCV004530381.1-S-SCI-CP-T1"  # ADR 0004's measured example: `out` reaches 14 records


# --- helpers ---------------------------------------------------------------------------------------------------


def cli(capsys, *args) -> tuple[int, str, str]:
    rc = main([str(a) for a in args])
    got = capsys.readouterr()
    return rc, got.out, got.err


def kv(path: Path) -> dict[str, str]:
    rows = duckdb.connect().execute(f"SELECT key, value FROM parquet_kv_metadata('{path}')").fetchall()
    p = "clinvar_gkm_parquet."
    return {k.decode()[len(p) :]: v.decode() for k, v in rows if k.decode().startswith(p)}


def ids(path: Path, col: str = "id") -> list[str]:
    return [r[0] for r in duckdb.connect().execute(f"SELECT \"{col}\" FROM read_parquet('{path}')").fetchall()]


def parse_bundle(path: Path) -> dict[str, list[tuple[str, str]]]:
    """section -> [(key, record text)], in file order, with stdlib json (not the tool's reader)."""
    out: dict[str, list[tuple[str, str]]] = {}
    section = None
    dec = json.JSONDecoder()
    for line in gzip.decompress(Path(path).read_bytes()).decode().split("\n"):
        if line.startswith('  "') and line.endswith(": {"):
            section = json.loads(line.strip()[:-3])
            out[section] = []
        elif line.startswith("    "):
            key, end = dec.raw_decode(line[4:])
            out[section].append((key, line[4 + end + 2 :].removesuffix(",")))
    return out


def walk_refs(node, path: str = ""):
    """Every `#/<section>/<id>` string in document order, with the baseline refs path (ADR 0003)."""
    if isinstance(node, dict):
        for k, v in node.items():
            yield from walk_refs(v, f"{path}.{k}" if path else k)
    elif isinstance(node, list):
        for v in node:
            yield from walk_refs(v, path + "[]")
    elif isinstance(node, str) and node.startswith("#/"):
        section, _, key = node[2:].partition("/")
        yield {"path": path, "section": section, "id": key}


def built_keys(out: Path) -> set[tuple[str, str]]:
    """(section, key) of every row of a baseline build."""
    return {(s, k) for s in SECTIONS for k in ids(out / f"{s}.parquet")}


def bundle_keys(path: Path) -> set[tuple[str, str]]:
    return {(s, k) for s, recs in parse_bundle(path).items() for k, _ in recs}


def pointing_to(target: str) -> set[tuple[str, str]]:
    """Records of the fixture holding the pointer `target`."""
    return {
        (s, k)
        for s, recs in parse_bundle(FIXTURE).items()
        for k, r in recs
        if any(f"#/{x['section']}/{x['id']}" == target for x in walk_refs(json.loads(r)))
    }


def sha(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


# --- specs -----------------------------------------------------------------------------------------------------


def test_default_spec_is_baseline(tmp_path, capsys):
    out = tmp_path / "out"
    rc, stdout, _ = cli(capsys, "build", *FULL, "--out", out)
    assert rc == 0 and "built 20 table(s) of spec baseline 1.0, 331 rows" in stdout
    assert sorted(p.stem for p in out.glob("*.parquet")) == sorted(SECTIONS)
    records = parse_bundle(FIXTURE)
    con = duckdb.connect()
    for s in SECTIONS:
        got = con.execute(
            f"SELECT id, refs, CAST(data AS VARCHAR) FROM read_parquet('{out / s}.parquet') ORDER BY id"
        ).fetchall()
        want = sorted(records.get(s, []))
        assert [i for i, _, _ in got] == [k for k, _ in want]
        assert [d for _, _, d in got] == [r for _, r in want]  # data is the record, byte for byte
        assert [refs for _, refs, _ in got] == [list(walk_refs(json.loads(r))) for _, r in want]
    meta = kv(out / "scv.parquet")
    assert (meta["table_spec"], meta["table_spec_version"]) == ("baseline", "1.0")
    assert "selection" not in meta and "table_spec_builtin" not in meta
    m = json.loads((out / "manifest.json").read_text())
    spec_file = builtin_dir("baseline", "1.0") / "tables.yaml"
    assert m["table_spec"] == {"name": "baseline", "version": "1.0", "builtin": True, "sha256": sha(spec_file)}
    assert "selection" not in m
    assert cli(capsys, "verify", out)[0] == 0
    assert cli(capsys, "verify", out, "--bundle-recompute", *FULL, "--work-dir", tmp_path / "w")[0] == 0


def test_clinvar_gkm_spec(tmp_path, capsys):
    out = tmp_path / "out"
    rc, stdout, _ = cli(capsys, "build", *FULL, "--spec", "clinvar-gkm-1.0", "--out", out)
    assert rc == 0 and "built 22 table(s) of spec clinvar-gkm 1.0" in stdout
    assert len(list(out.glob("*.parquet"))) == 22
    meta = kv(out / "vcv_evidenceLine.parquet")
    assert (meta["table_spec"], meta["table_spec_version"]) == ("clinvar-gkm", "1.0")
    assert cli(capsys, "verify", out)[0] == 0
    assert cli(capsys, "verify", out, "--spec", "clinvar-gkm@1.0")[0] == 0
    rc, _, err = cli(capsys, "verify", out, "--spec", "baseline")
    assert rc == 2 and "written by clinvar-gkm 1.0" in err
    assert cli(capsys, "verify", out, "--bundle-recompute", *FULL, "--work-dir", tmp_path / "w")[0] == 0


def _custom_spec(tmp_path: Path, sections=("gene", "submitter", "scv")) -> Path:
    raw = yaml.safe_load((builtin_dir("baseline", "1.0") / "tables.yaml").read_text())
    raw["name"] = "my-subset"
    raw["tables"] = [t for t in raw["tables"] if t["name"] in sections]
    path = tmp_path / "my-subset.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return path


def test_custom_spec_file(tmp_path, capsys):
    spec = _custom_spec(tmp_path)
    out = tmp_path / "out"
    rc, stdout, _ = cli(capsys, "build", *FULL, "--spec", spec, "--out", out)
    assert rc == 0 and "built 3 table(s) of spec my-subset 1.0 (custom" in stdout
    assert sorted(p.stem for p in out.glob("*.parquet")) == ["gene", "scv", "submitter"]
    meta = kv(out / "gene.parquet")
    assert meta["table_spec"] == "my-subset" and meta["table_spec_builtin"] == "false"
    assert meta["table_spec_sha256"] == sha(spec)
    m = json.loads((out / "manifest.json").read_text())
    assert m["table_spec"] == {"name": "my-subset", "version": "1.0", "builtin": False, "sha256": sha(spec)}
    assert m["sections"]["vcv"] == len(parse_bundle(FIXTURE)["vcv"])  # every section still read and counted
    # verify needs the file, with the recorded sha256
    rc, _, err = cli(capsys, "verify", out)
    assert rc == 2 and "custom table spec my-subset 1.0" in err and "pass that tables.yaml with --spec" in err
    assert cli(capsys, "verify", out, "--spec", spec)[0] == 0
    rc = cli(capsys, "verify", out, "--spec", spec, "--bundle-recompute", *FULL, "--work-dir", tmp_path / "w")[0]
    assert rc == 0
    changed = tmp_path / "changed.yaml"
    changed.write_text(spec.read_text() + "# edited\n")
    rc, _, err = cli(capsys, "verify", out, "--spec", changed)
    assert rc == 2 and "but the files were written by custom table spec my-subset" in err
    # the oracle compares clinvar-gkm builds only
    rc, _, err = cli(capsys, "verify", out, "--spec", spec, "--against", out)
    assert rc == 2 and "needs a set built with the built-in clinvar-gkm spec" in err


# --- selective builds ------------------------------------------------------------------------------------------


def test_driver_where_on_baseline_follow_in(tmp_path, capsys):
    """--where over the baseline driver table's JSON `data`; `in` with depth 1 adds every record pointing to the
    driver record. Then select regenerates the mini-bundle, and verify recomputes the subset against it."""
    out = tmp_path / "out"
    where = "data->>'$.name' = 'OMIM'"
    opts = ["--driver", "submitter", "--where", where, "--follow", "in", "--max-depth", "1"]
    rc, stdout, _ = cli(capsys, "build", *FULL, "--out", out, *opts)
    assert rc == 0 and "a selection driven by submitter" in stdout
    omim = "clinvar.submitter:3"
    want = {("submitter", omim)} | pointing_to(f"#/submitter/{omim}")
    assert len(want) > 1 and built_keys(out) == want
    meta = kv(out / "scv.parquet")
    params = json.loads(meta["selection"])
    assert params["driver"] == "submitter" and params["where"] == where and params["ids_sha256"] is None
    assert (params["follow"], params["max_depth"], params["sections"]) == ("in", 1, None)
    assert params["seeds"] == {"submitter": {"count": 1, "sha256": sel.ids_sha256([omim])}}
    assert params["selected_records"] == len(want)
    # the source keys still name the published input
    assert meta["source_bundle_sha256"] == FIXTURE_SHA and meta["source_release"] == SOURCE_RELEASE
    assert meta["source_deltas"] == "[]"
    m = json.loads((out / "manifest.json").read_text())
    assert m["selection"] == params and m["inputs"]["full"]["sha256"] == FIXTURE_SHA
    assert sum(m["sections"].values()) == len(want)  # what the tables were built from: the mini-bundle
    assert cli(capsys, "verify", out)[0] == 0

    # select writes the same mini-bundle; the recompute runs against it
    mini = tmp_path / "mini.json.gz"
    rc, stdout, _ = cli(capsys, "select", *FULL, "--out", mini, *opts)
    assert rc == 0 and f"selection: {meta['selection']}" in stdout
    assert sha(mini) == params["selected_sha256"] and bundle_keys(mini) == want
    rc = cli(capsys, "verify", out, "--bundle-recompute", "--full", mini, "--work-dir", tmp_path / "w")[0]
    assert rc == 0
    rc, _, err = cli(capsys, "select", *FULL, "--out", mini, *opts)
    assert rc == 2 and "already exists" in err
    # recompute against anything but the mini-bundle is refused
    rc, _, err = cli(capsys, "verify", out, "--bundle-recompute", *FULL)
    assert rc == 2 and "is not the mini-bundle these files were built from" in err
    rc, _, err = cli(capsys, "verify", out, "--bundle-recompute", "--full", mini, "--delta", "x.json.gz")
    assert rc == 2 and "recompute against the mini-bundle alone" in err
    # the oracle refuses a subset
    rc, _, err = cli(capsys, "verify", out, "--against", out)
    assert rc == 2 and "is a selection" in err


def test_driver_where_on_clinvar_gkm(tmp_path, capsys):
    """--where over a typed column of the clinvar-gkm driver table (a struct field)."""
    out = tmp_path / "out"
    where = "quality.name = 'expert panel'"
    rc, _, _ = cli(
        capsys, "build", *FULL, "--spec", "clinvar-gkm-1.0", "--out", out, "--driver", "vcv", "--where", where
    )
    assert rc == 0
    want = {k for k, r in parse_bundle(FIXTURE)["vcv"] if json.loads(r)["quality"]["name"] == "expert panel"}
    assert want and set(ids(out / "vcv.parquet")) == want
    params = json.loads(kv(out / "vcv.parquet")["selection"])
    assert params["seeds"] == {"vcv": {"count": len(want), "sha256": sel.ids_sha256(want)}}
    assert len(list(out.glob("*.parquet"))) == 22
    assert cli(capsys, "verify", out)[0] == 0
    mini = tmp_path / "mini.json.gz"
    opts = ["--spec", "clinvar-gkm-1.0", "--driver", "vcv", "--where", where]
    assert cli(capsys, "select", *FULL, "--out", mini, *opts)[0] == 0
    assert sha(mini) == params["selected_sha256"]
    rc = cli(capsys, "verify", out, "--bundle-recompute", "--full", mini, "--work-dir", tmp_path / "w")[0]
    assert rc == 0


def test_ids_and_ids_file(tmp_path, capsys):
    scvs = [k for k, _ in parse_bundle(FIXTURE)["scv"]][:3]
    f = tmp_path / "ids.txt"
    f.write_text(f"{scvs[0]}\n\n  {scvs[1]}  \n")
    out = tmp_path / "out"
    opts = ["--driver", "scv", "--ids-file", f, "--ids", scvs[2], "--max-depth", "0"]
    assert cli(capsys, "build", *FULL, "--out", out, *opts)[0] == 0
    assert built_keys(out) == {("scv", k) for k in scvs}
    params = json.loads(kv(out / "gene.parquet")["selection"])
    assert params["where"] is None and params["ids_sha256"] == sel.ids_sha256(scvs)
    assert cli(capsys, "verify", out)[0] == 0


def test_where_and_ids_are_united(tmp_path, capsys):
    vcvs = [k for k, _ in parse_bundle(FIXTURE)["vcv"]]
    out = tmp_path / "out"
    opts = ["--driver", "vcv", "--where", f"id = '{vcvs[0]}'", "--ids", vcvs[1], "--max-depth", "0"]
    assert cli(capsys, "build", *FULL, "--out", out, *opts)[0] == 0
    assert set(ids(out / "vcv.parquet")) == {vcvs[0], vcvs[1]}


def test_no_where_and_no_ids_selects_every_driver_row(tmp_path, capsys):
    """A row-filter driver table (clinvar-gkm's vcv_evidenceLine) gives only its own rows, not its section's."""
    out = tmp_path / "out"
    opts = ["--spec", "clinvar-gkm-1.0", "--driver", "vcv_evidenceLine", "--max-depth", "0"]
    assert cli(capsys, "build", *FULL, "--out", out, *opts)[0] == 0
    full = tmp_path / "full"
    assert (
        cli(capsys, "build", *FULL, "--spec", "clinvar-gkm-1.0", "--out", full, "--tables", "vcv_evidenceLine")[0]
        == 0
    )
    want = set(ids(full / "vcv_evidenceLine.parquet"))
    assert want and set(ids(out / "vcv_evidenceLine.parquet")) == want
    assert ids(out / "rcv_evidenceLine.parquet") == ids(out / "evidenceLine.parquet") == []
    assert json.loads(kv(out / "vcv.parquet")["selection"])["seeds"]["evidenceLine"]["count"] == len(want)


@pytest.mark.parametrize("follow, depth", [("out", None), ("out", 2), ("both", 2), ("in", None)])
def test_follow_modes_depth_and_sections(tmp_path, capsys, follow, depth):
    """The build's mini-bundle is the engine's selection with the same options (the engine is checked against a
    breadth-first search in test_select.py); only --sections are written."""
    sections = ["vcv", "propositionSomaticClinicalImpact", "evidenceLine", "scv", "variation", "submitter"]
    opts = ["--driver", "vcv", "--ids", SOMATIC_VCV, "--follow", follow, "--sections", ",".join(sections)]
    if depth is not None:
        opts += ["--max-depth", depth]
    out = tmp_path / "out"
    assert cli(capsys, "build", *FULL, "--out", out, *opts)[0] == 0
    engine = sel.select(
        local_input(FIXTURE, sha=FIXTURE_SHA),
        {"vcv": [SOMATIC_VCV]},
        tmp_path / "engine.json.gz",
        follow=follow,
        max_depth=depth,
        sections=sections,
    )
    got = built_keys(out)
    assert got == bundle_keys(engine.path) and {s for s, _ in got} <= set(sections)
    params = json.loads(kv(out / "vcv.parquet")["selection"])
    assert params["selected_sha256"] == engine.sha256
    assert params["sections"] == [s for s in SECTIONS if s in sections]  # in bundle order
    assert (params["follow"], params["max_depth"]) == (follow, depth)
    rc = cli(capsys, "verify", out, "--bundle-recompute", "--full", engine.path, "--work-dir", tmp_path / "w")[0]
    assert rc == 0


# --- deltas ----------------------------------------------------------------------------------------------------


def _replayed_sections(deltas) -> dict[str, list[tuple[bytes, bytes]]]:
    """The full plus the deltas, replayed here by hand (design plan §4.3): per key, the latest delta wins; a
    delta's deletes apply before its upserts; the untouched full records keep their order, and the upserts follow
    in the order the deltas first touch each key."""
    state = fixture_sections()
    touched: dict[str, dict[str, tuple[bytes, bytes] | None]] = {}
    for d_inp, m_inp in deltas:
        man = json.loads(Path(m_inp.source).read_text())
        for section, entry in man["sections"].items():
            for k in entry["deleted"]:
                touched.setdefault(section, {})[k] = None
        for section, recs in parse_bundle(Path(d_inp.source)).items():
            for k, r in recs:
                touched.setdefault(section, {})[k] = (json.dumps(k).encode(), r.encode())
    out = {}
    for s in SECTIONS:
        t = touched.get(s, {})
        recs = [(k, r) for k, r in state.get(s, []) if json.loads(k) not in t]
        recs += [v for v in t.values() if v is not None]
        if recs:
            out[s] = recs
    return out


def test_replayed_selection_equals_selection_on_the_replayed_bundle(tmp_path):
    """A selection cut from a full plus deltas (replay.ReplayedSource) is byte for byte the selection cut from
    the replayed state written out as a bundle."""
    deltas = two_synthetic_deltas(tmp_path, SOURCE_RELEASE)
    replayed = _replayed_sections(deltas)
    materialized = write_gz(tmp_path / "replayed.json.gz", bundle_bytes(replayed))
    inputs = BuildInputs(
        full=local_input(FIXTURE, sha=FIXTURE_SHA), full_source_release=SOURCE_RELEASE, deltas=tuple(deltas)
    )
    s = fixture_sections()
    gene0, rcv0 = json.loads(s["gene"][0][0]), json.loads(s["rcv"][0][0])
    cases = [
        ({"gene": [gene0]}, {"follow": "in", "max_depth": 2}),
        ({"rcv": [rcv0]}, {"follow": "out"}),
        ({"rcv": [rcv0], "gene": [gene0]}, {"follow": "both", "max_depth": 2}),
    ]
    for i, (seeds, kw) in enumerate(cases):
        a = sel.select(ReplayedSource(inputs.full, load_chain(inputs)), seeds, tmp_path / f"a{i}.json.gz", **kw)
        b = sel.select(local_input(materialized), seeds, tmp_path / f"b{i}.json.gz", **kw)
        assert a.path.read_bytes() == b.path.read_bytes(), (seeds, kw)
        assert a.source_sha256 == FIXTURE_SHA and a.source_records == b.source_records
    # the replaced records are the deltas' versions; the deleted scv and the added-then-deleted gene are gone
    got = parse_bundle(tmp_path / "a2.json.gz")
    assert dict(got["gene"])[gene0] == s["gene"][1][1].decode()
    assert dict(got["rcv"])[rcv0] == s["rcv"][1][1].decode()
    assert "synthetic:gene-new" not in {k for k, _ in got["gene"]}


def test_build_with_deltas_and_a_driver(tmp_path, capsys):
    deltas = two_synthetic_deltas(tmp_path, SOURCE_RELEASE)
    s = fixture_sections()
    rcv0 = json.loads(s["rcv"][0][0])
    delta_args = [a for d, _ in deltas for a in ("--delta", d.source)]
    opts = ["--driver", "rcv", "--where", f"id = '{rcv0}'", "--follow", "out"]
    out = tmp_path / "out"
    rc, stdout, _ = cli(capsys, "build", *FULL, *delta_args, "--out", out, *opts)
    assert rc == 0 and f"source release {D2}" in stdout
    data = dict(
        duckdb.connect()
        .execute(f"SELECT id, CAST(data AS VARCHAR) FROM read_parquet('{out / 'rcv.parquet'}')")
        .fetchall()
    )
    assert data[rcv0] == s["rcv"][1][1].decode()  # D2's version of the driver record
    meta = kv(out / "rcv.parquet")
    assert json.loads(meta["source_deltas"]) == [
        {"release": D1, "sha256": sha(deltas[0][0].source)},
        {"release": D2, "sha256": sha(deltas[1][0].source)},
    ]
    assert meta["source_bundle_sha256"] == FIXTURE_SHA and meta["source_release"] == D2
    m = json.loads((out / "manifest.json").read_text())
    assert [d["release"] for d in m["inputs"]["deltas"]] == [D1, D2] and m["selection"] == json.loads(
        meta["selection"]
    )
    mini = tmp_path / "mini.json.gz"
    assert cli(capsys, "select", *FULL, *delta_args, "--out", mini, *opts)[0] == 0
    assert sha(mini) == json.loads(meta["selection"])["selected_sha256"]
    assert cli(capsys, "verify", out)[0] == 0
    rc = cli(capsys, "verify", out, "--bundle-recompute", "--full", mini, "--work-dir", tmp_path / "w")[0]
    assert rc == 0
    # --where sees the replayed state: D1 gave gene 0 gene 1's record, so both now match gene 1's id
    (g0, _), (g1, gr1) = s["gene"][:2]
    where = f"data->>'$.id' = '{json.loads(gr1)['id']}'"
    for extra, want in (([], {json.loads(g1)}), (delta_args, {json.loads(g0), json.loads(g1)})):
        o = tmp_path / f"genes-{len(extra)}"
        assert (
            cli(
                capsys, "build", *FULL, *extra, "--out", o, "--driver", "gene", "--where", where, "--max-depth", 0
            )[0]
            == 0
        )
        assert set(ids(o / "gene.parquet")) == want


# --- failures --------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "opts, message",
    [
        (["--driver", "nope"], "--driver nope: not a table of spec baseline 1.0"),
        (["--driver", "vcv", "--where", "id = 'x'; DROP TABLE t"], "a ';' is not allowed"),
        (
            ["--driver", "vcv", "--where", "id = 'no-such-vcv'"],
            "no driver keys: --where \"id = 'no-such-vcv'\" matches no row of driver table vcv",
        ),
        (
            ["--driver", "vcv", "--tables", "gene,vcv", "--sections", "vcv,evidenceLine"],
            "--tables and --sections conflict: gene reads gene, outside --sections vcv,evidenceLine",
        ),
        (["--driver", "vcv", "--sections", "gene"], "must include the driver's section vcv"),
        (["--where", "id = 'x'"], "--where need(s) --driver"),
        (["--driver", "vcv", "--where", "no_such_column = 1"], "--where 'no_such_column = 1' on driver table vcv"),
        (["--driver", "vcv", "--ids", "VCV-not-there"], "1 driver key(s) not in clinvar-gkm_excerpt.json.gz"),
    ],
)
def test_selection_failures(tmp_path, capsys, opts, message):
    out = tmp_path / "out"
    rc, _, err = cli(capsys, "build", *FULL, "--out", out, *opts)
    assert rc == 2 and message in err, err
    assert not out.exists() or not list(out.iterdir())  # no partial set
