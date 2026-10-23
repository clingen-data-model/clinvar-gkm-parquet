"""scripts/extract-fixtures.py, run on the provisional fixture and on its current-shape copy (current_shape(),
written with write_bundle). No real 1.1.0 full is available yet; these are the bundles the suite has. The checks
here parse the output independently of the script (stdlib json, not its pointer regex)."""

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest
from _fixture import PROVISIONAL
from _tables_helpers import current_shape, read_fixture, write_bundle

from clinvar_gkm_parquet.bundle import stage_bundle
from clinvar_gkm_parquet.model import InputFile

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "extract-fixtures.py"
_spec = importlib.util.spec_from_file_location("extract_fixtures", SCRIPT)
xf = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = xf  # dataclasses look the module up
_spec.loader.exec_module(xf)

NAME = xf.DEFAULT_NAME


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module")
def current(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("in") / "clinvar-gkm_current-shape.json.gz"
    return write_bundle(current_shape(read_fixture(PROVISIONAL)), path)


@pytest.fixture(params=["provisional", "current-shape"])
def source(request, current) -> Path:
    return PROVISIONAL if request.param == "provisional" else current


def _args(src, out: Path, *extra: str) -> list[str]:
    return [
        str(src),
        "--source-url",
        f"file://{Path(src).name}",
        "--release",
        "2026-10-18",
        "--out",
        str(out),
        *extra,
    ]


def run(src, out: Path, *extra: str) -> dict:
    return xf.extract(xf.parse_args(_args(src, out, *extra)), log=lambda msg: None)


def parse_bundle(path: Path) -> tuple[list[str], dict[str, list[tuple[bytes, bytes]]]]:
    """(section order, {section: [(key JSON bytes, record bytes)]}) with stdlib json, independent of the script."""
    order, out, section = [], {}, None
    dec = json.JSONDecoder()
    for line in gzip.decompress(path.read_bytes()).split(b"\n"):
        if line.startswith(b'  "') and line.endswith(b": {"):
            section = json.loads(line.strip()[:-3])
            order.append(section)
            out[section] = []
        elif line.startswith(b"    "):
            text = line[4:].decode()
            _, end = dec.raw_decode(text)
            key = text[:end].encode()
            record = line[4 + len(key) + 2 :].removesuffix(b",")
            out[section].append((key, record))
    return order, out


def pointers(node, found: set[str]) -> set[str]:
    if isinstance(node, str):
        if node.startswith("#/"):
            found.add(node)
    elif isinstance(node, dict):
        for v in node.values():
            pointers(v, found)
    elif isinstance(node, list):
        for v in node:
            pointers(v, found)
    return found


# ---------------------------------------------------------------------------------------------------------------
# The excerpt


def test_closure_is_complete(tmp_path, source):
    prov = run(source, tmp_path)
    _, out = parse_bundle(tmp_path / NAME)
    _, src = parse_bundle(source)
    present = {f"#/{s}/{json.loads(k)}" for s, recs in out.items() for k, _ in recs}
    in_source = {f"#/{s}/{json.loads(k)}" for s, recs in src.items() for k, _ in recs}
    referenced = set()
    for recs in out.values():
        for _, r in recs:
            pointers(json.loads(r), referenced)
    assert referenced and referenced - present == set(prov["dangling_pointers"])
    # A dangling pointer is one the source doesn't hold either (the provisional examples kept a few).
    assert not set(prov["dangling_pointers"]) & in_source


def test_record_bytes_are_verbatim_in_source_order(tmp_path, source):
    run(source, tmp_path)
    order, out = parse_bundle(tmp_path / NAME)
    src_order, src = parse_bundle(source)
    assert order == [s for s in src_order if s in out]
    for section, recs in out.items():
        position = {k: i for i, (k, _) in enumerate(src[section])}
        assert [position[k] for k, _ in recs] == sorted(position[k] for k, _ in recs)
        assert all((k, r) in set(src[section]) for k, r in recs)


def test_output_is_a_valid_bundle(tmp_path, source):
    prov = run(source, tmp_path)
    path = tmp_path / NAME
    assert (tmp_path / (NAME + ".sha256")).read_text() == f"{_sha(path)}  {NAME}\n"
    assert prov["sha256"] == _sha(path)
    inp = InputFile(str(path), NAME, "2026-10-18", _sha(path), "explicit", path.stat().st_size)
    staged = stage_bundle(inp, tmp_path / "work")
    assert {s: sec.records for s, sec in staged.sections.items()} == prov["counts"]
    assert staged.schema_version == prov["schema_version"] == "1.1.0"
    assert json.loads((tmp_path / "PROVENANCE.json").read_text()) == prov


def test_provenance_records_the_source(tmp_path, source):
    prov = run(source, tmp_path, "--sha256", _sha(source))
    src = prov["source"]
    assert src["url"] == f"file://{source.name}" and src["release"] == "2026-10-18"
    assert src["sha256"] == src["sha256_published"] == _sha(source)
    assert src["size"] == source.stat().st_size and src["schema_version"] == "1.1.0"
    assert src["records"] == sum(len(v) for v in parse_bundle(source)[1].values())
    assert prov["generator"] == "scripts/extract-fixtures.py" and prov["script_version"] == xf.SCRIPT_VERSION
    assert prov["tool_version"] and prov["table_spec_version"] == "1.0"
    assert prov["passes"] >= 2
    for seed in prov["seeds"]:
        assert seed["shapes"] and seed["closure_bytes"] > 0


def test_byte_identical_across_runs(tmp_path, source):
    run(source, tmp_path / "a")
    run(source, tmp_path / "b")
    files = sorted(p.name for p in (tmp_path / "a").iterdir())
    assert files == sorted([NAME, NAME + ".sha256", "PROVENANCE.json"])
    for name in files:
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes(), name
    assert gzip.open(tmp_path / "a" / NAME).read()  # mtime 0 is what makes this hold
    assert (tmp_path / "a" / NAME).read_bytes()[4:8] == b"\0\0\0\0"


# ---------------------------------------------------------------------------------------------------------------
# Shapes and seeds


def test_shapes_with_no_match_are_reported(tmp_path, capsys):
    prov = run(PROVISIONAL, tmp_path)
    by = {e["shape"]: e for e in prov["shapes"]}
    # The provisional fixture has no copy-number, therapy-group or range-location records (its README).
    for name in ("copy-number-count", "copy-number-change", "therapy-group", "uncertain-range-location"):
        assert by[name]["status"] == "not-found" and by[name]["candidates"] == 0
        assert name in prov["shapes_not_found"]
    assert by["germline-vcv"]["status"] == "selected" and len(by["germline-vcv"]["seeds"]) == 2
    assert {e["status"] for e in prov["shapes"]} <= {"selected", "covered", "not-found", "over-budget"}
    assert xf.main(_args(PROVISIONAL, tmp_path / "again")) == 0
    assert "WARNING: shape copy-number-count: not-found" in capsys.readouterr().err


def test_every_shape_is_reported(tmp_path):
    prov = run(PROVISIONAL, tmp_path)
    assert [e["shape"] for e in prov["shapes"]] == [s.name for s in xf.shapes() if not s.extension] + [
        s.name for s in xf.shapes() if s.extension
    ]


def test_extension_shapes_cover_the_spec_slot_maps():
    from clinvar_gkm_parquet.spec import load_spec

    names = {s.name for s in xf.shapes() if s.extension}
    want = {
        f"ext:{t.name}.{col}:{n}"
        for t in load_spec("1.0").tables
        for col, m in t.extension_slots.items()
        for n in m
    }
    assert names == want and any(n.startswith("ext:scv.") for n in names)


def test_extension_names_seen_are_covered(tmp_path, current):
    prov = run(current, tmp_path)
    _, out = parse_bundle(tmp_path / NAME)
    by = {e["shape"]: e for e in prov["shapes"]}
    for name, col in (
        ("submittedCondition", "extensions"),
        ("clinvarScvId", "extensions"),
        ("description", "classification"),
    ):
        e = by[f"ext:scv.{col}:{name}"]
        assert e["status"] in ("selected", "covered")
        holder = e.get("covered_by") or e["seeds"][0]
        section, _, key = holder.partition(":")
        rec = dict(out[section])[json.dumps(key).encode()]
        assert name in xf.ext_names({col: json.loads(rec).get(col)}, set())


def _real_record(section: str, index: int = 0) -> dict:
    """One record of the provisional fixture, parsed (the shape predicates are tested on real records changed
    minimally where the fixture has no record of that shape)."""
    return json.loads(read_fixture(PROVISIONAL)[section][index][1])


def _set_ext(rec: dict, name: str, value) -> dict:
    exts = [e for e in rec["extensions"] if e["name"] != name]
    rec["extensions"] = [*exts, {"name": name, "value": value}]
    return rec


@pytest.mark.parametrize(
    ("shape", "section", "change"),
    [
        (
            "copy-number-count",
            "variation",
            lambda r: _set_ext(r, "categoricalVariationType", "CategoricalCnvCount"),
        ),
        (
            "copy-number-change",
            "variation",
            lambda r: _set_ext(r, "categoricalVariationType", "CategoricalCnvChange"),
        ),
        ("haplotype", "variation", lambda r: _set_ext(r, "clinvarSubclassType", "Haplotype")),
        ("genotype", "variation", lambda r: _set_ext(r, "clinvarSubclassType", "Genotype")),
        ("uncertain-range-location", "location", lambda r: {**r, "start": [None, r["start"]]}),
        (
            "therapy-group",
            "propositionSomaticClinicalImpact",
            lambda r: {**r, "objectTherapeutic": "#/therapyGroup/x"},
        ),
    ],
)
def test_shape_predicates_on_changed_real_records(shape, section, change):
    s = {x.name: x for x in xf.shapes()}[shape]
    rec = _real_record(section)
    before = json.dumps(rec, separators=(",", ":")).encode()
    after = json.dumps(change(rec), separators=(",", ":")).encode()
    key = b'"k"'

    def hit(text: bytes) -> bool:
        return s.matches_key_and_text(key, text) and (s.confirm is None or s.confirm(json.loads(text)))

    assert section in s.sections and not hit(before) and hit(after)


def test_explicit_seed_is_included(tmp_path):
    prov = run(
        PROVISIONAL, tmp_path, "--seed", "gene:ncbigene:9132", "--seed", "submitter:clinvar.submitter:500062"
    )
    seeds = {(x["section"], x["key"]): x["shapes"] for x in prov["seeds"]}
    assert "explicit" in seeds[("gene", "ncbigene:9132")]
    assert "explicit" in seeds[("submitter", "clinvar.submitter:500062")]
    assert prov["selection"]["explicit_seeds"] == ["gene:ncbigene:9132", "submitter:clinvar.submitter:500062"]


def test_missing_explicit_seed_fails(tmp_path):
    with pytest.raises(xf.ExtractError, match="--seed not in the bundle: gene:ncbigene:0"):
        run(PROVISIONAL, tmp_path, "--seed", "gene:ncbigene:0")
    assert not tmp_path.exists() or not any(tmp_path.iterdir())


def test_seed_closures_over_budget_are_reported(tmp_path):
    prov = run(PROVISIONAL, tmp_path, "--max-seed-bytes", "1000")
    by = {e["shape"]: e for e in prov["shapes"]}
    assert by["rcv"]["status"] == "over-budget" and by["rcv"]["smallest_closure_bytes"] > 1000
    assert "rcv" in prov["shapes_over_budget"]
    assert all(s["closure_bytes"] <= 1000 for s in prov["seeds"])


def test_excerpt_over_max_bytes_fails_and_writes_nothing(tmp_path):
    with pytest.raises(xf.ExtractError, match="over --max-bytes 1,000"):
        run(PROVISIONAL, tmp_path / "out", "--max-bytes", "1000")
    assert not (tmp_path / "out").exists()


# ---------------------------------------------------------------------------------------------------------------
# Inputs: checksum, stdin, re-opened passes


def test_sha256_mismatch_fails_and_writes_nothing(tmp_path):
    with pytest.raises(xf.ExtractError, match="sha256 mismatch: computed .*, published 0000"):
        run(PROVISIONAL, tmp_path / "out", "--sha256", "0" * 64)
    assert not (tmp_path / "out").exists()
    assert xf.main(_args(PROVISIONAL, tmp_path / "out", "--sha256", "0" * 64)) == 1


def _cli(args: list[str], stdin: Path) -> subprocess.CompletedProcess:
    with open(stdin, "rb") as fh:
        return subprocess.run([sys.executable, str(SCRIPT), *args], stdin=fh, capture_output=True, text=True)


def test_stdin_with_reopen_matches_path_mode(tmp_path, source):
    run(source, tmp_path / "path")
    for mode, extra in (("reopen", ["--reopen", str(source)]), ("cmd", ["--reopen-cmd", f"cat {source}"])):
        args = _args("-", tmp_path / mode, *extra)
        args[2] = f"file://{source.name}"  # the same --source-url as path mode
        res = _cli(args, stdin=source)
        assert res.returncode == 0, res.stderr
        assert "passes in" in res.stderr and "peak memory" in res.stderr
        for name in (NAME, NAME + ".sha256", "PROVENANCE.json"):
            assert (tmp_path / mode / name).read_bytes() == (tmp_path / "path" / name).read_bytes(), (mode, name)


def test_stdin_without_reopen_fails(tmp_path):
    res = _cli(_args("-", tmp_path / "out"), stdin=PROVISIONAL)
    assert res.returncode == 1
    assert "stdin can't be read twice" in res.stderr and "--reopen" in res.stderr
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("every", [xf.CHECKPOINT_BYTES, 1000])
def test_reopened_source_must_match_pass_1(tmp_path, current, monkeypatch, every):
    monkeypatch.setattr(xf, "CHECKPOINT_BYTES", every)
    run(PROVISIONAL, tmp_path / "same", "--reopen", str(PROVISIONAL))
    with pytest.raises(xf.ExtractError, match="re-opened source differs from the pass-1 stream"):
        run(PROVISIONAL, tmp_path / "other", "--reopen", str(current))
    assert not (tmp_path / "other").exists()


def test_small_checkpoints_give_the_same_excerpt(tmp_path, monkeypatch):
    run(PROVISIONAL, tmp_path / "a")
    monkeypatch.setattr(xf, "CHECKPOINT_BYTES", 512)
    run(PROVISIONAL, tmp_path / "b")
    assert (tmp_path / "a" / NAME).read_bytes() == (tmp_path / "b" / NAME).read_bytes()


def test_unsupported_schema_version_fails(tmp_path):
    text = gzip.decompress(PROVISIONAL.read_bytes()).replace(
        b'"schema_version": "1.1.0"', b'"schema_version": "9.9.9"', 1
    )
    path = tmp_path / "b.json.gz"
    path.write_bytes(gzip.compress(text, mtime=0))
    with pytest.raises(xf.ExtractError, match="schema_version '9.9.9' is not one this tool reads"):
        run(path, tmp_path / "out")


def test_bad_seed_syntax(tmp_path):
    with pytest.raises(xf.ExtractError, match="expected section:key"):
        run(PROVISIONAL, tmp_path, "--seed", "nosuchsection:x")
