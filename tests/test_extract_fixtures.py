"""scripts/extract-fixtures.py, run on the bundle fixture (the real 1.1.0 excerpt) and on a copy of it without its
sequenceReference section (written with write_bundle), whose location pointers dangle. A full bundle is too large
for the suite; these are the bundles it has. The checks here parse the output independently of the script (stdlib
json, not its pointer regex)."""

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest
from _fixture import FIXTURE, PROVENANCE
from _tables_helpers import read_fixture, write_bundle

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
def dangling(tmp_path_factory) -> Path:
    """The fixture without its sequenceReference records: every location's sequence pointer dangles."""
    path = tmp_path_factory.mktemp("in") / "clinvar-gkm_dangling.json.gz"
    return write_bundle({**read_fixture(), "sequenceReference": []}, path)


@pytest.fixture(params=["excerpt", "dangling"])
def source(request, dangling) -> Path:
    return FIXTURE if request.param == "excerpt" else dangling


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
    # A dangling pointer is one the source doesn't hold either; the excerpt has none, its trimmed copy some.
    assert not set(prov["dangling_pointers"]) & in_source
    assert bool(prov["dangling_pointers"]) == (source != FIXTURE)


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
    prov = run(FIXTURE, tmp_path)
    by = {e["shape"]: e for e in prov["shapes"]}
    # The excerpt lacks what its source lacked (its PROVENANCE.json): a few extension names, no plan §8.1 shape.
    missing = PROVENANCE["shapes_not_found"]
    assert missing and prov["shapes_not_found"] == missing
    for name in missing:
        assert by[name]["status"] == "not-found" and by[name]["candidates"] == 0
    assert by["germline-vcv"]["status"] == "selected" and len(by["germline-vcv"]["seeds"]) == 2
    assert {e["status"] for e in prov["shapes"]} <= {"selected", "covered", "not-found", "over-budget"}
    assert xf.main(_args(FIXTURE, tmp_path / "again")) == 0
    assert f"WARNING: shape {missing[0]}: not-found" in capsys.readouterr().err


def test_excerpt_of_the_excerpt_is_the_excerpt(tmp_path):
    """Run with the selection the fixture was cut with, the extractor gives back the fixture's bytes."""
    sel = PROVENANCE["selection"]
    seeds = [a for s in sel["explicit_seeds"] for a in ("--seed", s)]
    opts = ["--per-shape", sel["per_shape"], "--candidates", sel["candidates"]]
    run(FIXTURE, tmp_path, *map(str, opts), "--max-seed-bytes", str(sel["max_seed_bytes"]), *seeds)
    assert (tmp_path / NAME).read_bytes() == FIXTURE.read_bytes()


def test_every_shape_is_reported(tmp_path):
    prov = run(FIXTURE, tmp_path)
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


def test_extension_names_seen_are_covered(tmp_path):
    prov = run(FIXTURE, tmp_path)
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


def _ext_value(rec: dict, name: str):
    return next((e.get("value") for e in rec.get("extensions", []) if e["name"] == name), None)


def _real_record(section: str, pick) -> dict:
    """The first fixture record of `section` for which pick(rec) holds, parsed: a record not of the shape under
    test, chosen by its own fields, not by the script's predicate."""
    return next(r for r in (json.loads(t) for _, t in read_fixture()[section]) if pick(r))


def _set_ext(rec: dict, name: str, value) -> dict:
    exts = [e for e in rec["extensions"] if e["name"] != name]
    rec["extensions"] = [*exts, {"name": name, "value": value}]
    return rec


def _canonical(r: dict) -> bool:
    return _ext_value(r, "categoricalVariationType") == "CanonicalAllele"


def _simple(r: dict) -> bool:
    return _ext_value(r, "clinvarSubclassType") == "SimpleAllele"


@pytest.mark.parametrize(
    ("shape", "section", "pick", "change"),
    [
        (
            "copy-number-count",
            "variation",
            _canonical,
            lambda r: _set_ext(r, "categoricalVariationType", "CategoricalCnvCount"),
        ),
        (
            "copy-number-change",
            "variation",
            _canonical,
            lambda r: _set_ext(r, "categoricalVariationType", "CategoricalCnvChange"),
        ),
        ("haplotype", "variation", _simple, lambda r: _set_ext(r, "clinvarSubclassType", "Haplotype")),
        ("genotype", "variation", _simple, lambda r: _set_ext(r, "clinvarSubclassType", "Genotype")),
        (
            "uncertain-range-location",
            "location",
            lambda r: isinstance(r.get("start"), int) and isinstance(r.get("end"), int),
            lambda r: {**r, "start": [None, r["start"]]},
        ),
        (
            "therapy-group",
            "propositionSomaticClinicalImpact",
            lambda r: "#/therapyGroup/" not in json.dumps(r),
            lambda r: {**r, "objectTherapeutic": "#/therapyGroup/x"},
        ),
    ],
)
def test_shape_predicates_on_changed_real_records(shape, section, pick, change):
    s = {x.name: x for x in xf.shapes()}[shape]
    rec = _real_record(section, pick)
    before = json.dumps(rec, separators=(",", ":")).encode()
    after = json.dumps(change(rec), separators=(",", ":")).encode()
    key = b'"k"'

    def hit(text: bytes) -> bool:
        return s.matches_key_and_text(key, text) and (s.confirm is None or s.confirm(json.loads(text)))

    assert section in s.sections and not hit(before) and hit(after)
    # and the fixture holds records of the shape as published, unchanged
    assert any(hit(t.encode()) for _, t in read_fixture()[section])


def test_explicit_seed_is_included(tmp_path):
    fixture = read_fixture()
    gene, submitter = f"gene:{fixture['gene'][-1][0]}", f"submitter:{fixture['submitter'][-1][0]}"
    prov = run(FIXTURE, tmp_path, "--seed", gene, "--seed", submitter)
    seeds = {(x["section"], x["key"]): x["shapes"] for x in prov["seeds"]}
    for seed in (gene, submitter):
        assert "explicit" in seeds[tuple(seed.split(":", 1))]
    assert prov["selection"]["explicit_seeds"] == [gene, submitter]


def test_missing_explicit_seed_fails(tmp_path):
    with pytest.raises(xf.ExtractError, match="--seed not in the bundle: gene:ncbigene:0"):
        run(FIXTURE, tmp_path, "--seed", "gene:ncbigene:0")
    assert not tmp_path.exists() or not any(tmp_path.iterdir())


def test_seed_closures_over_budget_are_reported(tmp_path):
    prov = run(FIXTURE, tmp_path, "--max-seed-bytes", "1000")
    by = {e["shape"]: e for e in prov["shapes"]}
    assert by["rcv"]["status"] == "over-budget" and by["rcv"]["smallest_closure_bytes"] > 1000
    assert "rcv" in prov["shapes_over_budget"]
    assert all(s["closure_bytes"] <= 1000 for s in prov["seeds"])


def test_excerpt_over_max_bytes_fails_and_writes_nothing(tmp_path):
    with pytest.raises(xf.ExtractError, match="over --max-bytes 1,000"):
        run(FIXTURE, tmp_path / "out", "--max-bytes", "1000")
    assert not (tmp_path / "out").exists()


# ---------------------------------------------------------------------------------------------------------------
# Inputs: checksum, stdin, re-opened passes


def test_sha256_mismatch_fails_and_writes_nothing(tmp_path):
    with pytest.raises(xf.ExtractError, match="sha256 mismatch: computed .*, published 0000"):
        run(FIXTURE, tmp_path / "out", "--sha256", "0" * 64)
    assert not (tmp_path / "out").exists()
    assert xf.main(_args(FIXTURE, tmp_path / "out", "--sha256", "0" * 64)) == 1


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
    res = _cli(_args("-", tmp_path / "out"), stdin=FIXTURE)
    assert res.returncode == 1
    assert "stdin can't be read twice" in res.stderr and "--reopen" in res.stderr
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("every", [xf.CHECKPOINT_BYTES, 1000])
def test_reopened_source_must_match_pass_1(tmp_path, dangling, monkeypatch, every):
    monkeypatch.setattr(xf, "CHECKPOINT_BYTES", every)
    run(FIXTURE, tmp_path / "same", "--reopen", str(FIXTURE))
    with pytest.raises(xf.ExtractError, match="re-opened source differs from the pass-1 stream"):
        run(FIXTURE, tmp_path / "other", "--reopen", str(dangling))
    assert not (tmp_path / "other").exists()


def test_small_checkpoints_give_the_same_excerpt(tmp_path, monkeypatch):
    run(FIXTURE, tmp_path / "a")
    monkeypatch.setattr(xf, "CHECKPOINT_BYTES", 512)
    run(FIXTURE, tmp_path / "b")
    assert (tmp_path / "a" / NAME).read_bytes() == (tmp_path / "b" / NAME).read_bytes()


def test_unsupported_schema_version_fails(tmp_path):
    text = gzip.decompress(FIXTURE.read_bytes()).replace(
        b'"schema_version": "1.1.0"', b'"schema_version": "9.9.9"', 1
    )
    path = tmp_path / "b.json.gz"
    path.write_bytes(gzip.compress(text, mtime=0))
    with pytest.raises(xf.ExtractError, match="schema_version '9.9.9' is not one this tool reads"):
        run(path, tmp_path / "out")


def test_bad_seed_syntax(tmp_path):
    with pytest.raises(xf.ExtractError, match="expected section:key"):
        run(FIXTURE, tmp_path, "--seed", "nosuchsection:x")
