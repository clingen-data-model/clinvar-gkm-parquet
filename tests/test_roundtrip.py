"""The round trip (ADR 0005): a full sections build is a lossless encoding of the bundle's uncompressed text.

`rebuild` writes the excerpt back byte for byte (the expectation is the fixture's own text, decompressed by
stdlib gzip); the recorded sha256s are recomputed here from that text with hashlib; and every refusal and
build-time failure is checked on the real excerpt, changed minimally where a test says so. Nothing here is a
hand-written record.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import shutil
from pathlib import Path

import duckdb
import pytest
from _fixture import FIXTURE, SOURCE_RELEASE
from _input_helpers import local_input, synthetic_delta
from _tables_helpers import connect, current_shape, read_fixture

from clinvar_gkm_parquet import bundle
from clinvar_gkm_parquet.build import EMPTY_SHA256, KV_PREFIX
from clinvar_gkm_parquet.cli import main
from clinvar_gkm_parquet.compat import SCHEMA_SECTIONS
from clinvar_gkm_parquet.errors import RoundTripError, SpecError
from clinvar_gkm_parquet.roundtrip import RoundTripMismatch, lossless_problems, rebuild, record_sql
from clinvar_gkm_parquet.spec import parse_spec, resolve_spec
from clinvar_gkm_parquet.sqltemplate import render_generic

REAL = FIXTURE.parent.name == "real-1.1.0"
real_only = pytest.mark.skipif(not REAL, reason="reads the real 1.1.0 excerpt")
SPEC, _ = resolve_spec("sections")
TEXT = gzip.decompress(FIXTURE.read_bytes())
FULL = ["--full", FIXTURE, "--full-source-release", SOURCE_RELEASE]


def cli(capsys, *args) -> tuple[int, str, str]:
    rc = main([str(a) for a in args])
    got = capsys.readouterr()
    return rc, got.out, got.err


def section_texts(text: bytes) -> dict[str, bytes]:
    """section -> its record lines as written (each with its ',' and newline), from the text with re alone."""
    out = {}
    for m in re.finditer(rb'^  "(\w+)": \{\n(.*?)^  \}', text, re.M | re.S):
        out[m.group(1).decode()] = m.group(2)
    return out


def kv_of(path: Path) -> dict[str, str]:
    rows = duckdb.connect().execute(f"SELECT key, value FROM parquet_kv_metadata('{path}')").fetchall()
    return {k.decode()[len(KV_PREFIX) :]: v.decode() for k, v in rows if k.decode().startswith(KV_PREFIX)}


def rewrite(path: Path, select: str) -> None:
    """Rewrite a built file with `select` (over `t`), keeping its key-value metadata: a tampered file."""
    con = duckdb.connect()
    kv = con.execute(f"SELECT key, value FROM parquet_kv_metadata('{path}')").fetchall()
    meta = ", ".join(f"'{k.decode()}': '{v.decode().replace(chr(39), chr(39) * 2)}'" for k, v in kv)
    tmp = path.with_name(path.name + ".new")
    con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{path}')")
    con.execute(f"COPY ({select}) TO '{tmp}' (FORMAT parquet, KV_METADATA {{{meta}}})")
    con.close()
    tmp.replace(path)


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> Path:
    if not REAL:
        pytest.skip("reads the real 1.1.0 excerpt")
    out = tmp_path_factory.mktemp("rt") / "sections"
    assert main(["build", *map(str, FULL), "--out", str(out)]) == 0
    return out


@pytest.fixture
def copy(built, tmp_path) -> Path:
    d = tmp_path / "copy"
    shutil.copytree(built, d)
    return d


# --- the spec --------------------------------------------------------------------------------------------------


def test_sections_spec_is_lossless():
    assert SPEC.lossless and lossless_problems(SPEC) == []
    for t in SPEC.tables:
        assert (
            t.json is not None and t.columns[-1].name == "ordinal" and t.columns[-1].derivation.kind == "ordinal"
        )
        for c in t.columns_of("bare-id"):
            assert c.derivation.targets, (t.name, c.name)
            if len(c.derivation.targets) > 1:  # its section column follows it
                sc = t.columns[t.columns.index(c) + 1]
                assert (sc.name, sc.derivation.kind, sc.derivation.path) == (
                    f"{c.name}_section",
                    "pointer-section",
                    c.derivation.path,
                )
        for c in t.columns:
            assert len(c.derivation.pointer_targets) == len(c.derivation.pointers)


def _raw_sections() -> dict:
    import yaml

    from clinvar_gkm_parquet.spec import SPEC_FILE, builtin_dir

    return yaml.safe_load((builtin_dir("sections", "1.0") / SPEC_FILE).read_text())


@pytest.mark.parametrize(
    "change, message",
    [
        (lambda r: r["tables"].pop(), "must read each section of bundle schema 1.1.0 once"),
        (lambda r: r["tables"][0].pop("json"), "table sequenceReference: needs a json block"),
        (lambda r: r["tables"][0]["columns"].pop(), "needs one ordinal column, as its last column"),
        (lambda r: r["tables"][1]["columns"][-2]["derivation"].pop("targets"), "needs its target section(s)"),
        (lambda r: r["tables"][6]["columns"].pop(5), "needs a pointer-section column of the same path"),
        (
            lambda r: r["tables"][6]["columns"][3]["derivation"].update(pointers=["[*].allele", "[*].location"]),
            "pointers need their target sections",
        ),
        (lambda r: r["tables"][6]["extension_slots"].update(extensions={"x": "value_string"}), "json-type"),
        (lambda r: r["tables"][1]["columns"].pop(3), "needs all of range-exact, range-min, range-max"),
    ],
)
def test_lossless_spec_rules(change, message):
    from clinvar_gkm_parquet.spec import check_rules

    raw = _raw_sections()
    change(raw)
    spec = parse_spec(raw, require_name=True)
    with pytest.raises(SpecError, match=re.escape(message)):
        check_rules(spec, builtin=True)


def test_record_sql_refuses_a_float_column():
    raw = _raw_sections()
    raw["tables"][3]["columns"][3]["type"] = "DOUBLE"  # copyNumberCount.copies
    with pytest.raises(SpecError, match="no exact JSON text for type DOUBLE"):
        record_sql(parse_spec(raw).table("copyNumberCount"))


# --- the round trip on the excerpt -----------------------------------------------------------------------------


@real_only
def test_rebuild_is_byte_identical(built, tmp_path, capsys):
    for name in ("bundle.json", "bundle.json.gz"):
        rc, out, _ = cli(capsys, "rebuild", built, "-o", tmp_path / name)
        assert rc == 0 and hashlib.sha256(TEXT).hexdigest() in out
    assert (tmp_path / "bundle.json").read_bytes() == TEXT
    assert gzip.decompress((tmp_path / "bundle.json.gz").read_bytes()) == TEXT
    assert sorted(p.name for p in tmp_path.iterdir()) == ["bundle.json", "bundle.json.gz"]  # no partial files


@real_only
def test_build_records_the_text(built):
    manifest = json.loads((built / "manifest.json").read_text())
    assert manifest["source_text_sha256"] == hashlib.sha256(TEXT).hexdigest()
    assert manifest["source_text_size"] == len(TEXT)
    texts = section_texts(TEXT)
    for t in SPEC.tables:
        kv = kv_of(built / t.file_name)
        want = hashlib.sha256(texts[t.name]).hexdigest() if t.name in texts else EMPTY_SHA256
        assert kv["section_text_sha256"] == want, t.name


@real_only
def test_ordinal_is_the_bundle_order(built):
    con = duckdb.connect()
    for section, records in read_fixture().items():
        got = con.execute(f"SELECT id FROM read_parquet('{built / (section + '.parquet')}') ORDER BY ordinal")
        assert [r[0] for r in got.fetchall()] == [k for k, _ in records], section


def test_stream_bundle_text_digests():
    class Sink:
        def __init__(self):
            self.texts = {}

        def begin(self, v):
            pass

        def open_section(self, s):
            return lambda k, r: None

        def section_text(self, s, sha):
            self.texts[s] = sha

        def close_section(self, s):
            pass

    sink = Sink()
    got = bundle.stream_bundle(local_input(FIXTURE), sink, text_digests=True)
    assert (got.text_sha256, got.text_size) == (hashlib.sha256(TEXT).hexdigest(), len(TEXT))
    assert sink.texts == {s: hashlib.sha256(b).hexdigest() for s, b in section_texts(TEXT).items()}
    assert bundle.stream_bundle(local_input(FIXTURE), Sink()).text_sha256 is None


@real_only
def test_verify_roundtrip(built, capsys):
    rc, out, _ = cli(capsys, "verify", built, "--roundtrip", "--full", FIXTURE)
    assert rc == 0 and "round trip: identical" in out


# --- rebuild refuses what it can't reproduce -------------------------------------------------------------------


@real_only
@pytest.mark.parametrize("spec", ["baseline", "clinvar-gkm"])
def test_rebuild_refuses_other_specs(tmp_path, capsys, spec):
    out = tmp_path / spec
    assert cli(capsys, "build", *FULL, "--spec", spec, "--out", out)[0] == 0
    assert not any("section_text_sha256" in kv_of(p) for p in out.glob("*.parquet"))
    assert "source_text_sha256" not in json.loads((out / "manifest.json").read_text())
    with pytest.raises(RoundTripError, match="not a lossless encoding"):
        rebuild(out, tmp_path / "b.json")


@real_only
def test_rebuild_refuses_a_table_subset(tmp_path, capsys):
    out = tmp_path / "subset"
    assert cli(capsys, "build", *FULL, "--tables", "gene,scv", "--out", out)[0] == 0
    with pytest.raises(RoundTripError, match="missing table"):
        rebuild(out, tmp_path / "b.json")


@real_only
def test_rebuild_refuses_a_selection(tmp_path, capsys):
    out = tmp_path / "sel"
    assert cli(capsys, "build", *FULL, "--driver", "submitter", "--max-depth", "0", "--out", out)[0] == 0
    assert not any("section_text_sha256" in kv_of(p) for p in out.glob("*.parquet"))
    with pytest.raises(RoundTripError, match="selective build"):
        rebuild(out, tmp_path / "b.json")


@real_only
def test_rebuild_refuses_deltas(tmp_path, capsys):
    scv0 = read_fixture()["scv"][0][0]
    d, _ = synthetic_delta(tmp_path, "2026-10-25", SOURCE_RELEASE, {}, {"scv": [scv0]})
    out = tmp_path / "replayed"
    assert cli(capsys, "build", *FULL, "--delta", d.source, "--out", out)[0] == 0
    assert not any("section_text_sha256" in kv_of(p) for p in out.glob("*.parquet"))
    with pytest.raises(RoundTripError, match="deltas replayed"):
        rebuild(out, tmp_path / "b.json")


@real_only
def test_rebuild_refuses_a_missing_table(copy, tmp_path):
    (copy / "gene.parquet").unlink()
    with pytest.raises(RoundTripError, match="missing table"):
        rebuild(copy, tmp_path / "b.json")


@real_only
def test_rebuild_refuses_a_missing_ordinal(copy, tmp_path):
    rewrite(
        copy / "gene.parquet",
        "SELECT * REPLACE (CASE WHEN ordinal = 3 THEN 99 ELSE ordinal END AS ordinal) FROM t",
    )
    with pytest.raises(RoundTripError, match=r"ordinals are not 0\.\.59"):
        rebuild(copy, tmp_path / "b.json")
    assert not (tmp_path / "b.json").exists()


@real_only
def test_rebuild_refuses_a_pointer_outside_its_targets(copy, tmp_path):
    rewrite(copy / "scv.parquet", "SELECT * REPLACE ('evidenceLine' AS proposition_section) FROM t")
    with pytest.raises(RoundTripError, match="names section evidenceLine, not one of"):
        rebuild(copy, tmp_path / "b.json")
    assert not list(tmp_path.glob("*b.json*"))


@real_only
def test_rebuild_refuses_a_changed_value(copy, tmp_path):
    rewrite(copy / "submitter.parquet", "SELECT * REPLACE (upper(name) AS name) FROM t")
    with pytest.raises(RoundTripMismatch, match="section submitter: the rebuilt record lines have sha256"):
        rebuild(copy, tmp_path / "b.json")
    assert not list(tmp_path.glob("*b.json*"))


@real_only
def test_rebuild_refuses_a_changed_manifest(copy, tmp_path):
    m = json.loads((copy / "manifest.json").read_text())
    m["source_text_sha256"] = "0" * 64
    (copy / "manifest.json").write_text(json.dumps(m))
    with pytest.raises(RoundTripMismatch, match="the manifest records source_text_sha256 0000"):
        rebuild(copy, tmp_path / "b.json")


@real_only
def test_rebuild_refuses_a_missing_manifest(copy, tmp_path):
    (copy / "manifest.json").unlink()
    with pytest.raises(RoundTripError, match="manifest.json: missing"):
        rebuild(copy, tmp_path / "b.json")
    assert not (tmp_path / "b.json").exists()


@real_only
def test_rebuild_refuses_an_existing_output(built, tmp_path):
    (tmp_path / "b.json").write_text("x")
    with pytest.raises(RoundTripError, match="already exists"):
        rebuild(built, tmp_path / "b.json")


@real_only
def test_verify_roundtrip_reports_a_difference(copy, capsys):
    rewrite(copy / "submitter.parquet", "SELECT * REPLACE (upper(name) AS name) FROM t")
    rc, out, _ = cli(capsys, "verify", copy, "--roundtrip")
    assert rc == 1 and "round trip: DIFFERS" in out and "section submitter" in out


# --- the build's round-trip check ------------------------------------------------------------------------------

BASE = current_shape(read_fixture()) if REAL else {}


def _build_one(section: str, edit) -> list[dict]:
    """Build one sections table from the excerpt with the first record of `section` changed by edit(text)."""
    sections = {s: list(r) for s, r in BASE.items()}
    k, r = sections[section][0]
    sections[section][0] = (k, edit(r))
    con = connect(sections)
    cur = con.execute(f"SELECT * FROM ({render_generic(SPEC.table(section))}) ORDER BY ordinal")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def _obj(edit):
    return lambda r: json.dumps(edit(json.loads(r)), ensure_ascii=False, separators=(",", ":"))


@real_only
@pytest.mark.parametrize(
    "section, edit",
    [
        ("scv", _obj(lambda o: {**o, "description": None})),  # a JSON null: read as absent
        ("submitter", _obj(lambda o: dict(reversed(list(o.items()))))),  # keys out of BigQuery's sorted order
        ("scv", _obj(lambda o: dict(sorted(o.items())))),  # keys out of the STRUCT order
        ("copyNumberCount", lambda r: r.replace('"copies":', '"copies":1.0,"x":').replace(',"x":', "", 1)),
        ("location", _obj(lambda o: {**o, "start": [None, None]})),  # an open Range: no column tells it apart
        ("allele", _obj(lambda o: {**o, "location": o["location"].replace("#/location/", "#/gene/")})),
        ("submitter", lambda r: r.replace(":", ": ", 1)),  # not the producer's compact layout
        ("gene", lambda r: r.replace("ncbigene", "\\u006ecbigene", 1)),  # an escape the producer does not write
    ],
)
def test_build_check_refuses_what_it_cannot_write_back(section, edit):
    with pytest.raises(duckdb.Error, match=rf"table {section}: round trip: key .*differs from the bundle line"):
        _build_one(section, edit)


@real_only
def test_build_check_compares_the_key_text():
    con = connect({s: list(r) for s, r in BASE.items()})
    con.execute(
        "UPDATE bundle_submitter SET key_json = replace(key_json, 'clinvar', '\\u0063linvar') WHERE ordinal = 0"
    )
    with pytest.raises(duckdb.Error, match="table submitter: round trip"):
        con.execute(f"SELECT * FROM ({render_generic(SPEC.table('submitter'))})").fetchall()


@real_only
def test_empty_list_and_control_character_round_trip():
    """Absent and empty lists stay apart at every depth (an absent nested list is NULL, an empty one []), and a
    control character is escaped as the producer escapes it (Python's lower-case \\u001f)."""

    def edit(o):
        o["primaryCoding"]["iris"] = []
        o["name"] = o["name"] + "\x1f"
        return o

    raw = BASE["gene"][0][1]
    assert "iris" in json.loads(raw)["primaryCoding"] and "mappings" in json.loads(raw)
    rows = _build_one("gene", _obj(edit))
    assert rows[0]["primaryCoding"]["iris"] == [] and rows[0]["name"].endswith("\x1f")
    assert json.dumps("\x1f") == '"\\u001f"'
    other = [
        r for r in rows[1:] if r["mappings"] is not None and not any(m["coding"]["iris"] for m in r["mappings"])
    ]
    assert all(m["coding"]["iris"] is None for r in other for m in r["mappings"])


def test_every_section_has_one_table():
    assert sorted(t.sections[0] for t in SPEC.tables) == sorted(SCHEMA_SECTIONS["1.1.0"])
