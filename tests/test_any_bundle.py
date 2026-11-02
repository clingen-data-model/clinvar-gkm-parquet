"""Any bundle plus its schema and column set (ADR 0006): real records from two producers through the real CLI, the
two readers, the provenance of a compiled build, and the rule that the generic path knows no producer.

CIViC's bundles and schema are copied byte for byte (tests/fixtures/civic/README.md), its column set is written
from docs/column-set-format.md, and ClinVar's pretty-printed sub-bundle is real
(tests/fixtures/clinvar-sub-bundle/). No test here patches anything but the network.
"""

from __future__ import annotations

import ast
import gzip
import json
import re
import shutil
from pathlib import Path

import duckdb
import pytest
import yaml
from _fixture import FIXTURE, FIXTURES, SECTIONS
from _input_helpers import bundle_bytes, fixture_sections, local_input, write_gz

from clinvar_gkm_parquet import build as build_mod
from clinvar_gkm_parquet import transport
from clinvar_gkm_parquet.build import KV_PREFIX
from clinvar_gkm_parquet.bundle import (
    BundleShape,
    RootRules,
    layout_of,
    peek_version,
    sniff_layout,
    stream_bundle,
)
from clinvar_gkm_parquet.cli import main
from clinvar_gkm_parquet.compiled import COMPILED_SPEC_FILE, compile_pairing, compiled_spec
from clinvar_gkm_parquet.compiler import table_spec_sha256
from clinvar_gkm_parquet.errors import BundleFormatError, InputError, SpecError, ToolError
from clinvar_gkm_parquet.model import BuildInputs, InputFile
from clinvar_gkm_parquet.spec import load_spec, resolve_spec

SRC = Path(__file__).resolve().parents[1] / "src" / "clinvar_gkm_parquet"
CIVIC = FIXTURES / "civic"
CIVIC_SCHEMA = CIVIC / "schema-set"
CIVIC_COLUMNS = CIVIC / "civic.columns.yaml"
CIVIC_BUNDLES = sorted(CIVIC.glob("civic-assertion-*-bundle.json"))
SUB_BUNDLE = FIXTURES / "clinvar-sub-bundle" / "clinvar-variation-10-bundle.json"
PAIRING = ("--bundle-schema", CIVIC_SCHEMA, "--column-set", CIVIC_COLUMNS)


def cli(capsys, *args) -> tuple[int, str, str]:
    rc = main([str(a) for a in args])
    got = capsys.readouterr()
    return rc, got.out, got.err


def kv_of(path: Path) -> dict[str, str]:
    rows = duckdb.connect().execute(f"SELECT key, value FROM parquet_kv_metadata('{path}')").fetchall()
    return {k.decode()[len(KV_PREFIX) :]: v.decode() for k, v in rows if k.decode().startswith(KV_PREFIX)}


def rows_of(path: Path, query: str = "SELECT *") -> list[tuple]:
    return duckdb.connect().execute(f"{query} FROM read_parquet('{path}')").fetchall()


# --- CIViC, end to end -------------------------------------------------------------------------------------------


@pytest.fixture(scope="module", params=CIVIC_BUNDLES, ids=lambda p: p.stem)
def civic(request, tmp_path_factory) -> tuple[Path, Path]:
    out = tmp_path_factory.mktemp("civic") / "out"
    assert main(["build", *map(str, PAIRING), "--full", str(request.param), "--out", str(out)]) == 0
    return request.param, out


def test_civic_builds_one_table_per_collection_and_keeps_its_root_values(civic):
    source, out = civic
    doc = json.loads(source.read_text())
    assert sorted(p.stem for p in out.glob("*.parquet")) == sorted(
        [k for k in doc if k not in ("metadata", "failedAssertionIds")] + ["root_values"]
    )
    for section, records in doc.items():
        if section in ("metadata", "failedAssertionIds"):
            continue
        assert sorted(r[0] for r in rows_of(out / f"{section}.parquet", "SELECT id")) == sorted(records)
    values = dict(rows_of(out / "root_values.parquet"))
    assert set(values) == {"metadata", "failedAssertionIds"}
    assert json.loads(values["metadata"]) == doc["metadata"] and values["failedAssertionIds"] == "[]"


def test_civic_verify_and_roundtrip_state_json_equal(civic, capsys):
    source, out = civic
    rc, stdout, stderr = cli(capsys, "verify", out)
    assert rc == 0, stdout + stderr
    rc, stdout, stderr = cli(capsys, "verify", out, "--roundtrip", "--full", source)
    assert rc == 0, stdout + stderr
    assert "round trip: JSON-equal" in stdout and "identical" not in stdout


def test_civic_bundle_recompute_passes_on_the_general_reader(civic, tmp_path, capsys):
    source, out = civic
    rc, stdout, stderr = cli(capsys, "verify", out, "--bundle-recompute", "--full", source, "--work-dir", tmp_path)
    assert rc == 0 and "bundle recompute: 0 group(s) differ" in stdout, stdout + stderr


def test_civic_rebuild_is_json_equal_to_the_source_and_says_so(civic, tmp_path, capsys):
    source, out = civic
    rebuilt = tmp_path / "rebuilt.json"
    rc, stdout, stderr = cli(capsys, "rebuild", out, "-o", rebuilt)
    assert rc == 0, stderr
    assert json.loads(rebuilt.read_text()) == json.loads(source.read_text())
    assert "JSON-equal to the source" in stdout and "byte for byte" not in stdout
    assert json.loads((out / "manifest.json").read_text())["round_trip"] == {
        "promise": "json-equal",
        "source_layout": "json",
    }


def test_civic_rebuilt_in_the_line_layout_rebuilds_byte_for_byte(civic, tmp_path, capsys):
    _, out = civic
    canonical = tmp_path / "canonical.json"
    assert cli(capsys, "rebuild", out, "-o", canonical)[0] == 0
    again = tmp_path / "again"
    assert cli(capsys, "build", *PAIRING, "--full", canonical, "--out", again)[0] == 0
    assert layout_of(local_input(canonical)) == "line"
    second = tmp_path / "second.json"
    rc, stdout, stderr = cli(capsys, "rebuild", again, "-o", second)
    assert rc == 0, stderr
    assert second.read_bytes() == canonical.read_bytes() and "byte for byte" in stdout
    assert json.loads((again / "manifest.json").read_text())["round_trip"]["promise"] == "byte-identical"


def test_civic_empty_collections_are_tables_with_no_rows(civic):
    source, out = civic
    doc = json.loads(source.read_text())
    for name in ("phenotype", "conditionSet", "therapy", "therapyGroup", "organization"):
        assert doc[name] == {} and rows_of(out / f"{name}.parquet") == []


def test_civic_float_values_keep_their_value(civic):
    source, out = civic
    doc = json.loads(source.read_text())
    want = [
        e["value"]
        for r in doc["molecularProfile"].values()
        for e in r.get("extensions", [])
        if "Score" in e["name"]
    ]
    query = "SELECT unnest(extensions).value_number"
    got = [r[0] for r in rows_of(out / "molecularProfile.parquet", query) if r[0] is not None]
    assert sorted(got) == sorted(want) and all(isinstance(v, float) for v in want) and want


def test_civic_keeps_a_map_and_a_pointer_or_object_position_as_json_text(civic):
    source, out = civic
    doc = json.loads(source.read_text())
    for key, protein in rows_of(out / "variant.parquet", "SELECT id, protein"):
        assert (json.loads(protein) if protein else None) == doc["variant"][key].get("protein")
    for key, by in rows_of(out / "assertion.parquet", "SELECT id, specifiedBy"):
        assert json.loads(by) == doc["assertion"][key]["specifiedBy"]


def test_civic_files_and_manifest_carry_the_three_hashes(civic):
    _, out = civic
    _, source = compile_pairing(str(CIVIC_SCHEMA), str(CIVIC_COLUMNS))
    m = json.loads((out / "manifest.json").read_text())
    written = json.loads((out / COMPILED_SPEC_FILE).read_text())
    for p in out.glob("*.parquet"):
        kv = kv_of(p)
        assert kv["schema_set_sha256"] == source.schema_set_sha256 == m["table_spec"]["schema_set_sha256"]
        assert kv["column_set_sha256"] == source.column_set_sha256 == m["table_spec"]["column_set_sha256"]
        assert kv["table_spec_sha256"] == table_spec_sha256(written) == m["table_spec"]["sha256"]
        assert kv["bundle_schema_version"] == "" and "table_spec_builtin" not in kv
    assert m["table_spec"]["file"] == COMPILED_SPEC_FILE and m["table_spec"]["builtin"] is True


def test_civic_set_is_checked_with_no_schema_on_hand(civic, tmp_path, capsys, monkeypatch):
    """A generic set can't assume its pairing is around later: verify and rebuild read the spec the build wrote."""
    source, out = civic
    moved = tmp_path / "moved"
    shutil.copytree(out, moved)

    def no_load(*a, **k):
        raise AssertionError("verify and rebuild need no schema or column set")

    monkeypatch.setattr("clinvar_gkm_parquet.compiled.load_schema_set", no_load)
    monkeypatch.setattr("clinvar_gkm_parquet.compiled.load_column_set", no_load)
    assert cli(capsys, "verify", moved)[0] == 0
    assert cli(capsys, "rebuild", moved, "-o", tmp_path / "r.json")[0] == 0
    assert json.loads((tmp_path / "r.json").read_text()) == json.loads(source.read_text())


def test_compile_spec_takes_the_pairing(capsys):
    rc, stdout, stderr = cli(capsys, "compile-spec", *PAIRING)
    assert rc == 0, stderr
    raw = yaml.safe_load(stdout)
    _, source = compile_pairing(str(CIVIC_SCHEMA), str(CIVIC_COLUMNS))
    assert table_spec_sha256(raw) == source.sha256 and raw["root"]["other_values"] is True
    assert f"# column_set_sha256: {source.column_set_sha256}" in stdout


def test_a_pairing_flag_alone_is_refused(tmp_path, capsys):
    rc, _, stderr = cli(
        capsys, "build", "--bundle-schema", CIVIC_SCHEMA, "--full", CIVIC_BUNDLES[0], "--out", tmp_path / "o"
    )
    assert rc == 2 and "--bundle-schema and --column-set go together" in stderr
    rc, _, stderr = cli(capsys, "compile-spec", "--column-set", CIVIC_COLUMNS)
    assert rc == 2 and "--bundle-schema and --column-set together" in stderr
    rc, _, stderr = cli(capsys, "compile-spec", "1.1.0", *PAIRING)
    assert rc == 2 and "not both" in stderr


def test_a_pairing_isnt_mixed_with_another_spec_a_delta_or_a_selection(tmp_path, capsys):
    full = CIVIC_BUNDLES[0]
    rc, _, stderr = cli(capsys, "build", *PAIRING, "--spec", "baseline", "--full", full, "--out", tmp_path / "o")
    assert rc == 2 and "not with --spec baseline" in stderr
    rc, _, stderr = cli(capsys, "build", *PAIRING, "--driver", "gene", "--full", full, "--out", tmp_path / "o")
    assert rc == 2 and "not with --driver or --delta" in stderr


def test_a_column_set_for_another_schema_is_refused(tmp_path, capsys):
    other = tmp_path / "other.yaml"
    other.write_text(CIVIC_COLUMNS.read_text().replace("schema: urn:civic", "schema: urn:other"))
    rc, _, stderr = cli(
        capsys,
        "build",
        "--bundle-schema",
        CIVIC_SCHEMA,
        "--column-set",
        other,
        "--full",
        CIVIC_BUNDLES[0],
        "--out",
        tmp_path / "o",
    )
    assert rc == 2 and "it says it pairs with schema 'urn:other:gkm-bundle" in stderr


def test_a_property_the_column_set_doesnt_list_stops_the_build(tmp_path, capsys):
    doc = json.loads(CIVIC_BUNDLES[0].read_text())
    next(iter(doc["feature"].values()))["description"] = "a property this column set doesn't list"
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(doc))
    rc, _, stderr = cli(capsys, "build", *PAIRING, "--full", bad, "--out", tmp_path / "o")
    assert rc == 2 and "description" in stderr and "feature" in stderr


def test_an_unknown_root_member_is_kept_under_an_open_root(tmp_path, capsys):
    doc = json.loads(CIVIC_BUNDLES[0].read_text())
    doc["extraRootMember"] = {"a": [1, 2.5, None]}
    path = tmp_path / "b.json"
    path.write_text(json.dumps(doc))
    out = tmp_path / "out"
    assert cli(capsys, "build", *PAIRING, "--full", path, "--out", out)[0] == 0
    assert cli(capsys, "rebuild", out, "-o", tmp_path / "r.json")[0] == 0
    assert json.loads((tmp_path / "r.json").read_text()) == doc


# --- ClinVar's pretty-printed sub-bundle, by the default flags ---------------------------------------------------


@pytest.fixture(scope="module")
def sub_bundle(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("sub") / "out"
    assert main(["build", "--full", str(SUB_BUNDLE), "--out", str(out)]) == 0
    return out


def test_the_sub_bundle_takes_the_general_reader_and_the_default_pairing(sub_bundle):
    assert layout_of(local_input(SUB_BUNDLE)) == "json"
    assert sorted(p.stem for p in sub_bundle.glob("*.parquet")) == sorted(
        SECTIONS
    )  # no root_values table: still 20
    kv = kv_of(sub_bundle / "scv.parquet")
    _, source = compiled_spec("1.1.0")
    assert kv["bundle_schema_version"] == "1.1.0" and kv["schema_set_sha256"] == source.schema_set_sha256
    # Its keys came sorted, so the spec the set carries is the key-sorted one, with the same columns.
    written = json.loads((sub_bundle / COMPILED_SPEC_FILE).read_text())
    assert all(t["json"]["key_order"] == "sorted" for t in written["tables"])
    assert kv["table_spec_sha256"] == table_spec_sha256(written) != source.sha256
    assert [t["columns"] for t in written["tables"]] == [t["columns"] for t in source.raw["tables"]]


def test_the_sub_bundle_rebuilds_json_equal(sub_bundle, tmp_path, capsys):
    rc, stdout, stderr = cli(capsys, "rebuild", sub_bundle, "-o", tmp_path / "r.json")
    assert rc == 0, stderr
    assert json.loads((tmp_path / "r.json").read_text()) == json.loads(SUB_BUNDLE.read_text())
    assert "JSON-equal" in stdout
    rc, stdout, stderr = cli(capsys, "verify", sub_bundle, "--roundtrip", "--full", SUB_BUNDLE)
    assert rc == 0 and "round trip: JSON-equal" in stdout, stdout + stderr


def test_a_fixed_spec_refuses_a_bundle_that_isnt_in_the_line_layout(tmp_path, capsys):
    for name in ("clinvar-gkm", "baseline"):
        rc, _, stderr = cli(capsys, "build", "--full", SUB_BUNDLE, "--spec", name, "--out", tmp_path / name)
        assert rc == 2 and "the record is not a JSON object on one line" in stderr


def test_a_selection_refuses_a_bundle_that_isnt_in_the_line_layout(tmp_path, capsys):
    rc, _, stderr = cli(
        capsys, "select", "--full", SUB_BUNDLE, "--driver", "gene", "--ids", "x", "--out", tmp_path / "s.json.gz"
    )
    assert rc == 2 and "a selection reads the line layout" in stderr


# --- the default path: versions ----------------------------------------------------------------------------------


def test_a_bundle_with_no_version_needs_its_pairing(tmp_path, capsys):
    rc, _, stderr = cli(capsys, "build", "--full", CIVIC_BUNDLES[0], "--out", tmp_path / "o")
    assert rc == 2 and "no 'schema_version' at the root" in stderr and "--bundle-schema and --column-set" in stderr


def test_an_unknown_version_whose_schemas_cant_be_fetched_is_refused(tmp_path, capsys):
    bad = write_gz(tmp_path / "in" / "b.json.gz", bundle_bytes({"gene": fixture_sections()["gene"]}, "8.8.8"))
    rc, _, stderr = cli(capsys, "build", "--full", bad, "--out", tmp_path / "out")
    assert rc == 2
    assert "bundle schema_version '8.8.8': table spec sections can't read it" in stderr
    assert "clinvar-gkm/8.8.8/clinvar-gkm-bundle.schema.json" in stderr and "HTTP 404" in stderr
    assert not (tmp_path / "out").exists()


def test_a_published_version_is_fetched_once_through_the_data_file(tmp_path, capsys, monkeypatch):
    """A version with no snapshot: the schema and the column set come from default-bundle.yaml's URLs, then the
    cache. Here the "network" serves the CIViC pairing, renamed to the version, from the fixture's files."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    version = "8.8.1"
    base = f"https://w3id.org/clingen/schema/clinvar-gkm/{version}/clinvar-gkm-bundle"
    files = json.loads((CIVIC_SCHEMA / "MANIFEST.json").read_text())["files"]
    root_doc = json.loads((CIVIC_SCHEMA / "civic-gkm-bundle-v0.1.0-a0.schema.json").read_text())
    root_doc["properties"]["schema_version"] = {"type": "string", "const": version}
    site = {f"{base}.schema.json": json.dumps(root_doc).encode()}
    for e in files:
        site[e["id"]] = (CIVIC_SCHEMA / e["file"]).read_bytes()
    column_doc = yaml.safe_load(CIVIC_COLUMNS.read_text())
    column_doc["version_property"] = "schema_version"
    site[f"{base}.columns.yaml"] = yaml.safe_dump(column_doc).encode()
    got: list[str] = []

    class Response:
        status = 200
        headers = {}

        def __init__(self, data):
            self.data = data

        def read(self):
            return self.data

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def urlopen(req, **kwargs):
        got.append(req.full_url)
        if req.full_url not in site:
            raise AssertionError(req.full_url)
        return Response(site[req.full_url])

    monkeypatch.setattr(transport, "_urlopen", urlopen)
    doc = json.loads(CIVIC_BUNDLES[1].read_text())
    doc = {"schema_version": version, **doc}
    path = tmp_path / "b.json"
    path.write_text(json.dumps(doc))
    assert cli(capsys, "build", "--full", path, "--out", tmp_path / "out")[0] == 0
    n = len(got)
    assert f"{base}.schema.json" in got and f"{base}.columns.yaml" in got
    compile_pairing.cache_clear()
    assert cli(capsys, "build", "--full", path, "--out", tmp_path / "out2")[0] == 0
    assert len(got) == n  # from the cache


def test_a_version_whose_schemas_load_but_whose_column_set_doesnt_fit_stops_at_compile(tmp_path, capsys):
    bad_columns = tmp_path / "c.yaml"
    doc = yaml.safe_load(CIVIC_COLUMNS.read_text())
    doc["sections"]["feature"]["name"] = "integer"
    bad_columns.write_text(yaml.safe_dump(doc))
    rc, _, stderr = cli(
        capsys,
        "build",
        "--bundle-schema",
        CIVIC_SCHEMA,
        "--column-set",
        bad_columns,
        "--full",
        CIVIC_BUNDLES[0],
        "--out",
        tmp_path / "o",
    )
    assert rc == 2 and "section feature: position name: contradicts the schema" in stderr


@pytest.mark.parametrize("name", ["clinvar-gkm", "baseline"])
def test_fixed_specs_keep_their_list_and_look_at_no_schema_set(tmp_path, capsys, monkeypatch, name):
    def no_load(*a, **k):
        raise AssertionError("a fixed spec must refuse from its own list")

    monkeypatch.setattr("clinvar_gkm_parquet.schemas.fetch_set", no_load)
    bad = write_gz(tmp_path / "in" / "b.json.gz", bundle_bytes({"gene": fixture_sections()["gene"]}, "8.8.8"))
    rc, _, stderr = cli(capsys, "build", "--full", bad, "--spec", name, "--out", tmp_path / name)
    assert rc == 2 and "bundle schema_version 8.8.8 is not one table spec 1.0 reads (1.1.0)" in stderr


def test_a_custom_spec_is_checked_against_the_schema_sets_it_lists(tmp_path, capsys):
    raw = yaml.safe_load((FIXTURES / "custom-spec" / "tables.yaml").read_text())
    raw["bundle_schema_versions"] = ["8.8.8"]
    path = tmp_path / "custom.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    rc, _, stderr = cli(capsys, "build", "--full", FIXTURE, "--spec", path, "--out", tmp_path / "out")
    assert rc == 2 and "bundle_schema_versions 8.8.8: can't read its sections" in stderr and "HTTP 404" in stderr


def test_compile_spec_refuses_what_a_build_refuses(capsys):
    rc, stdout, stderr = cli(capsys, "compile-spec", "8.8.8")
    assert rc == 2 and not stdout and "table spec sections can't read it" in stderr
    rc, _, stderr = cli(capsys, "compile-spec", "../1.1.0")
    assert rc == 2 and "is not a version" in stderr


def test_compile_spec_writes_what_a_build_uses(capsys):
    rc, stdout, _ = cli(capsys, "compile-spec", "1.1.0")
    _, source = compiled_spec("1.1.0")
    assert rc == 0 and table_spec_sha256(yaml.safe_load(stdout)) == source.sha256
    assert f"# schema_set_sha256: {source.schema_set_sha256}" in stdout


def test_build_api_with_no_spec_compiles_for_the_bundle(tmp_path):
    result = build_mod.build(BuildInputs(full=local_input(FIXTURE), full_source_release=None), tmp_path / "out")
    assert len(result.tables) == len(SECTIONS)
    kv = kv_of(tmp_path / "out" / "gene.parquet")
    assert (kv["table_spec"], kv["table_spec_version"]) == ("sections", "1.0") and "table_spec_builtin" not in kv


def test_resolving_the_compiled_spec_needs_a_version():
    with pytest.raises(SpecError, match="compiled for a bundle's schema_version"):
        resolve_spec("sections")
    with pytest.raises(SpecError, match="no built-in table spec sections 9.9"):
        resolve_spec("sections@9.9", "1.1.0")
    with pytest.raises(SpecError, match="has no file"):
        load_spec("1.0", "sections")


# --- the readers -------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "head, layout",
    [
        (b'{\n  "v": "1",\n  "a": {\n    "k": {"x":1},\n    "l": {}\n  }\n}\n', "line"),
        (b'{\n  "a": {\n    "k": {"x":1}\n  }\n}\n', "line"),
        (b'{\n  "a": {},\n  "m": {"x":1}\n}\n', "line"),
        (b'{\n  "v": "1"\n}\n', "line"),
        (b'{\n  "a": {\n    "k": {\n      "x": 1\n    }\n  }\n}\n', "json"),
        (b'{\n  "m": {\n    "x": 1,\n    "y": 2\n  }\n}\n', "json"),
        (b'{"a":{"k":{"x":1}}}\n', "json"),
        (b"[1]\n", "json"),
    ],
)
def test_the_layout_is_a_property_of_the_file(head, layout):
    assert sniff_layout(head) == layout


class Sink:
    def __init__(self):
        self.events = []

    def begin(self, version):
        self.events.append(("begin", version))

    def open_section(self, name):
        self.events.append(("open", name))
        return lambda k, r: self.events.append(("record", k, r))

    def close_section(self, name):
        self.events.append(("close", name))

    def root_value(self, name, value):
        self.events.append(("value", name, value))

    def section_text(self, name, sha):
        self.events.append(("text", name))


RULES = RootRules("v", lambda version: BundleShape(("a", "b", "c"), True))


def _stream(tmp_path, text: bytes, rules=RULES, name="b.json", **kw):
    path = tmp_path / name
    path.write_bytes(text)
    sink = Sink()
    return stream_bundle(local_input(path), sink, rules, **kw), sink.events


def test_the_general_reader_sorts_keys_and_orders_by_schema(tmp_path):
    text = json.dumps(
        {"m": {"z": 1, "a": [2.5]}, "b": {"k": {"y": 1, "x": {"d": 1, "c": "é"}}}, "v": "1", "a": {}}
    )
    streamed, events = _stream(tmp_path, text.encode())
    assert events == [
        ("begin", "1"),
        ("open", "a"),
        ("close", "a"),
        ("open", "b"),
        ("record", b'"k"', '{"x":{"c":"é","d":1},"y":1}'.encode()),
        ("close", "b"),
        ("value", "m", b'{"a":[2.5],"z":1}'),
    ]
    assert (
        streamed.layout == "json" and streamed.root == ("v", "a", "b", "m") and streamed.counts == {"a": 0, "b": 1}
    )


def test_the_line_grammar_reads_empty_collections_and_root_values_byte_for_byte(tmp_path):
    text = (
        b'{\n  "v": "1",\n  "a": {},\n  "m": {"z":1,"a":2},\n'
        b'  "b": {\n    "k": {"y":1,"x":2}\n  },\n  "n": []\n}\n'
    )
    streamed, events = _stream(tmp_path, text, text_digests=True)
    assert streamed.layout == "line" and streamed.root == ("v", "a", "m", "b", "n")
    assert ("value", "m", b'{"z":1,"a":2}') in events and ("value", "n", b"[]") in events
    assert ("record", b'"k"', b'{"y":1,"x":2}') in events


@pytest.mark.parametrize(
    "text, message",
    [
        (b'{"a": {}, "a": {}}', "duplicate key"),
        (b'{"a": {"k": {"x": NaN}}}', "NaN is not JSON"),
        (b"{not json", "not a JSON bundle"),
        (b"[1]", "the root of a bundle is a JSON object"),
        (b'{"a": [1]}', "collection 'a' is not an object"),
        (b'{"a": {"k": 1}}', "the record is not a JSON object"),
        (b'{"v": 1}', "v is not a string"),
    ],
)
def test_the_general_reader_refuses_loudly(tmp_path, text, message):
    with pytest.raises(ToolError, match=message):
        _stream(tmp_path, text)


@pytest.mark.parametrize(
    "line, message",
    [
        (b'  "a": {"x":1},\n', "collection 'a' must open on its own line, or be {}"),
        (b'  "m": {not json},\n', "a root member is not JSON on one line"),
        (b'  "m": 1,\n  "m": 2,\n', "root member 'm' appears twice"),
    ],
)
def test_the_line_grammar_refuses_a_bad_root_member(tmp_path, line, message):
    with pytest.raises(BundleFormatError, match=re.escape(message)):
        _stream(tmp_path, b'{\n  "v": "1",\n' + line + b'  "b": {\n    "k": {"x":1}\n  }\n}\n')


def test_a_root_closed_to_other_values_refuses_one(tmp_path):
    rules = RootRules("v", lambda version: BundleShape(("a",), False))
    with pytest.raises(BundleFormatError, match="unknown section 'm'"):
        _stream(tmp_path, b'{\n  "v": "1",\n  "m": 1\n}\n', rules)


def test_a_plain_file_and_a_gzip_file_read_the_same(tmp_path):
    text = b'{\n  "v": "1",\n  "b": {\n    "k": {"x":1}\n  }\n}\n'
    plain, events_plain = _stream(tmp_path, text, name="p.json")
    packed, events_packed = _stream(tmp_path, gzip.compress(text), name="z.json")
    assert events_plain == events_packed and plain.counts == packed.counts


def test_the_general_reader_checks_the_sha256_of_the_file(tmp_path):
    path = tmp_path / "b.json"
    path.write_text('{"v": "1"}')
    with pytest.raises(InputError, match="sha256 mismatch"):
        stream_bundle(local_input(path, sha="0" * 64), Sink(), RULES)


def test_the_version_comes_from_the_first_lines_of_a_url_with_one_get_and_no_range(monkeypatch):
    import io

    raw = FIXTURE.read_bytes()
    requests = []

    class Response(io.BytesIO):
        status = 200
        headers = {"Content-Length": str(len(raw))}

    def urlopen(req, **kwargs):
        requests.append(dict(req.headers))
        return Response(raw)

    monkeypatch.setattr(transport, "_urlopen", urlopen)
    inp = InputFile("https://example.org/x.json.gz", "x.json.gz", None, None, None, None)
    assert peek_version(inp, "schema_version") == "1.1.0"
    assert len(requests) == 1 and "Range" not in requests[0]


@pytest.mark.parametrize(
    "data, version",
    [
        (b'{"a": {}, "v": "2"}', "2"),
        (b'{\n  "v": "3",\n  "a": {\n    "k": {\n      "x": 1\n    }\n  }\n}\n', "3"),
        (b'{"a": {}}', None),
        (b'{\n  "a": {\n    "k": {"x":1}\n  }\n}\n', None),
    ],
)
def test_the_version_is_found_in_any_layout(tmp_path, data, version):
    path = tmp_path / "b.json"
    path.write_bytes(data)
    assert peek_version(local_input(path), "v") == version


@pytest.mark.parametrize(
    "data, message",
    [
        (gzip.compress(b""), "empty file"),
        (gzip.compress(b"[\n"), "not a JSON bundle"),
        (b"not gzip", "not a complete gzip stream"),
    ],
)
def test_a_file_that_doesnt_start_as_a_bundle_stops_the_peek(tmp_path, data, message):
    path = tmp_path / "b.json.gz"
    path.write_bytes(data)
    with pytest.raises(ToolError, match=message):
        peek_version(local_input(path), "v")


def test_a_wrong_checksum_is_the_error_a_peek_reports_first(tmp_path):
    path = tmp_path / "b.json.gz"
    path.write_bytes(gzip.compress(b"[\n"))
    with pytest.raises(InputError, match="sha256 mismatch"):
        peek_version(local_input(path, sha="0" * 64), "v")


# --- provenance (ADR 0006 §7), on the real excerpt ---------------------------------------------------------------


def _tamper_kv(directory: Path, key: str, value: str | None) -> None:
    for path in directory.glob("*.parquet"):
        con = duckdb.connect()
        rows = con.execute(f"SELECT key, value FROM parquet_kv_metadata('{path}')").fetchall()
        pairs = {k.decode(): v.decode() for k, v in rows if k.decode().startswith(KV_PREFIX)}
        if value is None:
            del pairs[KV_PREFIX + key]
        else:
            pairs[KV_PREFIX + key] = value
        meta = ", ".join(f"'{k}': '{v.replace(chr(39), chr(39) * 2)}'" for k, v in pairs.items())
        tmp = path.with_name(path.name + ".new")
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{path}')) TO '{tmp}' (FORMAT parquet, KV_METADATA {{{meta}}})"
        )
        con.close()
        tmp.replace(path)


@pytest.fixture(scope="module")
def excerpt_set(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("excerpt") / "out"
    assert main(["build", "--full", str(FIXTURE), "--out", str(out)]) == 0
    return out


@pytest.fixture
def excerpt_copy(excerpt_set, tmp_path) -> Path:
    d = tmp_path / "copy"
    shutil.copytree(excerpt_set, d)
    return d


def test_the_excerpt_set_holds_its_spec_and_rebuilds_byte_for_byte(excerpt_set, tmp_path, capsys):
    _, source = compiled_spec("1.1.0")
    written = json.loads((excerpt_set / COMPILED_SPEC_FILE).read_text())
    assert table_spec_sha256(written) == source.sha256
    m = json.loads((excerpt_set / "manifest.json").read_text())
    assert m["round_trip"] == {"promise": "byte-identical", "source_layout": "line"}
    rc, stdout, _ = cli(capsys, "rebuild", excerpt_set, "-o", tmp_path / "r.json")
    assert rc == 0 and "byte for byte" in stdout
    assert (tmp_path / "r.json").read_bytes() == gzip.decompress(FIXTURE.read_bytes())


@pytest.mark.parametrize("command", ["verify", "verify --roundtrip", "rebuild"])
def test_a_tampered_hash_in_the_files_is_refused(excerpt_copy, tmp_path, capsys, command):
    _tamper_kv(excerpt_copy, "table_spec_sha256", "0" * 64)
    args = {
        "verify": ["verify", excerpt_copy],
        "verify --roundtrip": ["verify", excerpt_copy, "--roundtrip"],
        "rebuild": ["rebuild", excerpt_copy, "-o", tmp_path / "r.json"],
    }[command]
    rc, _, stderr = cli(capsys, *args)
    assert rc == 2 and "the compiled spec differs from what the files record" in stderr, stderr
    assert "0" * 64 in stderr and "written by tool" in stderr and not (tmp_path / "r.json").exists()


def test_an_edited_compiled_spec_is_refused_and_rewording_it_is_not(excerpt_copy, tmp_path, capsys):
    path = excerpt_copy / COMPILED_SPEC_FILE
    raw = json.loads(path.read_text())
    raw["tables"][0]["description"] = "reworded"
    path.write_text(json.dumps(raw))
    assert cli(capsys, "verify", excerpt_copy)[0] == 0
    raw["tables"][0]["columns"][1]["type"] = "BIGINT"
    path.write_text(json.dumps(raw))
    rc, _, stderr = cli(capsys, "rebuild", excerpt_copy, "-o", tmp_path / "r.json")
    assert rc == 2 and "the compiled spec differs" in stderr


def test_a_set_missing_its_compiled_spec_is_refused(excerpt_copy, capsys):
    (excerpt_copy / COMPILED_SPEC_FILE).unlink()
    rc, _, stderr = cli(capsys, "verify", excerpt_copy)
    assert rc == 2 and "it is missing" in stderr


def test_a_manifest_that_disagrees_with_the_files_is_found(excerpt_copy, tmp_path, capsys):
    m = json.loads((excerpt_copy / "manifest.json").read_text())
    m["table_spec"]["column_set_sha256"] = "0" * 64
    (excerpt_copy / "manifest.json").write_text(json.dumps(m))
    rc, stdout, _ = cli(capsys, "verify", excerpt_copy)
    assert rc == 1 and "table_spec.column_set_sha256" in stdout
    rc, _, stderr = cli(capsys, "rebuild", excerpt_copy, "-o", tmp_path / "r.json")
    assert rc == 2 and "table_spec.column_set_sha256" in stderr


@pytest.mark.parametrize("name", ["clinvar-gkm", "baseline"])
def test_a_fixed_spec_records_no_compiled_hash_and_writes_no_spec_file(tmp_path, capsys, name):
    out = tmp_path / "out"
    assert cli(capsys, "build", "--full", FIXTURE, "--spec", name, "--tables", "gene", "--out", out)[0] == 0
    kv = kv_of(out / "gene.parquet")
    assert not {"table_spec_sha256", "schema_set_sha256", "column_set_sha256", "table_spec_builtin"} & set(kv)
    assert "schema_set_sha256" not in json.loads((out / "manifest.json").read_text())["table_spec"]
    assert not (out / COMPILED_SPEC_FILE).exists()


def test_verify_accepts_the_built_in_name_for_a_compiled_set(excerpt_set, capsys):
    assert cli(capsys, "verify", excerpt_set, "--spec", "sections")[0] == 0
    rc, _, stderr = cli(capsys, "verify", excerpt_set, "--spec", "baseline")
    assert rc == 2 and "--spec baseline is baseline 1.0, but the files were written by sections 1.0" in stderr


# --- the generic path knows no producer --------------------------------------------------------------------------

# What a move to another package takes: this path holds no producer's name or URL, and imports nothing that does.
CORE = ("compiler", "schemas", "columnset", "bundle", "transport", "errors", "model")
# The one neutral thing the scan allows: the tool's own distribution name, in one constant.
ALLOWED_LINES = {("transport", 'TOOL_NAME = "clinvar-gkm-parquet"')}
PRODUCER_WORDS = re.compile(r"clinvar|scv|vcv|\brcv\b|loc_start|civic|ncbi|clingen|bigquery", re.IGNORECASE)


def _package_imports(module: str) -> set[str]:
    """The package's modules `module` imports, at any depth of its code, lazy imports included."""
    tree = ast.parse((SRC / f"{module}.py").read_text(encoding="utf-8"))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level == 1:
            if node.module:
                found.add(node.module.split(".")[0])
            else:
                found.update(a.name for a in node.names)
    return found - {"__version__"}


def test_the_core_imports_only_the_core():
    reached, todo = set(), list(CORE)
    while todo:
        module = todo.pop()
        if module not in reached:
            reached.add(module)
            todo.extend(_package_imports(module))
    assert reached == set(CORE), sorted(reached - set(CORE))


@pytest.mark.parametrize("module", [*CORE, "default_pairing", "compiled"])
def test_the_core_names_no_producer(module):
    hits = []
    for line in (SRC / f"{module}.py").read_text(encoding="utf-8").splitlines():
        if PRODUCER_WORDS.search(line) and (module, line.strip().split("  #")[0].strip()) not in ALLOWED_LINES:
            hits.append(line.strip())
    assert not hits, hits


def test_the_default_bundle_is_data():
    path = SRC.parents[1] / "spec" / "sections" / "default-bundle.yaml"
    doc = yaml.safe_load(path.read_text())
    assert set(doc) == {"version_property", "schema_url", "column_set_url"}
    assert "{version}" in doc["schema_url"] and "{version}" in doc["column_set_url"]
