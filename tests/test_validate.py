"""`validate`: the rs engine (jsonschema-rs) gives the same verdict as the python engine (the GKM Toolkit's
reference validator) on real records and on negative cases generated from them, with the published 1.1.0 schema set
and the narrowed ClinVar-GKM set (tests/fixtures/schema-sets/, see its README). docs/validate.md has the full-size
runs.

The python engine needs the reference extra (`ga4gh.gkm`, Python 3.11+); without it, the tests that use it skip.
"""

from __future__ import annotations

import gzip
import json
import re
from functools import cache
from pathlib import Path

import jsonschema_rs
import pytest
from _fixture import COUNTS, FIXTURE, FIXTURES
from _input_helpers import bundle_bytes, fixture_sections, write_gz
from _validate_cases import UNDECLARED, ban_cases, banned_properties, list_item_cases, nodes, record_cases, records

from clinvar_gkm_parquet.cli import main
from clinvar_gkm_parquet.default_pairing import schema_source
from clinvar_gkm_parquet.errors import SchemaSetError
from clinvar_gkm_parquet.schemas import SchemaFile, SchemaSet, load_schema_set, read_set
from clinvar_gkm_parquet.validate import make_engine

NARROWED = FIXTURES / "schema-sets" / "clinvar-gkm-narrowed-a956b5a"
NARROWED_COMMIT = "a956b5a4dbba3ee9f6954101d0b726ebedae6d4b"
NARROWED_SHA256 = "98612fcac74526e4af76cd81a4cfadb09111c7df941c303ddc8cb79663618c6a"
SETS = ("narrowed", "published")

try:
    import ga4gh.gkm  # noqa: F401

    HAVE_REFERENCE = True
except ImportError:
    HAVE_REFERENCE = False
needs_reference = pytest.mark.skipif(not HAVE_REFERENCE, reason="needs the reference extra (ga4gh.gkm)")


@cache
def schema_set(name: str) -> SchemaSet:
    return read_set(NARROWED) if name == "narrowed" else load_schema_set(schema_source("1.1.0"))


@cache
def engine(set_name: str, engine_name: str):
    return make_engine(engine_name, schema_set(set_name))


def _sections(name: str) -> list[str]:
    return [s for s in schema_set(name).bundle["properties"] if s != "schema_version"]


@cache
def cases(name: str):
    rs = engine(name, "rs")
    bans, unreached = ban_cases(schema_set(name), lambda doc: rs._validator(doc).evaluate(doc).annotations())
    return bans + record_cases(_sections(name)), unreached


# --- the narrowed set fixture ------------------------------------------------------------------------------------


def test_narrowed_fixture_is_the_recorded_commit():
    s = read_set(NARROWED)
    assert s.sha256 == NARROWED_SHA256
    assert s.bundle_id == "https://w3id.org/clingen/schema/clinvar-gkm/1.1.0/clinvar-gkm-bundle.schema.json"
    for f in s.files.values():
        m = re.fullmatch(r"https://raw\.githubusercontent\.com/([^/]+/[^/]+)/([0-9a-f]{40})/.+", f.url)
        assert m, f.url
        if m.group(1) == "clingen-data-model/clinvar-gkm":
            assert m.group(2) == NARROWED_COMMIT
    # Not a shipped snapshot: shipped sets are the supported pairings, and this one would claim 1.1.0's identity.
    assert not str(NARROWED.resolve()).startswith(str((Path(__file__).parents[1] / "schemas").resolve()))


# --- whole bundles, through the CLI ------------------------------------------------------------------------------


# rs on the published set is left out: each worker would compile its sections (~1 min), and the in-process tests
# below already run that engine on every fixture record.
@pytest.mark.parametrize(
    ("set_name", "engine_name"),
    [
        ("narrowed", "rs"),
        pytest.param("narrowed", "python", marks=needs_reference),
        pytest.param("published", "python", marks=needs_reference),
    ],
)
def test_cli_passes_the_fixture(tmp_path, set_name, engine_name):
    out = tmp_path / "counts.json"
    schema = str(NARROWED) if set_name == "narrowed" else schema_source("1.1.0")
    argv = ["validate", str(FIXTURE), "--bundle-schema", schema, "--engine", engine_name, "--workers", "2"]
    assert main([*argv, "--json", str(out)]) == 0
    result = json.loads(out.read_text())
    assert result["failing"] == 0
    assert {s: v["records"] for s, v in result["sections"].items()} == COUNTS


@pytest.mark.parametrize("set_name", SETS)
def test_rs_and_python_agree_on_every_fixture_record(set_name):
    if not HAVE_REFERENCE:
        pytest.skip("needs the reference extra (ga4gh.gkm)")
    rs, py = engine(set_name, "rs"), engine(set_name, "python")
    for section, key, rec in records():
        doc = {"schema_version": "1.1.0", section: {key: rec}}
        assert rs.is_valid(doc) is py.is_valid(doc) is True, f"{section}/{key}"


def _bad_bundle(tmp_path, change) -> Path:
    sections = fixture_sections()
    change(sections)
    return write_gz(tmp_path / "b.json.gz", bundle_bytes(sections))


def test_cli_reports_a_failing_record_and_exits_1(tmp_path, capsys):
    def add_property(sections):
        key, rec = sections["allele"][0]
        sections["allele"][0] = (key, rec[:-1] + b',"' + UNDECLARED.encode() + b'":1}')

    bundle = _bad_bundle(tmp_path, add_property)
    failures = tmp_path / "failures.jsonl"
    argv = [
        "validate",
        str(bundle),
        "--bundle-schema",
        str(NARROWED),
        "--workers",
        "1",
        "--failures",
        str(failures),
    ]
    assert main(argv) == 1
    (f,) = [json.loads(line) for line in failures.read_text().splitlines()]
    key = json.loads(fixture_sections()["allele"][0][0])
    assert (f["section"], f["key"]) == ("allele", key)
    assert {e["path"] for e in f["errors"]} == {""}  # additional/unevaluated properties, reported at the record
    assert UNDECLARED in f["errors"][0]["message"]
    assert "FAIL allele/" in capsys.readouterr().err


def test_cli_stops_on_an_unknown_section_and_a_missing_version(tmp_path, capsys):
    bundle = _bad_bundle(tmp_path, lambda s: s.update(notASection=s["gene"][:1]))
    assert main(["validate", str(bundle), "--bundle-schema", str(NARROWED), "--workers", "1"]) == 2
    assert "notASection" in capsys.readouterr().err
    text = gzip.decompress(FIXTURE.read_bytes()).replace(b'  "schema_version": "1.1.0",\n', b"")
    no_version = write_gz(tmp_path / "nv.json.gz", text)
    assert main(["validate", str(no_version), "--bundle-schema", str(NARROWED), "--workers", "1"]) == 2
    assert "schema_version" in capsys.readouterr().err


def test_cli_validates_an_empty_section_against_the_schema(tmp_path):
    text = gzip.decompress(FIXTURE.read_bytes()).replace(
        b'  "schema_version": "1.1.0",\n', b'  "schema_version": "1.1.0",\n  "therapy": {},\n', 1
    )
    therapy = re.compile(rb'  "therapy": \{\n(    .*\n)*?  \},\n')
    text = therapy.sub(b"", text, count=1) if text.count(b'"therapy": {') > 1 else text
    bundle = write_gz(tmp_path / "e.json.gz", text)
    failures = tmp_path / "f.jsonl"
    argv = [
        "validate",
        str(bundle),
        "--bundle-schema",
        str(NARROWED),
        "--workers",
        "1",
        "--failures",
        str(failures),
    ]
    assert main(argv) == 1  # each section is non-empty (minProperties 1), as ClinVar-GKM's validator requires
    (f,) = [json.loads(line) for line in failures.read_text().splitlines()]
    assert (f["section"], f["key"]) == ("therapy", None)


# --- negative cases ----------------------------------------------------------------------------------------------


def _paths(problems) -> set[str]:
    return {p.path for p in problems}


def _same_place(rs_problems, py_problems) -> bool:
    """The engines report the same instance paths, but for one known difference: Python `jsonschema` reports a
    `false` subschema's error at the parent object, where jsonschema-rs reports it at the property."""
    lifted = {p.path.rsplit("/", 1)[0] if p.message.startswith("False schema") else p.path for p in rs_problems}
    return lifted == _paths(py_problems)


@needs_reference
@pytest.mark.parametrize("set_name", SETS)
def test_rs_and_python_agree_on_every_negative_case(set_name):
    rs, py = engine(set_name, "rs"), engine(set_name, "python")
    all_cases, _ = cases(set_name)
    assert len(all_cases) > 1000
    for case in all_cases:
        doc = case.doc
        a, b = rs.is_valid(doc), py.is_valid(doc)
        assert a is b, f"{set_name}: {case.name}: rs {a}, python {b}: {rs.problems(doc)} / {py.problems(doc)}"
        if case.must_reject and set_name == "narrowed":
            assert not a, case.name
        if not a:
            assert _same_place(rs.problems(doc), py.problems(doc)), case.name


def test_every_banned_property_is_rejected_where_it_applies():
    """Generated from the narrowed set: every `false` property of a ClinVar-GKM class, at every record node that
    class validates. The two excerpts reach every one of them."""
    all_cases, unreached = cases("narrowed")
    bans = [c for c in all_cases if c.kind == "banned-property"]
    banned = banned_properties(schema_set("narrowed"))
    assert len(bans) == sum(len(v) for v in banned.values()) == 204
    rs = engine("narrowed", "rs")
    assert [c.name for c in bans if rs.is_valid(c.doc)] == []
    assert unreached == set()
    assert {c.kind for c in cases("published")[0]} >= {"key-breaks-pattern", "undeclared-top-level"}


def test_named_positions_reject_an_undeclared_property():
    """The places the narrowed profiles close: a record's top level (an `allOf` profile), a `variation` record (a
    `oneOf` branch), and an item of the `clinvarHgvsList` extension's array."""
    rs = engine("narrowed", "rs")
    by_name = {c.name: c for c in cases("narrowed")[0]}
    variation = next(c for c in by_name.values() if c.kind == "undeclared-top-level" and c.section == "variation")
    assert not rs.is_valid(variation.doc)
    hgvs = list_item_cases("variation", "clinvarHgvsList")
    assert hgvs and all(not rs.is_valid(c.doc) for c in hgvs)
    kinds = {c.kind for c in by_name.values() if not rs.is_valid(c.doc)}
    assert kinds >= {
        "banned-property",
        "pointer-wrong-section",
        "pointer-inlined",
        "key-breaks-pattern",
        "extension-unknown-name",
        "extension-value-type",
        "undeclared-top-level",
        "undeclared-nested",
        "string-changed",
        "missing-property",
        "string-to-integer",
        "integer-to-fraction",
        "empty-object",
        "empty-array",
    }


# --- dialects ----------------------------------------------------------------------------------------------------


def _patterns(s: SchemaSet) -> set[str]:
    out: set[str] = set()

    def walk(n):
        if isinstance(n, dict):
            for k, v in n.items():
                if k == "pattern" and isinstance(v, str):
                    out.add(v)
                elif k == "patternProperties" and isinstance(v, dict):
                    out.update(v)
                    for x in v.values():
                        walk(x)
                elif k not in ("const", "enum", "default", "examples"):
                    walk(v)
        elif isinstance(n, list):
            for x in n:
                walk(x)

    for f in s.files.values():
        walk(f.doc)
    return out


@needs_reference
def test_regexes_match_the_same_real_strings_in_both_engines():
    from jsonschema import Draft202012Validator

    patterns = _patterns(schema_set("narrowed")) | _patterns(schema_set("published"))
    assert len(patterns) >= 50
    strings = set()
    for _, key, rec in records():
        strings.add(key)
        strings.update(v for _, v in nodes(rec) if isinstance(v, str))
    assert not [s for s in strings if s.endswith("\n")]  # where the engines' `$` differs (below)
    for p in sorted(patterns):
        rs = jsonschema_rs.validator_for({"type": "string", "pattern": p})
        py = Draft202012Validator({"type": "string", "pattern": p})
        assert [s for s in strings if rs.is_valid(s) != py.is_valid(s)] == [], p
    # The one difference found: Python's `$` also matches before a final newline; ECMA-262 and Rust's don't.
    rs = jsonschema_rs.validator_for({"pattern": "^clinvar:[0-9]+$"})
    py = Draft202012Validator({"pattern": "^clinvar:[0-9]+$"})
    assert (rs.is_valid("clinvar:1\n"), py.is_valid("clinvar:1\n")) == (False, True)
    for s in ("clinvar:١٢٣", "10.1/é", "clinvar:1 "):  # Unicode digits and word characters: the same
        assert rs.is_valid(s) == py.is_valid(s)


@needs_reference
@pytest.mark.parametrize(
    ("schema", "text"),
    [
        ({"type": "integer"}, "1.0"),
        ({"type": "integer"}, "1.5"),
        ({"type": "integer"}, "-0.0"),
        ({"type": "integer"}, "18446744073709551616"),
        ({"type": "integer"}, "123456789012345678901234567890"),
        ({"type": "number", "maximum": 9007199254740992}, "9007199254740993"),
        ({"const": 1}, "1.0"),
        ({"enum": [1]}, "1.0"),
        ({"uniqueItems": True}, "[1, 1.0]"),
        ({"uniqueItems": True}, '[{"a": 1, "b": 2}, {"b": 2, "a": 1}]'),
        ({"uniqueItems": True}, "[true, 1]"),
    ],
)
def test_numbers_and_equality_agree(schema, text):
    from jsonschema import Draft202012Validator

    value = json.loads(text)
    assert jsonschema_rs.validator_for(schema).is_valid(value) == Draft202012Validator(schema).is_valid(value)


@needs_reference
def test_multiple_of_differs_and_neither_set_uses_it():
    from jsonschema import Draft202012Validator

    assert jsonschema_rs.validator_for({"multipleOf": 0.1}).is_valid(0.3)
    assert not Draft202012Validator({"multipleOf": 0.1}).is_valid(0.3)  # binary floating point
    for name in SETS:
        assert all('"multipleOf"' not in f.raw.decode() for f in schema_set(name).files.values())


# --- no network --------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("engine_name", ["rs", pytest.param("python", marks=needs_reference)])
def test_a_ref_outside_the_set_stops_and_is_never_fetched(engine_name):
    root_id = "https://w3id.org/test/bundle.schema.json"
    root = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": root_id,
        "type": "object",
        "properties": {"x": {"$ref": "https://example.org/not/in/the/set.json"}},
    }
    s = SchemaSet(root_id, {root_id: SchemaFile(root_id, root_id, json.dumps(root).encode(), root)})
    with pytest.raises(SchemaSetError):
        e = make_engine(engine_name, s)
        e.is_valid({"x": 1})
