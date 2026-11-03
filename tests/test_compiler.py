"""The compiler (ADR 0006): on the real 1.1.0 schemas and its column set it reproduces the golden spec and the
build; it refuses a bad column-set entry and any pattern its rules don't cover.

Offline: the schemas come from the shipped snapshot. Bundle records are real (the excerpt, tests/_fixture.py). The
small schema sets of the fail-loud tests are hand-written schemas, not records.
"""

from __future__ import annotations

import copy
import gzip
import json
import re
from pathlib import Path

import duckdb
import pytest
import yaml
from _fixture import FIXTURE, GOLDEN_SECTIONS_SPEC, SECTIONS, SOURCE_RELEASE
from _input_helpers import local_input

from clinvar_gkm_parquet import build as build_mod
from clinvar_gkm_parquet.columnset import load_column_set, parse_column_set
from clinvar_gkm_parquet.compiled import compiled_spec
from clinvar_gkm_parquet.compiler import compile_sections, root_parts, spec_for_sorted_input, table_spec_sha256
from clinvar_gkm_parquet.default_pairing import column_set_source, schema_source
from clinvar_gkm_parquet.errors import CompileError
from clinvar_gkm_parquet.model import BuildInputs
from clinvar_gkm_parquet.roundtrip import rebuild
from clinvar_gkm_parquet.schemas import SchemaFile, SchemaSet, load_schema_set
from clinvar_gkm_parquet.spec import Spec, Table, check_compiled, parse_spec

SCHEMAS = load_schema_set(schema_source("1.1.0"))
COLUMN_SET = load_column_set(column_set_source("1.1.0"))
GOLDEN = parse_spec(yaml.safe_load(GOLDEN_SECTIONS_SPEC.read_text(encoding="utf-8")), require_name=True)
COMPILED, SOURCE = compiled_spec("1.1.0")


def columns() -> dict:
    return copy.deepcopy(COLUMN_SET.doc)


def checked(raw: dict) -> Spec:
    spec = parse_spec(raw, require_name=True)
    check_compiled(spec, "the test's spec")
    return spec


# --- golden: the 1.1.0 spec the research produced ----------------------------------------------------------------


def _shape(type_str: str) -> object:
    """A DuckDB type with STRUCT field order removed."""

    def norm(t):
        if t.id == "struct":
            return ("struct", tuple(sorted((n, norm(c)) for n, c in t.children)))
        if t.id == "list":
            return ("list", norm(t.children[0][1]))
        return str(t)

    return norm(duckdb.sqltype(type_str))


def _column_facts(t: Table, *, ordered: bool) -> dict:
    facts = {}
    for c in t.columns:
        d = c.derivation
        pointers = dict(zip(d.pointers, d.pointer_targets, strict=True))
        facts[c.name] = (c.type if ordered else _shape(c.type), c.nulls, d.kind, d.path, d.targets, pointers)
    return facts


# Accepted differences between the compiled spec and tests/fixtures/sections-golden/tables.yaml, beyond the column
# and field order of the key-sorted tables (the bundle text doesn't depend on it). Empty: the compiler reproduces
# every table.
ACCEPTED_DIFFERENCES: dict[str, list[str]] = {}


@pytest.mark.parametrize("golden", GOLDEN.tables, ids=lambda t: t.name)
def test_compiled_table_equals_the_golden_table(golden):
    t = COMPILED.table(golden.name)
    ordered = golden.json.key_order == "columns"
    if ordered:
        assert [c.name for c in t.columns] == [c.name for c in golden.columns]
    got, want = _column_facts(t, ordered=ordered), _column_facts(golden, ordered=ordered)
    differ = sorted(n for n in set(got) | set(want) if got.get(n) != want.get(n))
    assert differ == ACCEPTED_DIFFERENCES.get(golden.name, []), [(n, got.get(n), want.get(n)) for n in differ]
    assert t.columns[0].name == "id" and t.columns[-1].name == "ordinal"
    assert t.sections == golden.sections and t.row_filter is None
    assert t.extension_slots == golden.extension_slots
    assert t.json == golden.json
    assert t.known_fields is golden.known_fields is True
    assert t.sort_key == golden.sort_key == "id"


def test_tables_are_the_bundle_schema_collections_in_order():
    assert tuple(t.name for t in COMPILED.tables) == SECTIONS == tuple(t.name for t in GOLDEN.tables)


def test_compiled_spec_keeps_the_top_level_facts():
    assert COMPILED.lossless and COMPILED.writer == GOLDEN.writer
    assert (COMPILED.name, COMPILED.spec_version) == (GOLDEN.name, GOLDEN.spec_version)
    assert COMPILED.bundle_schema_versions == ("1.1.0",)
    assert SCHEMAS.sha256 in COMPILED.sources[0]
    assert (COMPILED.root.version_property, COMPILED.root.other_values) == ("schema_version", False)


def test_the_source_records_the_pairing():
    assert (SOURCE.name, SOURCE.version, SOURCE.builtin, SOURCE.path) == ("sections", "1.0", True, None)
    assert SOURCE.schema_set_sha256 == SCHEMAS.sha256 and SOURCE.column_set_sha256 == COLUMN_SET.sha256
    assert SOURCE.sha256 == table_spec_sha256(compile_sections(SCHEMAS, COLUMN_SET.doc))


def test_sorted_tables_take_schema_order_and_key_order_tables_the_column_sets():
    assert [c.name for c in COMPILED.table("location").columns] == [
        "id",
        "type",
        "digest",
        "sequenceReference",
        "loc_start",
        "loc_start_min",
        "loc_start_max",
        "loc_end",
        "loc_end_min",
        "loc_end_max",
        "ordinal",
    ]
    doc = columns()
    doc["sections"]["submitter"] = {"id": None, "name": None, "type": None}
    assert [c.name for c in checked(compile_sections(SCHEMAS, doc)).table("submitter").columns] == [
        "id",
        "type",
        "name",
        "ordinal",
    ]
    doc["key_order"]["submitter"] = None
    assert [c.name for c in parse_spec(compile_sections(SCHEMAS, doc)).table("submitter").columns] == [
        "id",
        "name",
        "type",
        "ordinal",
    ]


def test_output_is_deterministic_and_ignores_entry_order_in_a_sorted_table():
    assert compile_sections(SCHEMAS, COLUMN_SET.doc) == compile_sections(SCHEMAS, COLUMN_SET.doc)
    doc = columns()
    doc["sections"]["location"] = dict(reversed(list(doc["sections"]["location"].items())))
    assert compile_sections(SCHEMAS, doc) == compile_sections(SCHEMAS, COLUMN_SET.doc)


def test_descriptions_come_from_the_schema_and_transforms_from_the_rules():
    t = COMPILED.table("location")
    assert t.description == "VRS SequenceLocation records."
    assert t.column("sequenceReference").description.startswith("A reference to a SequenceReference")
    assert t.column("sequenceReference").derivation.transform == "strip ^#/[^/]+/"
    assert COMPILED.table("variation").column("members_section").derivation.transform.startswith("the <section>")


# --- build equivalence: the real excerpt -------------------------------------------------------------------------


def _full(path: Path) -> BuildInputs:
    return BuildInputs(full=local_input(path, release=SOURCE_RELEASE), full_source_release=SOURCE_RELEASE)


def _rows(directory: Path, table: Table, names: list[str]) -> list[tuple]:
    selected = ", ".join(f'"{n}"' for n in names)
    path = directory / table.file_name
    return duckdb.connect().execute(f"SELECT {selected} FROM read_parquet('{path}') ORDER BY id").fetchall()


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> tuple[Path, Path]:
    root = tmp_path_factory.mktemp("compiled")
    build_mod.build(_full(FIXTURE), root / "compiled", spec=COMPILED, spec_source=SOURCE)
    build_mod.build(_full(FIXTURE), root / "golden", spec=GOLDEN)
    return root / "compiled", root / "golden"


def test_build_with_the_compiled_spec_rebuilds_the_bundle_text(built, tmp_path):
    out = tmp_path / "rebuilt.json"
    result = rebuild(built[0], out)
    assert out.read_bytes() == gzip.decompress(FIXTURE.read_bytes())
    assert result.text_sha256 == result.expected_text_sha256 and result.promise == "byte-identical"


@pytest.mark.parametrize("golden", GOLDEN.tables, ids=lambda t: t.name)
def test_rows_equal_those_of_the_golden_spec_by_column_name(built, golden):
    names = [c.name for c in golden.columns]
    compiled_rows = _rows(built[0], COMPILED.table(golden.name), names)
    assert compiled_rows == _rows(
        built[1], golden, names
    )  # DuckDB returns a STRUCT as a dict: field order is free


def test_a_sorted_table_really_is_written_in_schema_order(built):
    path = built[0] / "sequenceReference.parquet"
    got = [r[0] for r in duckdb.connect().execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()]
    assert got == [c.name for c in COMPILED.table("sequenceReference").columns]
    assert got != [c.name for c in GOLDEN.table("sequenceReference").columns]


# --- the checksum covers what affects the tables -----------------------------------------------------------------


def _raw() -> dict:
    return copy.deepcopy(compile_sections(SCHEMAS, COLUMN_SET.doc))


@pytest.mark.parametrize(
    "change",
    [
        lambda r: r["tables"][0].update(description="reworded"),
        lambda r: r["tables"][0]["columns"][1].update(description="reworded"),
        lambda r: r["tables"][1]["columns"][-2]["derivation"].update(transform="reworded"),
        lambda r: r.update(sources=["reworded"]),
        lambda r: r.update(status="released"),
    ],
)
def test_rewording_leaves_the_checksum_unchanged(change):
    raw = _raw()
    change(raw)
    assert table_spec_sha256(raw) == SOURCE.sha256


@pytest.mark.parametrize(
    "change",
    [
        lambda r: r["tables"][0]["columns"][1].update(type="BIGINT"),
        lambda r: r["tables"][0]["columns"].reverse(),
        lambda r: r["tables"][0]["columns"][1]["derivation"].update(path="$.other"),
        lambda r: r["tables"][1]["columns"][3]["derivation"].update(targets=["gene"]),
        lambda r: r["tables"][0]["json"].update(key_order="columns"),
        lambda r: r["tables"][0]["json"].update(record_id=True),
        lambda r: r["root"].update(version_property=None),
        lambda r: r["tables"].reverse(),
    ],
)
def test_changing_what_builds_the_tables_changes_the_checksum(change):
    raw = _raw()
    change(raw)
    assert table_spec_sha256(raw) != SOURCE.sha256


def test_the_spec_for_sorted_input_changes_only_the_key_order():
    raw = _raw()
    sorted_raw = spec_for_sorted_input(raw)
    assert table_spec_sha256(sorted_raw) != SOURCE.sha256
    for before, after in zip(raw["tables"], sorted_raw["tables"], strict=True):
        assert before["columns"] == after["columns"]
        assert after["json"] == {"key_order": "sorted", "record_id": before["json"]["record_id"]}


# --- column-set strictness ---------------------------------------------------------------------------------------


def _set_path(doc: dict, path: tuple, value: object) -> None:
    node = doc["sections"]
    for step in path[:-1]:
        node = node[step]
    node[path[-1]] = value


@pytest.mark.parametrize(
    "change, message",
    [
        # Never used.
        (lambda o: o["sections"].update(notASection={"id": None}), r"sections names \['notASection'\]"),
        (lambda o: o["key_order"].update(notASection=None), r"key_order names \['notASection'\]"),
        (
            lambda o: o["key_order"].update(condition={"sorted_columns": ["nope"]}),
            r"section condition.*sorted_columns names \['nope'\], which are not columns of condition",
        ),
        (
            lambda o: o["sections"].update(gene=None),
            "section gene: .*the collection has no entry in the column set",
        ),
        (lambda o: o["sections"].pop("gene"), "section gene: .*the collection has no entry in the column set"),
        (lambda o: o.update(collections=["gene"]), "collections names 'gene', but the schema already says"),
        (lambda o: o.update(collections=["schema_version"]), "'schema_version' is the version, not a collection"),
        (lambda o: o.update(collections=["nope"]), "collections names 'nope', not a root property"),
        (lambda o: o.update(version_property="nope"), "version_property 'nope' is not a root property"),
        # The pairing.
        (
            lambda o: o.update(schema="urn:other"),
            "it says it pairs with schema 'urn:other', but the schema's \\$id is",
        ),
        # A position the schema doesn't have.
        (
            lambda o: _set_path(o, ("location", "nonsense"), None),
            r"section location: position \(the record\): names \['nonsense'\].*it has: id, type",
        ),
        (
            lambda o: _set_path(o, ("allele", "state", "bogus"), None),
            r"section allele: position state: names \['bogus'\]",
        ),
        # Contradicting the schema's type.
        (
            lambda o: _set_path(o, ("location", "digest"), "integer"),
            "position digest: contradicts the schema: it says integer, the schema allows string",
        ),
        (
            lambda o: _set_path(o, ("copyNumberCount", "copies"), "string"),
            "position copies: contradicts the schema: it says string, the schema allows integer, a Range",
        ),
        (
            lambda o: _set_path(o, ("copyNumberCount", "copies"), "->location"),
            "position copies: contradicts the schema: it says a pointer, the schema allows integer, a Range",
        ),
        (
            lambda o: _set_path(o, ("gene", "primaryCoding"), "->gene"),
            "position primaryCoding: contradicts the schema: it says a pointer, the schema allows an object",
        ),
        (
            lambda o: _set_path(o, ("location", "digest"), {"a": None}),
            "position digest: contradicts the schema: it says an object, the schema allows string",
        ),
        (
            lambda o: _set_path(o, ("location", "digest"), ["string"]),
            "position digest: contradicts the schema: it says a list, the schema allows string",
        ),
        (lambda o: _set_path(o, ("location", "digest"), "strng"), "position digest: unknown shape 'strng'"),
        (
            lambda o: _set_path(o, ("location", "sequenceReference"), "->nowhere"),
            r"position sequenceReference: a pointer names \['nowhere'\], not collections",
        ),
        (
            lambda o: _set_path(o, ("sequenceReference", "extensions"), {"a": None}),
            "position extensions: an extensions shape is the list of its value shapes",
        ),
        (
            lambda o: _set_path(o, ("allele", "state", "sequence"), "json"),
            "`json` is for a property of the record",
        ),
        (lambda o: _set_path(o, ("gene", "id as gene_id"), None), "lists 'id' twice"),
    ],
)
def test_a_bad_column_set_entry_stops_the_compile(change, message):
    doc = columns()
    change(doc)
    with pytest.raises(CompileError, match=message):
        compile_sections(SCHEMAS, doc)


def test_the_record_id_cant_be_renamed():
    doc = columns()
    entry = doc["sections"]["submitter"]
    entry.pop("id")
    entry["id as x"] = None
    with pytest.raises(CompileError, match="it can't be renamed"):
        compile_sections(SCHEMAS, doc)


def test_an_open_extension_value_may_name_positions_the_schema_lacks():
    doc = columns()
    doc["sections"]["sequenceReference"]["extensions"] = [{"bogus": "string"}]
    t = parse_spec(compile_sections(SCHEMAS, doc)).table("sequenceReference")
    assert "value_object STRUCT(bogus VARCHAR)" in t.column("extensions").type


def test_an_open_position_must_state_its_type():
    doc = columns()
    doc["sections"]["sequenceReference"]["extensions"] = [{"bogus": None}]
    with pytest.raises(CompileError, match=r"position extensions\.value\.bogus: the schema says nothing"):
        compile_sections(SCHEMAS, doc)


def test_a_column_set_keys_are_checked_when_it_loads():
    with pytest.raises(CompileError, match=r"unknown key\(s\) \['notes'\]"):
        parse_column_set(b"schema: x\nsections: {a: {b}}\nnotes: y\n", "here")
    with pytest.raises(CompileError, match="`schema` must name the \\$id"):
        parse_column_set(b"sections: {a: {b}}\n", "here")
    with pytest.raises(CompileError, match="`sections` must map"):
        parse_column_set(b"schema: x\n", "here")
    with pytest.raises(CompileError, match=r"can't read it.*\['a'\] written twice"):
        parse_column_set(b"schema: x\nsections:\n  t: {a, b, a}\n", "here")


def test_the_1_1_0_column_set_entries_parse_as_flow_mappings():
    assert COLUMN_SET.doc["sections"]["location"]["id"] is None
    assert COLUMN_SET.doc["sections"]["rcv"] is COLUMN_SET.doc["sections"]["vcv"]


# --- fail-loud rules: small hand-written schema sets -------------------------------------------------------------

ROOT = "urn:example:bundle:1"
W3 = "https://w3id.org/example/json"
STRING = {"type": "string"}


def closed(**properties) -> dict:
    return {"type": "object", "properties": properties, "additionalProperties": False}


def make_set(sections: dict[str, dict], classes: dict[str, dict] | None = None, **bundle_keys) -> SchemaSet:
    """A schema set: sections (name -> record schema) and classes (name -> schema, at W3/<name>)."""
    props = {
        name: {"type": "object", "patternProperties": {".+": record}, "additionalProperties": False}
        for name, record in sections.items()
    }
    bundle = {"$id": ROOT, "properties": props, **bundle_keys}
    files = {ROOT: SchemaFile(ROOT, ROOT, json.dumps(bundle).encode(), bundle)}
    for name, doc in (classes or {}).items():
        sid = f"{W3}/{name}"
        doc = {"$id": sid, **doc}
        files[sid] = SchemaFile(sid, sid, json.dumps(doc).encode(), doc)
    return SchemaSet(ROOT, files)


def ref(name: str) -> dict:
    return {"$ref": f"{W3}/{name}"}


def all_of(schema_set: SchemaSet, **overrides) -> dict:
    """A column set taking every declared property of every collection as the schema states it."""
    sections = {}
    for name, node in schema_set.bundle["properties"].items():
        if "patternProperties" not in node:
            continue
        record = next(iter(node["patternProperties"].values()))
        sections[name] = dict.fromkeys(record.get("properties", {})) or {"x": None}
    return {"schema": schema_set.bundle_id, "sections": sections, **overrides}


def compile_all(schema_set: SchemaSet, **overrides) -> dict:
    return compile_sections(schema_set, all_of(schema_set, **overrides))


RANGE = {
    "type": "array",
    "minItems": 2,
    "maxItems": 2,
    "items": {"oneOf": [{"type": "integer"}, {"type": "null"}]},
}
INT_OR_RANGE = {"oneOf": [{"type": "integer"}, RANGE]}
IRI = {"type": "string", "format": "iri-reference"}


@pytest.mark.parametrize(
    "record, classes, message",
    [
        (closed(x={"type": "object"}), None, "section t: position x: is an open object; the column set must list"),
        (
            closed(x={"type": "object", "properties": {"a": STRING}, "additionalProperties": True}),
            None,
            "section t: position x: is an open object",
        ),
        (
            closed(x=ref("Node")),
            {"Node": closed(child=ref("Node"))},
            "section t: position x.child: class .*Node.* reaches itself",
        ),
        (
            closed(x=ref("A")),
            {"A": closed(b=ref("B")), "B": closed(a=ref("A"))},
            "position x.b.a: class .*A.* reaches itself",
        ),
        (
            closed(x=ref("Loop")),
            {"Loop": {"oneOf": [ref("Loop"), STRING]}},
            "position x: class .*Loop.* reaches itself",
        ),
        (
            closed(x=closed(n=INT_OR_RANGE)),
            None,
            "position x.n: an integer-or-Range inside a value has no column form",
        ),
        (
            closed(x={"type": "array", "items": INT_OR_RANGE}),
            None,
            "position x: an integer-or-Range inside a value",
        ),
        (closed(x={"type": ["string", "integer"]}), None, "position x: is a union of string, integer"),
        (closed(x={"oneOf": [STRING, {"type": "boolean"}]}), None, "position x: is a union of string, boolean"),
        (closed(x=RANGE), None, "position x: is a Range with no integer form"),
        (closed(x=ref("Missing")), None, r"position x: unknown \$ref.*no schema .*Missing in the set"),
        (closed(x={"$ref": "#/$defs/Missing"}), None, r"position x: unknown \$ref.*no '\$defs'"),
        (closed(x={"oneOf": [IRI, closed(a=STRING)]}), None, "position x: may be a string or an inline object"),
        (closed(x={"description": "nothing"}), None, "position x: the schema says nothing about its value"),
        (closed(x={"type": "array"}), None, "position x: is an array with no `items`"),
        (
            closed(x={"if": {}, "then": {}, "type": "string"}),
            None,
            "position x: uses if, which the rules can't read",
        ),
        (
            closed(x={"type": "object", "additionalProperties": STRING}),
            None,
            "position x: is a map whose keys are data, which has no typed form; the column set must state it as ",
        ),
        (closed(id={"type": "integer"}), None, "position id: the record's id must be a string"),
        # Extensions.
        (
            closed(extensions={"type": "string"}),
            None,
            "position extensions: an extensions property is string, not a list",
        ),
        (
            closed(extensions={"type": "array", "items": {"type": "object"}}),
            None,
            r"position extensions: its elements are not \{name, value\}",
        ),
        (
            closed(extensions={"type": "array", "items": closed(name=STRING, value=STRING, extra=STRING)}),
            None,
            r"its elements are not \{name, value\} \(extra: \['extra'\]\)",
        ),
        (
            closed(extensions={"type": "array", "items": closed(name=STRING, value={"type": "object"})}),
            None,
            "position extensions.value: the value is open",
        ),
        (
            closed(extensions={"type": "array", "items": closed(name=STRING, value={"type": "array"})}),
            None,
            "position extensions.value: the value is an array with no `items`",
        ),
    ],
)
def test_a_pattern_the_rules_dont_cover_stops_the_compile(record, classes, message):
    schema_set = make_set({"t": record, "u": closed(a=STRING)}, classes)
    with pytest.raises(CompileError, match=message):
        compile_all(schema_set)


def test_a_failure_says_what_the_column_set_must_state():
    schema_set = make_set({"t": closed(x={"oneOf": [IRI, closed(a=STRING)]})})
    with pytest.raises(CompileError) as e:
        compile_all(schema_set)
    assert str(e.value).startswith(f"schema {ROOT}: section t: position x:")
    assert "the column set must state which: `->section` for a pointer" in str(e.value)


def test_a_multi_target_pointer_inside_a_value_stops_the_compile():
    schema_set = make_set({"t": closed(x=closed(p=STRING)), "u": closed(a=STRING)})
    doc = all_of(schema_set)
    doc["sections"]["t"] = {"x": {"p": "->t|u"}}
    with pytest.raises(CompileError, match=r"position x.p: a pointer inside a value has several targets \(t, u\)"):
        compile_sections(schema_set, doc)


def test_a_multi_target_pointer_as_an_extension_slot_stops_the_compile():
    extensions = {"type": "array", "items": closed(name=STRING, value=STRING)}
    schema_set = make_set({"t": closed(extensions=extensions), "u": closed(a=STRING)})
    doc = all_of(schema_set)
    doc["sections"]["t"] = {"extensions": ["->t|u"]}
    with pytest.raises(
        CompileError, match="position extensions.value: a pointer inside a value has several targets"
    ):
        compile_sections(schema_set, doc)


def test_a_bundle_level_allof_the_rules_cant_read_stops_the_compile():
    with pytest.raises(CompileError, match=r"allOf\[0\]: the rules read only patternProperties"):
        compile_all(make_set({"t": closed(x=STRING)}, allOf=[{"required": ["t"]}]))


def test_a_bad_section_pattern_in_a_bundle_allof_stops_the_compile():
    allof = [{"patternProperties": {"(": {"additionalProperties": closed()}}}]
    with pytest.raises(CompileError, match=r"allOf\[0\]: bad pattern '\('"):
        compile_all(make_set({"t": closed(x=STRING)}, allOf=allof))


def test_a_schema_of_the_wrong_json_shape_stops_the_compile():
    schema_set = make_set({"t": closed(x=STRING)})
    record = schema_set.bundle["properties"]["t"]["patternProperties"][".+"]
    record["allOf"] = [5]
    with pytest.raises(CompileError, match="section t: malformed"):
        compile_all(schema_set)


def test_a_bundle_with_no_collections_stops_the_compile():
    with pytest.raises(CompileError, match="it has no collections"):
        compile_sections(make_set({}), {"schema": ROOT, "sections": {"a": {"b": None}}})


def test_a_collection_cant_take_the_root_values_tables_name():
    schema_set = make_set({"root_values": closed(x=STRING)})
    with pytest.raises(CompileError, match="can't be named root_values"):
        compile_all(schema_set)


def test_an_integer_or_range_at_the_top_level_is_three_columns_and_may_be_renamed():
    schema_set = make_set({"t": closed(id=STRING, n=INT_OR_RANGE)})
    t = parse_spec(compile_all(schema_set)).table("t")
    assert [(c.name, c.type, c.derivation.kind, c.derivation.path) for c in t.columns[1:-1]] == [
        ("n", "BIGINT", "range-exact", "$.n"),
        ("n_min", "BIGINT", "range-min", "$.n"),
        ("n_max", "BIGINT", "range-max", "$.n"),
    ]
    assert t.json.record_id is True and t.json.key_order == "sorted"
    doc = all_of(schema_set)
    doc["sections"]["t"] = {"id": None, "n as m": None}
    t = parse_spec(compile_sections(schema_set, doc)).table("t")
    assert [c.name for c in t.columns] == ["id", "m", "m_min", "m_max", "ordinal"]
    assert {c.derivation.path for c in t.columns[1:-1]} == {"$.n"}


def test_a_section_with_no_id_property_has_no_record_id():
    assert parse_spec(compile_all(make_set({"t": closed(x=STRING)}))).table("t").json.record_id is False


def test_a_union_merges_first_branch_then_new_fields_and_allof_narrows():
    base = closed(a=STRING, b={"type": "integer"})
    # The profile narrows `a`'s value to a const of the same type; `minimum` says nothing about the columns.
    narrowing = {"properties": {"a": {"const": "x"}, "b": {"minimum": 1}}}
    profile = {"allOf": [ref("Base"), narrowing], "type": "object", "unevaluatedProperties": False}
    record = closed(x={"anyOf": [ref("Profile"), closed(c=STRING, a=STRING)]})
    spec = parse_spec(compile_all(make_set({"t": record}, {"Base": base, "Profile": profile})))
    assert spec.table("t").column("x").type == "STRUCT(a VARCHAR, b BIGINT, c VARCHAR)"


def test_an_allof_narrowing_keeps_what_its_base_declares():
    """A profile's {properties: {...}} on a property its base declares leaves the base's other properties there."""
    base = closed(s={"oneOf": [closed(type=STRING, code=STRING), IRI]})
    profile = {"allOf": [ref("Base"), {"properties": {"s": {"properties": {"code": {"const": "x"}}}}}]}
    schema_set = make_set({"t": profile}, {"Base": base})
    doc = {"schema": ROOT, "sections": {"t": {"s": {"type": None, "code": None}}}}
    assert parse_spec(compile_sections(schema_set, doc)).table("t").column("s").type == (
        'STRUCT("type" VARCHAR, code VARCHAR)'
    )


def test_a_pointer_shape_is_a_bare_id_in_the_column_with_its_section_when_it_has_several():
    record = closed(a=IRI, b={"type": "array", "items": IRI}, c=closed(d=IRI), e=IRI)
    schema_set = make_set({"t": record, "u": closed(x=STRING)})
    doc = all_of(schema_set)
    doc["sections"]["t"] = {"a": "->u", "b": "->t|u", "c": {"d": "->u"}, "e": None}
    t = parse_spec(compile_sections(schema_set, doc)).table("t")
    assert (t.column("a").derivation.kind, t.column("a").derivation.targets) == ("bare-id", ("u",))
    assert t.column("b").derivation.targets == ("t", "u") and t.column("b_section").type == "VARCHAR[]"
    assert dict(zip(t.column("c").derivation.pointers, t.column("c").derivation.pointer_targets, strict=True)) == {
        ".d": ("u",)
    }
    assert (t.column("e").derivation.kind, t.column("e").type) == ("scalar-path", "VARCHAR")


def test_closed_extensions_are_read_from_the_schema_with_slots_in_order():
    item = closed(name=STRING, value={"type": "array", "items": closed(g=STRING)})
    extensions = {
        "type": "array",
        "items": {
            "oneOf": [
                item,
                closed(name=STRING, value={"type": "boolean"}),
                closed(name=STRING, value={"type": "array", "items": STRING}),
                closed(name=STRING, value=STRING),
                closed(name=STRING, value={"type": "number"}),
            ]
        },
    }
    t = parse_spec(compile_all(make_set({"t": closed(extensions=extensions)}))).table("t")
    c = t.column("extensions")
    assert c.type == (
        'STRUCT("name" VARCHAR, value_string VARCHAR, value_boolean BOOLEAN, value_number DOUBLE, '
        "value_string_array VARCHAR[], value_object_array STRUCT(g VARCHAR)[])[]"
    )
    assert c.derivation.kind == "extension-slot"
    assert t.extension_slots == {"extensions": "json-type"}


def test_nested_extensions_mark_the_column_for_json_type_slots():
    extensions = {"type": "array", "items": closed(name=STRING, value=STRING)}
    t = parse_spec(compile_all(make_set({"t": closed(x=closed(extensions=extensions))}))).table("t")
    assert t.column("x").derivation.kind == "typed-copy" and t.extension_slots == {"x": "json-type"}


def test_a_number_position_is_a_double_and_integer_narrows_it():
    schema_set = make_set({"t": closed(x={"type": "number"}, y={"type": "number"})})
    doc = all_of(schema_set)
    doc["sections"]["t"] = {"x": None, "y": "integer"}
    t = parse_spec(compile_sections(schema_set, doc)).table("t")
    assert (t.column("x").type, t.column("y").type) == ("DOUBLE", "BIGINT")


def test_json_keeps_a_map_or_a_pointer_or_object_position_as_text():
    record = closed(m={"type": "object", "additionalProperties": STRING}, p={"oneOf": [IRI, closed(a=STRING)]})
    schema_set = make_set({"t": record})
    doc = all_of(schema_set)
    doc["sections"]["t"] = {"m": "json", "p": "json"}
    t = parse_spec(compile_sections(schema_set, doc)).table("t")
    assert {c.derivation.kind for c in t.columns[1:-1]} == {"json-text"}
    assert {c.type for c in t.columns[1:-1]} == {"VARCHAR"}
    doc["sections"]["t"] = {"m": {"a": "string"}, "p": "json"}
    with pytest.raises(CompileError, match="makes this a map whose keys are data"):
        compile_sections(schema_set, doc)


# --- the root ----------------------------------------------------------------------------------------------------


def test_the_schema_says_which_root_properties_are_collections():
    bundle = {
        "$id": ROOT,
        "$defs": {"M": {"properties": {"a": STRING}}, "C": {"patternProperties": {".": closed(x=STRING)}}},
        "properties": {
            "version": {"const": "1"},
            "by_pattern": {"patternProperties": {".": closed(x=STRING)}},
            "by_ref": {"$ref": "#/$defs/C"},
            "by_additional": {"type": "object", "additionalProperties": closed(x=STRING)},
            "metadata": {"$ref": "#/$defs/M"},
            "listed": {"type": "array", "items": STRING},
            "scalar": STRING,
            "silent": {"type": "object"},
        },
    }
    schema_set = SchemaSet(ROOT, {ROOT: SchemaFile(ROOT, ROOT, b"{}", bundle)})
    parts = root_parts(schema_set, "version")
    assert parts.collections == ("by_pattern", "by_ref", "by_additional") and parts.other_values
    assert root_parts(schema_set, "version", ["silent"]).collections[-1] == "silent"
    with pytest.raises(CompileError, match="says it is not a collection"):
        root_parts(schema_set, "version", ["listed"])


def test_a_root_closed_to_other_properties_holding_only_collections_has_no_other_values():
    schema_set = make_set({"t": closed(x=STRING)}, additionalProperties=False)
    assert root_parts(schema_set).other_values is False
    assert root_parts(make_set({"t": closed(x=STRING)})).other_values is True


def test_the_format_docs_small_example_compiles():
    text = (Path(__file__).resolve().parents[1] / "docs" / "column-set-format.md").read_text()
    doc = yaml.safe_load(re.search(r"```yaml\n(.*?)```", text, re.S).group(1))
    extensions = {"type": "array", "items": {"$ref": f"{W3}/Extension"}}
    open_value = {"type": ["number", "string", "boolean", "object", "array", "null"]}
    coding = closed(code=STRING, system=STRING, extra=STRING)
    mapping = closed(coding=coding, relation=STRING)
    gene = closed(id=STRING, type=STRING, name=STRING, mappings={"type": "array", "items": mapping}, other=STRING)
    variant = closed(
        id=STRING, type=STRING, name=STRING, gene=IRI, start=INT_OR_RANGE, end=INT_OR_RANGE, extensions=extensions
    )
    schema_set = make_set({"gene": gene, "variant": variant}, {"Extension": closed(name=STRING, value=open_value)})
    bundle_id = doc["schema"]
    bundle = {**schema_set.bundle, "$id": bundle_id}
    files = {k: v for k, v in schema_set.files.items() if k != ROOT}
    files[bundle_id] = SchemaFile(bundle_id, bundle_id, b"{}", bundle)
    t = checked(compile_sections(SchemaSet(bundle_id, files), doc)).table("variant")
    assert [c.name for c in t.columns] == [
        "id",
        "type",
        "name",
        "gene",
        "start",
        "start_min",
        "start_max",
        "stop_pos",
        "stop_pos_min",
        "stop_pos_max",
        "extensions",
        "ordinal",
    ]
    assert t.column("extensions").type == 'STRUCT("name" VARCHAR, value_string VARCHAR, value_boolean BOOLEAN)[]'
