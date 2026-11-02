"""The sections compiler (ADR 0006): on the real 1.1.0 schemas it reproduces the checked-in spec and the build; it
refuses a bad overlay and any pattern its rules don't cover; a schema set that meets the contract needs no overlay.

Offline: the schemas come from the shipped snapshot. Bundle records are real (the excerpt, tests/_fixture.py). The
small schema sets of the fail-loud tests and tests/fixtures/schema-contract/ are hand-written schemas, not records.
"""

from __future__ import annotations

import copy
import gzip
import io
import json
import re
import shutil
from pathlib import Path

import duckdb
import pytest
import yaml
from _fixture import FIXTURE, FIXTURES, GOLDEN_SECTIONS_SPEC, SOURCE_RELEASE
from _input_helpers import bundle_bytes, fixture_sections, local_input, write_gz

from clinvar_gkm_parquet import build as build_mod
from clinvar_gkm_parquet import compiler as compiler_mod
from clinvar_gkm_parquet import sources, verify
from clinvar_gkm_parquet.bundle import peek_schema_version
from clinvar_gkm_parquet.cli import main
from clinvar_gkm_parquet.compiler import compile_sections, compiled_spec, load_overlay, overlay_path, spec_sha256
from clinvar_gkm_parquet.errors import CompileError, InputError, SpecError, ToolError
from clinvar_gkm_parquet.model import BuildInputs, InputFile
from clinvar_gkm_parquet.roundtrip import rebuild
from clinvar_gkm_parquet.schemas import SchemaFile, SchemaSet, load_schema_set, read_set, sections_of, write_set
from clinvar_gkm_parquet.spec import Spec, Table, check_compiled, load_spec, parse_spec, resolve_spec

SCHEMAS = load_schema_set("1.1.0")
GOLDEN = parse_spec(yaml.safe_load(GOLDEN_SECTIONS_SPEC.read_text(encoding="utf-8")), require_name=True)


def overlay() -> dict:
    return copy.deepcopy(load_overlay("1.1.0"))


def checked(raw: dict) -> Spec:
    spec = parse_spec(raw, require_name=True)
    check_compiled(spec, "the test's spec")
    return spec


COMPILED, SOURCE = compiled_spec("1.1.0")


# --- golden: the checked-in spec ---------------------------------------------------------------------------------


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
# and field order of the key-sorted tables (which ADR 0006 allows, because the bundle text doesn't depend on it).
# Empty: the compiler reproduces every table.
ACCEPTED_DIFFERENCES: dict[str, list[str]] = {}


@pytest.mark.parametrize("golden", GOLDEN.tables, ids=lambda t: t.name)
def test_compiled_table_equals_the_checked_in_table(golden):
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


def test_tables_are_the_bundle_schema_sections_in_order():
    assert tuple(t.name for t in COMPILED.tables) == sections_of("1.1.0")
    assert [t.name for t in COMPILED.tables] == [t.name for t in GOLDEN.tables]


def test_compiled_spec_is_what_the_tool_resolves():
    assert compile_sections(SCHEMAS, load_overlay("1.1.0")) is not None
    assert (SOURCE.name, SOURCE.version, SOURCE.builtin, SOURCE.path) == ("sections", "1.0", True, None)
    assert SOURCE.sha256 == spec_sha256(compile_sections(SCHEMAS, load_overlay("1.1.0")))
    assert SOURCE.schema_set_sha256 == SCHEMAS.sha256


def test_compiled_spec_keeps_the_checked_in_top_level_facts():
    assert COMPILED.lossless and COMPILED.writer == GOLDEN.writer
    assert (COMPILED.name, COMPILED.spec_version) == (GOLDEN.name, GOLDEN.spec_version)
    assert COMPILED.bundle_schema_versions == ("1.1.0",)
    assert SCHEMAS.sha256 in COMPILED.sources[0]


def test_sorted_tables_take_schema_order_and_columns_tables_the_overlays():
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
    # Schema order: Allele declares location before state, though the overlay lists neither first.
    names = [c.name for c in COMPILED.table("allele").columns]
    assert names.index("name") < names.index("expressions") < names.index("location") < names.index("state")


def test_output_is_deterministic_and_ignores_overlay_order_in_a_sorted_table():
    assert compile_sections(SCHEMAS, load_overlay("1.1.0")) == compile_sections(SCHEMAS, load_overlay("1.1.0"))
    ov = overlay()
    ov["sections"]["location"] = dict(reversed(list(ov["sections"]["location"].items())))
    assert compile_sections(SCHEMAS, ov) == compile_sections(SCHEMAS, load_overlay("1.1.0"))


def test_overlay_order_sets_the_columns_of_a_columns_table():
    ov = overlay()
    ov["sections"]["submitter"] = {"id": None, "name": None, "type": None}
    assert [c.name for c in checked(compile_sections(SCHEMAS, ov)).table("submitter").columns] == [
        "id",
        "type",
        "name",
        "ordinal",
    ]
    ov["key_order"]["submitter"] = None
    assert [c.name for c in parse_spec(compile_sections(SCHEMAS, ov)).table("submitter").columns] == [
        "id",
        "name",
        "type",
        "ordinal",
    ]


def test_descriptions_come_from_the_schema_and_transforms_from_the_rules():
    t = COMPILED.table("location")
    assert t.description == "VRS SequenceLocation records."
    assert t.column("sequenceReference").description.startswith("A reference to a SequenceReference")
    assert t.column("sequenceReference").derivation.transform == "strip ^#/[^/]+/"
    assert COMPILED.table("variation").column("members_section").derivation.transform.startswith("the <section>")


# --- build equivalence: the real excerpt -------------------------------------------------------------------------


def _full(path: Path, schema_version_release: str = SOURCE_RELEASE) -> BuildInputs:
    inp = local_input(path, release=schema_version_release)
    return BuildInputs(full=inp, full_source_release=schema_version_release)


def _rows(directory: Path, table: Table, names: list[str]) -> list[tuple]:
    selected = ", ".join(f'"{n}"' for n in names)
    path = directory / table.file_name
    return duckdb.connect().execute(f"SELECT {selected} FROM read_parquet('{path}') ORDER BY id").fetchall()


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> tuple[Path, Path]:
    root = tmp_path_factory.mktemp("compiled")
    build_mod.build(_full(FIXTURE), root / "compiled", spec=COMPILED, spec_source=SOURCE)
    build_mod.build(_full(FIXTURE), root / "checked-in", spec=GOLDEN)
    return root / "compiled", root / "checked-in"


def test_build_with_the_compiled_spec_rebuilds_the_bundle_text(built, tmp_path):
    out = tmp_path / "rebuilt.json"
    result = rebuild(built[0], out)
    assert out.read_bytes() == gzip.decompress(FIXTURE.read_bytes())
    assert result.text_sha256 == result.expected_text_sha256


@pytest.mark.parametrize("golden", GOLDEN.tables, ids=lambda t: t.name)
def test_rows_equal_those_of_the_checked_in_spec_by_column_name(built, golden):
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


# --- overlay strictness ------------------------------------------------------------------------------------------


def _set_path(ov: dict, path: tuple, value: object) -> None:
    node = ov["sections"]
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
        (lambda o: o["sections"].update(gene=None), r"sections: \['gene'\] state nothing"),
        (
            lambda o: _set_path(
                o,
                ("allele", "expressions"),
                {"id": None, "extensions": None, "syntax": None, "value": None, "syntax_version": None},
            ),
            r"section allele: position expressions: repeats the schema",
        ),
        (lambda o: o.update(notes="x"), r"unknown key\(s\) \['notes'\]"),
        (lambda o: o.update(schema_version="1.0.1"), "says it is for schema_version '1.0.1'"),
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
        (
            lambda o: _set_path(o, ("location", "digest"), "strng"),
            "position digest: unknown shape 'strng'",
        ),
        (
            lambda o: _set_path(o, ("location", "sequenceReference"), "->nowhere"),
            r"position sequenceReference: a pointer names \['nowhere'\], not sections",
        ),
        (
            lambda o: _set_path(o, ("sequenceReference", "extensions"), {"a": None}),
            "position extensions: an extensions shape is the list of its value shapes",
        ),
    ],
)
def test_a_bad_overlay_entry_stops_the_compile(change, message):
    ov = overlay()
    change(ov)
    with pytest.raises(CompileError, match=message):
        compile_sections(SCHEMAS, ov)


def test_an_open_extension_value_may_name_positions_the_schema_lacks():
    ov = overlay()
    ov["sections"]["sequenceReference"]["extensions"] = [{"bogus": "string"}]
    t = parse_spec(compile_sections(SCHEMAS, ov)).table("sequenceReference")
    assert "value_object STRUCT(bogus VARCHAR)" in t.column("extensions").type


def test_an_open_position_must_state_its_type():
    ov = overlay()
    ov["sections"]["sequenceReference"]["extensions"] = [{"bogus": None}]
    with pytest.raises(
        CompileError, match=r"section sequenceReference: position extensions\.value\.bogus: says nothing"
    ):
        compile_sections(SCHEMAS, ov)


def test_an_entry_written_twice_is_refused(tmp_path, monkeypatch):
    path = tmp_path / "sections" / "overlays" / "6.6.6.yaml"
    path.parent.mkdir(parents=True)
    path.write_text('schema_version: "6.6.6"\nsections:\n  t: {a, b, a}\n')
    monkeypatch.setattr(compiler_mod, "overlay_path", lambda version: path)
    with pytest.raises(CompileError, match=r"can't read the overlay.*\['a'\] written twice"):
        load_overlay("6.6.6")


def test_a_version_without_an_overlay_has_none():
    assert load_overlay("0.0.0") is None and overlay_path("1.1.0").name == "1.1.0.yaml"


def test_1_1_0_needs_its_overlay():
    with pytest.raises(CompileError, match="section sequenceReference: position extensions: its elements"):
        compile_sections(SCHEMAS, None)


def test_overlay_entries_are_valid_yaml_flow_mappings():
    ov = yaml.safe_load(overlay_path("1.1.0").read_text())
    assert ov["sections"]["location"]["id"] is None
    assert ov["sections"]["rcv"] is ov["sections"]["vcv"]


# --- fail-loud rules: small hand-written schema sets -------------------------------------------------------------

V = "7.7.0"
ROOT = f"https://w3id.org/clingen/schema/clinvar-gkm/{V}"
STRING = {"type": "string"}


def closed(**properties) -> dict:
    return {"type": "object", "properties": properties, "additionalProperties": False}


def make_set(sections: dict[str, dict], classes: dict[str, dict] | None = None, **bundle_keys) -> SchemaSet:
    """A schema set: sections (name -> record schema) and classes (name -> schema, at ROOT/json/<name>)."""
    bundle_id = f"{ROOT}/bundle"
    props = {
        name: {"type": "object", "patternProperties": {".+": record}, "additionalProperties": False}
        for name, record in sections.items()
    }
    bundle = {"$id": bundle_id, "properties": {"schema_version": STRING, **props}, **bundle_keys}
    files = {bundle_id: SchemaFile(bundle_id, bundle_id, json.dumps(bundle).encode(), bundle)}
    for name, doc in (classes or {}).items():
        sid = f"{ROOT}/json/{name}"
        doc = {"$id": sid, **doc}
        files[sid] = SchemaFile(sid, sid, json.dumps(doc).encode(), doc)
    return SchemaSet(V, bundle_id, files)


def ref(name: str) -> dict:
    return {"$ref": f"{ROOT}/json/{name}"}


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
        (
            closed(x={"type": "object"}),
            None,
            r"section t: position x: is an open object; the schema must define it",
        ),
        (
            closed(x={"type": "object", "properties": {"a": STRING}, "additionalProperties": True}),
            None,
            "section t: position x: is an open object",
        ),
        (
            {"type": "object", "properties": {"x": STRING}},
            None,
            r"section t: position \(the record\): is an open object",
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
        (closed(x={"type": "number"}), None, "position x: is a float"),
        (closed(x={"description": "nothing"}), None, "position x: says nothing about its value"),
        (closed(x={"type": "array"}), None, "position x: is an array with no `items`"),
        (
            closed(x={"if": {}, "then": {}, "type": "string"}),
            None,
            "position x: uses if, which the rules can't read",
        ),
        (
            closed(x={"type": "object", "additionalProperties": STRING}),
            None,
            "position x: uses additionalProperties with a schema",
        ),
        (
            closed(x={"type": "string", "pattern": "^#/.*"}),
            None,
            "position x: uses pointer pattern '.*', not of the form",
        ),
        (
            closed(x={"type": "string", "pattern": "^#/(nowhere)/"}),
            None,
            r"position x: a pointer names \['nowhere'\], not sections of the bundle schema",
        ),
        (
            closed(x=closed(p={"type": "string", "pattern": "^#/(t|u)/"})),
            None,
            r"position x.p: a pointer inside a value has several targets \(t, u\)",
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
        (
            closed(
                extensions={"type": "array", "items": closed(name=STRING, value={"type": ["number", "string"]})}
            ),
            None,
            "position extensions.value: is a float",
        ),
    ],
)
def test_a_pattern_the_rules_dont_cover_stops_the_compile(record, classes, message):
    schema_set = make_set({"t": record, "u": closed(a=STRING)}, classes)
    with pytest.raises(CompileError, match=message):
        compile_sections(schema_set)


def test_a_failure_says_what_the_schema_must_state():
    schema_set = make_set({"t": closed(x={"oneOf": [IRI, closed(a=STRING)]})})
    with pytest.raises(CompileError) as e:
        compile_sections(schema_set)
    assert str(e.value).startswith(f"schema set {V}: section t: position x:")
    assert "the schema must write a pointer as a string whose pattern names its target sections" in str(e.value)


def test_a_section_that_is_no_map_of_records_stops_the_compile():
    schema_set = make_set({})
    schema_set.bundle["properties"]["t"] = {"type": "object"}
    with pytest.raises(CompileError, match="section t: .*one patternProperties entry"):
        compile_sections(schema_set)


def test_a_bundle_level_allof_the_rules_cant_read_stops_the_compile():
    with pytest.raises(CompileError, match=r"bundle schema allOf\[0\]: the rules read only patternProperties"):
        compile_sections(make_set({"t": closed(x=STRING)}, allOf=[{"required": ["t"]}]))


def test_a_bundle_that_says_another_schema_version_stops_the_compile():
    schema_set = make_set({"t": closed(x=STRING)})
    schema_set.bundle["properties"]["schema_version"] = {"const": "1.1.0"}
    with pytest.raises(CompileError, match="the bundle schema says schema_version '1.1.0'"):
        compile_sections(schema_set)


def test_a_schema_of_the_wrong_json_shape_stops_the_compile():
    schema_set = make_set({"t": closed(x=STRING)})
    schema_set.bundle["properties"]["t"]["patternProperties"][".+"]["allOf"] = ["not a schema"]
    with pytest.raises(CompileError, match="section t: malformed"):
        compile_sections(schema_set)


def test_a_bad_section_pattern_in_a_bundle_allof_stops_the_compile():
    allof = [{"patternProperties": {"(": {"additionalProperties": closed()}}}]
    with pytest.raises(CompileError, match=r"bundle schema allOf\[0\]: bad pattern '\('"):
        compile_sections(make_set({"t": closed(x=STRING)}, allOf=allof))


def test_a_bundle_with_no_sections_stops_the_compile():
    with pytest.raises(CompileError, match="the bundle schema has no sections"):
        compile_sections(make_set({}))


def test_an_integer_or_range_at_the_top_level_is_three_columns():
    spec = parse_spec(compile_sections(make_set({"t": closed(id=STRING, n=INT_OR_RANGE)})))
    t = spec.table("t")
    assert [(c.name, c.type, c.derivation.kind, c.derivation.path) for c in t.columns[1:-1]] == [
        ("n", "BIGINT", "range-exact", "$.n"),
        ("n_min", "BIGINT", "range-min", "$.n"),
        ("n_max", "BIGINT", "range-max", "$.n"),
    ]
    assert t.json.record_id is True and t.json.key_order == "sorted"


def test_a_multi_target_pointer_as_an_extension_slot_stops_the_compile():
    value = {"type": "string", "pattern": "^#/(t|u)/"}
    extensions = {"type": "array", "items": closed(name=STRING, value=value)}
    with pytest.raises(
        CompileError, match=r"position extensions.value: a pointer inside a value has several targets"
    ):
        compile_sections(make_set({"t": closed(extensions=extensions), "u": closed(x=STRING)}))


def test_a_section_with_no_id_property_has_no_record_id():
    spec = parse_spec(compile_sections(make_set({"t": closed(x=STRING)})))
    assert spec.table("t").json.record_id is False


def test_a_union_merges_first_branch_then_new_fields_and_refines_allof():
    base = closed(a=STRING, b={"type": "integer"})
    # The profile narrows `a` (a later declaration replaces an earlier one); `minItems`-like keywords say nothing.
    profile = {
        "allOf": [ref("Base"), {"properties": {"a": {"type": "boolean"}, "b": {"minimum": 1}}}],
        "type": "object",
    }
    profile["unevaluatedProperties"] = False
    record = closed(x={"anyOf": [ref("Profile"), closed(c=STRING, a={"type": "boolean"})]})
    spec = parse_spec(compile_sections(make_set({"t": record}, {"Base": base, "Profile": profile})))
    assert spec.table("t").column("x").type == "STRUCT(a BOOLEAN, b BIGINT, c VARCHAR)"


def test_a_pointer_pattern_narrows_an_iri_reference_by_any_spelling():
    iri = {"$id": f"{ROOT}/json/iri", **IRI}
    record = closed(
        a={"$ref": f"{ROOT}/json/iri", "pattern": "^#/u/"},
        b={"type": "array", "items": {"type": "string", "pattern": "^#/(t|u)/.+$"}},
        c=closed(d={"type": "string", "pattern": "^#/(u)/"}),
        e=ref("iri"),
    )
    t = parse_spec(compile_sections(make_set({"t": record, "u": closed(x=STRING)}, {"iri": iri}))).table("t")
    assert (t.column("a").derivation.kind, t.column("a").derivation.targets) == ("bare-id", ("u",))
    assert t.column("b").derivation.targets == ("t", "u") and t.column("b_section").type == "VARCHAR[]"
    assert dict(zip(t.column("c").derivation.pointers, t.column("c").derivation.pointer_targets, strict=True)) == {
        ".d": ("u",)
    }
    assert (t.column("e").derivation.kind, t.column("e").type) == ("scalar-path", "VARCHAR")


def test_closed_extensions_are_read_from_the_schema_with_slots_in_order():
    item = closed(name=STRING, value={"type": "array", "items": closed(g={"type": "string", "pattern": "^#/u/"})})
    extensions = {
        "type": "array",
        "items": {
            "oneOf": [
                item,
                closed(name=STRING, value={"type": "boolean"}),
                closed(name=STRING, value={"type": "array", "items": STRING}),
                closed(name=STRING, value=STRING),
            ]
        },
    }
    t = parse_spec(compile_sections(make_set({"t": closed(extensions=extensions), "u": closed(x=STRING)}))).table(
        "t"
    )
    c = t.column("extensions")
    assert c.type == (
        'STRUCT("name" VARCHAR, value_string VARCHAR, value_boolean BOOLEAN, value_string_array VARCHAR[], '
        "value_object_array STRUCT(g VARCHAR)[])[]"
    )
    assert c.derivation.kind == "extension-slot" and c.derivation.pointers == ("[*].value_object_array[*].g",)
    assert t.extension_slots == {"extensions": "json-type"}


def test_nested_extensions_mark_the_column_for_json_type_slots():
    extensions = {"type": "array", "items": closed(name=STRING, value=STRING)}
    t = parse_spec(compile_sections(make_set({"t": closed(x=closed(extensions=extensions))}))).table("t")
    assert t.column("x").derivation.kind == "typed-copy" and t.extension_slots == {"x": "json-type"}


# --- the schema contract: no overlay -----------------------------------------------------------------------------

CONTRACT_VERSION = "9.9.0"
CONTRACT = FIXTURES / "schema-contract" / CONTRACT_VERSION
CONTRACT_SECTIONS = (
    "sequenceReference",
    "location",
    "allele",
    "copyNumberCount",
    "copyNumberChange",
    "gene",
    "variation",
    "condition",
    "conditionSet",
)


def contract_spec() -> dict:
    return compile_sections(read_set(CONTRACT_VERSION, CONTRACT))


def test_a_schema_set_meeting_the_contract_compiles_with_no_overlay():
    spec = parse_spec(contract_spec(), require_name=True)
    assert [t.name for t in spec.tables] == list(CONTRACT_SECTIONS)
    for golden_table in (GOLDEN.table(s) for s in CONTRACT_SECTIONS):
        t = spec.table(golden_table.name)
        got, want = _column_facts(t, ordered=False), _column_facts(golden_table, ordered=False)
        assert got == want, golden_table.name
        assert t.extension_slots == golden_table.extension_slots
        assert t.json.key_order == "sorted" and t.json.sorted_columns == ()
        assert t.json.record_id == golden_table.json.record_id


def test_the_contract_set_states_no_pointer_as_iri_reference_and_no_open_value():
    text = "".join(p.read_text() for p in CONTRACT.rglob("*") if p.is_file() and p.name != "MANIFEST.json")
    assert "iri-reference" not in text and 'additionalProperties": true' not in text
    assert re.search(r'"pattern": "\^#/\(allele\|copyNumberCount\|copyNumberChange\)/"', text)


def contract_text() -> bytes:
    """The excerpt's records of the contract's sections, keys sorted as the contract says, under its version."""
    sections = {
        name: [
            (
                key,
                json.dumps(json.loads(record), sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode(),
            )
            for key, record in records
        ]
        for name, records in fixture_sections().items()
        if name in CONTRACT_SECTIONS
    }
    return bundle_bytes(sections, schema_version=CONTRACT_VERSION)


@pytest.fixture
def cache(tmp_path, monkeypatch) -> Path:
    """Where the tool caches fetched schema sets, private to the test. A set put here is one it has fetched."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    return tmp_path / "xdg" / "clinvar-gkm-parquet" / "schemas"


def cli(capsys, *args) -> tuple[int, str, str]:
    rc = main([str(a) for a in args])
    got = capsys.readouterr()
    return rc, got.out, got.err


def test_real_records_with_sorted_keys_build_and_rebuild_under_the_contract(cache, tmp_path, capsys):
    shutil.copytree(CONTRACT, cache / CONTRACT_VERSION)
    text = contract_text()
    bundle_path = write_gz(tmp_path / "in" / "contract.json.gz", text)
    out = tmp_path / "out"
    rc, stdout, stderr = cli(
        capsys, "build", "--full", bundle_path, "--full-source-release", SOURCE_RELEASE, "--out", out
    )
    assert rc == 0, stderr
    assert "of spec sections 1.0" in stdout
    assert sorted(p.stem for p in out.glob("*.parquet")) == sorted(CONTRACT_SECTIONS)
    rc, stdout, stderr = cli(capsys, "verify", out, "--roundtrip")
    assert rc == 0, stdout + stderr
    rebuilt = tmp_path / "rebuilt.json"
    rc, _, stderr = cli(capsys, "rebuild", out, "-o", rebuilt)
    assert rc == 0, stderr
    assert rebuilt.read_bytes() == text
    count = duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{out}/variation.parquet')").fetchone()[0]
    assert count == len(fixture_sections()["variation"]) > 0


def test_a_section_outside_the_versions_schema_is_unknown_there(cache, tmp_path, capsys):
    shutil.copytree(CONTRACT, cache / CONTRACT_VERSION)
    records = fixture_sections()
    bad = write_gz(tmp_path / "in" / "b.json.gz", bundle_bytes({"scv": records["scv"]}, CONTRACT_VERSION))
    rc, _, stderr = cli(capsys, "build", "--full", bad, "--out", tmp_path / "out")
    assert rc == 2 and "unknown section 'scv' (a 9.9.0 bundle has: sequenceReference, location" in stderr


def test_section_order_is_judged_by_the_versions_schema(cache, tmp_path, capsys):
    shutil.copytree(CONTRACT, cache / CONTRACT_VERSION)
    sections = {s: [(k, r) for k, r in fixture_sections()[s]] for s in ("gene", "allele")}  # allele comes first
    text = bundle_bytes(sections, CONTRACT_VERSION)
    bad = write_gz(tmp_path / "in" / "b.json.gz", text)
    rc, _, stderr = cli(capsys, "build", "--full", bad, "--out", tmp_path / "out")
    assert rc == 2 and "section allele comes after gene" in stderr and "the bundle schema's order" in stderr


def test_fixed_specs_keep_their_list_and_look_at_no_schema_set(tmp_path, capsys, monkeypatch):
    def no_load(*a, **k):
        raise AssertionError("a fixed spec must refuse from its own list")

    monkeypatch.setattr("clinvar_gkm_parquet.schemas.fetch_set", no_load)
    bad = write_gz(tmp_path / "in" / "b.json.gz", bundle_bytes({"gene": fixture_sections()["gene"]}, "8.8.8"))
    for name in ("clinvar-gkm", "baseline"):
        rc, _, stderr = cli(capsys, "build", "--full", bad, "--spec", name, "--out", tmp_path / name)
        assert rc == 2 and "bundle schema_version 8.8.8 is not one table spec 1.0 reads (1.1.0)" in stderr


def test_a_custom_spec_is_checked_against_the_schema_sets_it_lists(cache, tmp_path, capsys):
    shutil.copytree(CONTRACT, cache / CONTRACT_VERSION)
    raw = yaml.safe_load((FIXTURES / "custom-spec" / "tables.yaml").read_text())
    raw["bundle_schema_versions"] = [CONTRACT_VERSION]
    path = tmp_path / "custom.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    rc, _, stderr = cli(capsys, "build", "--full", FIXTURE, "--spec", path, "--out", tmp_path / "out")
    assert rc == 2 and f"section(s) ['scv'] are not sections of bundle schema {CONTRACT_VERSION}" in stderr


def test_a_custom_spec_listing_a_version_that_cant_load_is_refused(tmp_path, capsys):
    raw = yaml.safe_load((FIXTURES / "custom-spec" / "tables.yaml").read_text())
    raw["bundle_schema_versions"] = ["8.8.8"]
    path = tmp_path / "custom.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    rc, _, stderr = cli(capsys, "build", "--full", FIXTURE, "--spec", path, "--out", tmp_path / "out")
    assert rc == 2 and "bundle_schema_versions 8.8.8: can't read its sections" in stderr and "HTTP 404" in stderr


def test_an_unknown_version_whose_schemas_cant_be_fetched_is_refused(tmp_path, capsys):
    bad = write_gz(tmp_path / "in" / "b.json.gz", bundle_bytes({"gene": fixture_sections()["gene"]}, "8.8.8"))
    rc, _, stderr = cli(capsys, "build", "--full", bad, "--out", tmp_path / "out")
    assert rc == 2
    assert "bundle schema_version '8.8.8': table spec sections can't read it" in stderr
    assert "clinvar-gkm/8.8.8/clinvar-gkm-bundle.schema.json" in stderr and "HTTP 404" in stderr
    assert not (tmp_path / "out").exists()


def test_a_version_whose_schemas_load_but_miss_the_contract_stops_at_compile(cache, tmp_path, capsys):
    V_DIR = cache / V
    write_set(make_set({"gene": closed(id=STRING, x={"type": "object"})}), V_DIR)
    bad = write_gz(tmp_path / "in" / "b.json.gz", bundle_bytes({"gene": fixture_sections()["gene"]}, V))
    rc, _, stderr = cli(capsys, "build", "--full", bad, "--out", tmp_path / "out")
    assert rc == 2
    assert f"bundle schema_version '{V}': table spec sections can't read it" in stderr
    assert "section gene: position x: is an open object" in stderr
    assert "the schema must define it fully" in stderr


def test_compile_spec_writes_what_a_build_uses(capsys):
    rc, stdout, _ = cli(capsys, "compile-spec", "1.1.0")
    assert rc == 0
    raw = yaml.safe_load(stdout)
    assert spec_sha256(raw) == SOURCE.sha256 and parse_spec(raw, require_name=True) == COMPILED
    assert f"# schema_set_sha256: {SCHEMAS.sha256}" in stdout and f"# table_spec_sha256: {SOURCE.sha256}" in stdout


def test_compile_spec_refuses_what_a_build_refuses(capsys):
    rc, stdout, stderr = cli(capsys, "compile-spec", "8.8.8")
    assert rc == 2 and not stdout and "table spec sections can't read it" in stderr
    rc, _, stderr = cli(capsys, "compile-spec", "../1.1.0")
    assert rc == 2 and "is not a version" in stderr


def test_build_api_with_no_spec_compiles_for_the_bundle(tmp_path):
    result = build_mod.build(_full(FIXTURE), tmp_path / "out")
    assert len(result.tables) == len(COMPILED.tables)
    kv = verify.read_kv(duckdb.connect(), tmp_path / "out" / "gene.parquet")
    assert (kv["table_spec"], kv["table_spec_version"]) == ("sections", "1.0") and "table_spec_builtin" not in kv


def test_build_api_labels_a_compiled_spec_passed_without_its_source(tmp_path):
    build_mod.build(_full(FIXTURE), tmp_path / "out", spec=COMPILED)
    kv = verify.read_kv(duckdb.connect(), tmp_path / "out" / "gene.parquet")
    assert "table_spec_builtin" not in kv and kv["table_spec_sha256"] == SOURCE.sha256


def test_the_version_comes_from_the_first_lines_of_a_url_with_one_get_and_no_range(monkeypatch):
    raw = FIXTURE.read_bytes()
    requests = []

    class Response(io.BytesIO):
        status = 200
        headers = {"Content-Length": str(len(raw))}

    def urlopen(req, **kwargs):
        requests.append(dict(req.headers))
        return Response(raw)

    monkeypatch.setattr(sources, "_urlopen", urlopen)
    inp = InputFile("https://example.org/x.json.gz", "x.json.gz", None, None, None, None)
    assert peek_schema_version(inp) == "1.1.0"
    assert len(requests) == 1 and "Range" not in requests[0]


@pytest.mark.parametrize(
    "data, error, message",
    [
        (gzip.compress(b""), "BundleFormatError", "empty file"),
        (gzip.compress(b"[\n"), "BundleFormatError", "expected '{' on line 1"),
        (gzip.compress(b'{\n  "x": 1\n'), "SchemaVersionError", "line 2 must be"),
        (b"not gzip", "BundleFormatError", "not a complete gzip stream"),
    ],
)
def test_a_file_that_doesnt_start_as_a_bundle_stops_the_peek(tmp_path, data, error, message):
    path = tmp_path / "b.json.gz"
    path.write_bytes(data)
    with pytest.raises(ToolError, match=message) as e:
        peek_schema_version(local_input(path))
    assert type(e.value).__name__ == error


def test_a_wrong_checksum_is_the_error_a_peek_reports_first(tmp_path):
    path = tmp_path / "b.json.gz"
    path.write_bytes(gzip.compress(b"[\n"))
    with pytest.raises(InputError, match="sha256 mismatch"):
        peek_schema_version(local_input(path, sha="0" * 64))


# --- provenance (ADR 0006 §7) ------------------------------------------------------------------------------------


def _rewrite_kv(path: Path, key: str, value: str | None) -> None:
    """A tampered file: its metadata entry `clinvar_gkm_parquet.<key>` changed (None: removed)."""
    con = duckdb.connect()
    rows = con.execute(f"SELECT key, value FROM parquet_kv_metadata('{path}')").fetchall()
    pairs = {k.decode(): v.decode() for k, v in rows if k.decode().startswith(build_mod.KV_PREFIX)}
    if value is None:
        del pairs[build_mod.KV_PREFIX + key]
    else:
        pairs[build_mod.KV_PREFIX + key] = value
    meta = ", ".join(f"'{k}': '{v.replace(chr(39), chr(39) * 2)}'" for k, v in pairs.items())
    tmp = path.with_name(path.name + ".new")
    con.execute(f"COPY (SELECT * FROM read_parquet('{path}')) TO '{tmp}' (FORMAT parquet, KV_METADATA {{{meta}}})")
    con.close()
    tmp.replace(path)


def _tamper_all(directory: Path, key: str, value: str | None) -> None:
    for p in directory.glob("*.parquet"):
        _rewrite_kv(p, key, value)


@pytest.fixture
def built_copy(built, tmp_path) -> Path:
    d = tmp_path / "copy"
    shutil.copytree(built[0], d)
    return d


def test_files_and_manifest_carry_both_hashes(built):
    for p in built[0].glob("*.parquet"):
        kv = verify.read_kv(duckdb.connect(), p)
        assert kv["table_spec_sha256"] == SOURCE.sha256 and kv["schema_set_sha256"] == SCHEMAS.sha256
        assert kv["bundle_schema_version"] == "1.1.0" and "table_spec_builtin" not in kv
    m = json.loads((built[0] / "manifest.json").read_text())
    assert m["table_spec"] == {
        "name": "sections",
        "version": "1.0",
        "builtin": True,
        "sha256": SOURCE.sha256,
        "schema_set_sha256": SCHEMAS.sha256,
    }


@pytest.mark.parametrize("name", ["clinvar-gkm", "baseline"])
def test_a_fixed_spec_records_no_compiled_hash(tmp_path, capsys, name):
    out = tmp_path / "out"
    assert cli(capsys, "build", "--full", FIXTURE, "--spec", name, "--tables", "gene", "--out", out)[0] == 0
    kv = verify.read_kv(duckdb.connect(), out / "gene.parquet")
    assert not {"table_spec_sha256", "schema_set_sha256", "table_spec_builtin"} & set(kv)
    assert "schema_set_sha256" not in json.loads((out / "manifest.json").read_text())["table_spec"]


@pytest.mark.parametrize(
    "key, message",
    [
        ("schema_set_sha256", "the schema set of bundle schema 1.1.0 differs: the files (written by tool "),
        ("table_spec_sha256", "the compiled spec of bundle schema 1.1.0 differs, from the same schema set"),
    ],
)
@pytest.mark.parametrize("command", ["verify", "verify --roundtrip", "rebuild"])
def test_a_recorded_hash_that_doesnt_hold_is_refused(built_copy, tmp_path, capsys, key, message, command):
    _tamper_all(built_copy, key, "0" * 64)
    args = {
        "verify": ["verify", built_copy],
        "verify --roundtrip": ["verify", built_copy, "--roundtrip"],
        "rebuild": ["rebuild", built_copy, "-o", tmp_path / "r.json"],
    }[command]
    rc, _, stderr = cli(capsys, *args)
    assert rc == 2 and message in stderr and "0" * 64 in stderr, stderr
    assert not (tmp_path / "r.json").exists()


def test_a_set_with_no_recorded_hash_is_refused(built_copy, capsys):
    _tamper_all(built_copy, "schema_set_sha256", None)
    rc, _, stderr = cli(capsys, "verify", built_copy)
    assert rc == 2 and "don't record bundle_schema_version, table_spec_sha256 and schema_set_sha256" in stderr


def test_a_set_of_other_rules_is_refused_naming_the_tool(built_copy, capsys):
    _tamper_all(built_copy, "table_spec_version", "9.9")
    _tamper_all(built_copy, "tool_version", "9.9.9")
    rc, _, stderr = cli(capsys, "rebuild", built_copy, "-o", built_copy / "r.json")
    assert rc == 2 and "written by sections 9.9 (tool 9.9.9); this tool compiles sections 1.0" in stderr


def test_a_manifest_that_disagrees_with_the_files_is_found(built_copy, tmp_path, capsys):
    m = json.loads((built_copy / "manifest.json").read_text())
    m["table_spec"]["schema_set_sha256"] = "0" * 64
    (built_copy / "manifest.json").write_text(json.dumps(m))
    rc, stdout, _ = cli(capsys, "verify", built_copy)
    assert rc == 1 and "table_spec.schema_set_sha256" in stdout
    rc, _, stderr = cli(capsys, "rebuild", built_copy, "-o", tmp_path / "r.json")
    assert rc == 2 and "table_spec.schema_set_sha256" in stderr


def test_verify_accepts_the_built_in_name_for_a_compiled_set(built, capsys):
    assert cli(capsys, "verify", built[0], "--spec", "sections")[0] == 0
    rc, _, stderr = cli(capsys, "verify", built[0], "--spec", "baseline")
    assert rc == 2 and "--spec baseline is baseline 1.0, but the files were written by sections 1.0" in stderr


def test_resolving_the_compiled_spec_needs_a_version():
    with pytest.raises(SpecError, match="compiled for a bundle's schema_version"):
        resolve_spec("sections")
    with pytest.raises(SpecError, match="no built-in table spec sections 9.9"):
        resolve_spec("sections@9.9", "1.1.0")
    with pytest.raises(SpecError, match="has no file"):
        load_spec("1.0", "sections")
