"""Fail-loud checks: what the spec can't map stops the build with an error naming table, column and key.

Each test takes one real fixture record and changes it minimally, as the
test says; nothing here is a hand-written record.
"""

from __future__ import annotations

import copy
import dataclasses
import json

import duckdb
import pytest
from _tables_helpers import SPEC, build, connect, read_fixture

from clinvar_gkm_parquet.errors import SpecError
from clinvar_gkm_parquet.sqltemplate import render

BASE = read_fixture()


def _mutated(section, pick, change, rekey=None):
    """A copy of the fixture with the first record of `section` for which pick(key, rec) holds changed in place
    by change(rec) (and its key replaced by rekey(key), if given). Returns (sections, key)."""
    sections = copy.deepcopy(BASE)
    for i, (k, text) in enumerate(sections[section]):
        rec = json.loads(text)
        if pick(k, rec):
            change(rec)
            new_key = rekey(k) if rekey else k
            sections[section][i] = (new_key, json.dumps(rec, separators=(",", ":")))
            return sections, new_key
    raise AssertionError(f"no {section} record to mutate")


def _fails(sections, table, *needles):
    """The build stops with a message holding every needle, and the slow reference SQL (render(fast=False)) stops
    with the same message."""
    msgs = []
    for fast in (True, False):
        con = connect(sections)
        with pytest.raises(duckdb.InvalidInputException) as exc:
            build(con, SPEC.table(table), fast=fast)
        msgs.append(str(exc.value))
    msg = msgs[0]
    for n in needles:
        assert n in msg, msg
    assert msgs[1] == msg
    return msg


def _ext_named(name):
    return lambda k, rec: any(e.get("name") == name for e in rec.get("extensions", []))


def _rename_ext(old, new):
    def change(rec):
        for e in rec["extensions"]:
            if e["name"] == old:
                e["name"] = new

    return change


def test_unknown_extension_name_top_level():
    # scv: rename the clinvarScvId extension to a name the slot map doesn't list.
    sections, key = _mutated("scv", _ext_named("clinvarScvId"), _rename_ext("clinvarScvId", "clinvarScvIdRenamed"))
    _fails(sections, "scv", "table scv: column extensions", f"key {key}", "unknown extension name", "Renamed")


def test_unknown_extension_name_nested():
    # scv.classification.extensions: rename the `description` extension.
    def pick(k, rec):
        return any(e["name"] == "description" for e in rec.get("classification", {}).get("extensions", []))

    def change(rec):
        _rename_ext("description", "descriptionRenamed")(rec["classification"])

    sections, key = _mutated("scv", pick, change)
    _fails(
        sections,
        "scv",
        "table scv: column classification",
        f"key {key}",
        "unknown extension name at .extensions[]",
    )


def test_extension_name_from_another_columns_map():
    # Slot maps are per column: clinvarGeneList is in variation.extensions' map, not evidenceLine.extensions'.
    # Rename an evidenceLine extension to it.
    sections, key = _mutated(
        "evidenceLine",
        lambda k, rec: k.startswith("clinvar.submission:") and _ext_named("submittedCondition")(k, rec),
        _rename_ext("submittedCondition", "clinvarGeneList"),
    )
    _fails(
        sections, "evidenceLine", "table evidenceLine: column extensions", f"key {key}", "unknown extension name"
    )


@pytest.mark.parametrize("table", ["evidenceLine", "vcv_evidenceLine", "rcv_evidenceLine"])
def test_evidence_line_id_of_no_known_shape(table):
    # evidenceLine: re-key one VCV evidence line as XCV..., a shape none of the three tables takes.
    sections, key = _mutated(
        "evidenceLine", lambda k, rec: k.startswith("VCV"), lambda rec: None, rekey=lambda k: "X" + k[1:]
    )
    _fails(sections, table, f"table {table}: row filter", f"key {key}", "evidence-line id of no known shape")


def test_non_pointer_proposition_object():
    # An SCV proposition whose #/condition/ object pointer is replaced by its bare id.
    def pick(k, rec):
        return k.startswith("SCV") and isinstance(rec.get("object"), str)

    def change(rec):
        rec["object"] = rec["object"].split("/", 2)[2]

    sections, key = _mutated("propositionPathogenicity", pick, change)
    _fails(
        sections,
        "propositionPathogenicity",
        "table propositionPathogenicity: column object_",
        f"key {key}",
        "not a #/<section>/<id> pointer",
    )


def test_inline_object_where_a_pointer_is_required():
    # The same, with the pointer replaced by an inline object (the examples' inlined form).
    def change(rec):
        rec["object"] = {"id": rec["object"].split("/", 2)[2], "type": "MappableConcept"}

    sections, key = _mutated("propositionPathogenicity", lambda k, rec: k.startswith("SCV"), change)
    _fails(sections, "propositionPathogenicity", f"key {key}", "not a #/<section>/<id> pointer")


def test_non_pointer_inside_a_typed_copy():
    # scv.contributions[].contributor: replace the #/submitter/ pointer with its bare id.
    def change(rec):
        c = rec["contributions"][0]
        c["contributor"] = c["contributor"].split("/", 2)[2]

    sections, key = _mutated("scv", lambda k, rec: "contributions" in rec, change)
    _fails(sections, "scv", "table scv: column contributions", f"key {key}", "pointer at [].contributor")


@pytest.mark.parametrize(
    ("where", "table", "column", "pos"),
    [
        ("quality", "scv", "quality", "(column)"),  # a struct column
        ("contributions", "scv", "contributions", "[]"),  # a list element
        ("extensions.value", "scv", "extensions", "[].value_submitted_condition"),  # an extension value
        ("hgvs", "variation", "extensions", "[].value_hgvs_array[]"),  # deep inside an extension value
    ],
)
def test_field_the_spec_type_lacks_stops_the_build(where, table, column, pos):
    """A bundle field the spec type doesn't have must not be dropped: add one field to one real object."""

    def change(rec):
        if where == "quality":
            rec["quality"]["addedField"] = "x"
        elif where == "contributions":
            rec["contributions"][0]["addedField"] = "x"
        elif where == "extensions.value":
            next(e for e in rec["extensions"] if e["name"] == "submittedCondition")["value"]["addedField"] = "x"
        else:
            next(e for e in rec["extensions"] if e["name"] == "clinvarHgvsList")["value"][0]["addedField"] = "x"

    picks = {
        "quality": lambda k, rec: "quality" in rec,
        "contributions": lambda k, rec: "contributions" in rec,
        "extensions.value": _ext_named("submittedCondition"),
        "hgvs": _ext_named("clinvarHgvsList"),
    }
    sections, key = _mutated(table, picks[where], change)
    _fails(
        sections,
        table,
        f"table {table}: column {column}",
        f"key {key}",
        f"not in the spec type at {pos}",
        "addedField",
    )


def test_top_level_field_no_column_covers():
    # scv: a new top-level field (as BigQuery's SELECT t.* would export as a new column).
    sections, key = _mutated("scv", lambda k, rec: True, lambda rec: rec.__setitem__("addedField", "x"))
    _fails(sections, "scv", "table scv: record", f"key {key}", "addedField")


def test_object_where_a_scalar_is_expected():
    # submitter.name replaced by an object: JSON_VALUE would give NULL; the tool stops.
    sections, key = _mutated(
        "submitter", lambda k, rec: "name" in rec, lambda rec: rec.__setitem__("name", {"x": 1})
    )
    _fails(sections, "submitter", "table submitter: column name", f"key {key}", "expected a scalar")


def test_duckdb_json_casts_alone_would_not_do():
    """Why typed copy walks the type itself (DuckDB 1.5.4): json_transform drops unknown keys silently, and
    CAST(JSON AS STRUCT) rejects absent keys, which the bundle omits all the time (strip_empty)."""
    con = duckdb.connect()
    assert con.execute("""SELECT json_transform('{"a":1,"b":2}', '{"a":"INTEGER"}')""").fetchone()[0] == {"a": 1}
    with pytest.raises(duckdb.ConversionException, match="does not have key"):
        con.execute("""SELECT CAST('{"a":1}'::JSON AS STRUCT(a INT, b INT))""")


def test_slot_map_name_for_a_missing_slot_is_a_spec_error():
    t = SPEC.table("evidenceLine")
    bad = dataclasses.replace(
        t, extension_slots={"extensions": {**t.extension_slots["extensions"], "x": "value_nope"}}
    )
    with pytest.raises(SpecError, match="slot"):
        render(bad)


def test_extension_struct_without_slot_map_is_a_spec_error():
    t = SPEC.table("evidenceLine")
    with pytest.raises(SpecError, match="extension_slots"):
        render(dataclasses.replace(t, extension_slots={}))


def test_unknown_gene_context_qualifier_extension_name():
    # propositionOncogenicity (the fixture's pathogenicity qualifiers have no extensions): rename the
    # geneContextQualifier's submittedGeneSymbols extension.
    def pick(k, rec):
        exts = rec.get("geneContextQualifier", {}).get("extensions", [])
        return any(e.get("name") == "submittedGeneSymbols" for e in exts)

    def change(rec):
        for e in rec["geneContextQualifier"]["extensions"]:
            if e["name"] == "submittedGeneSymbols":
                e["name"] = "submittedGeneSymbolsRenamed"

    sections, key = _mutated("propositionOncogenicity", pick, change)
    _fails(
        sections,
        "propositionOncogenicity",
        "column gene_context_source",
        f"key {key}",
        "unknown geneContextQualifier extension name",
        "submittedGeneSymbolsRenamed",
    )
