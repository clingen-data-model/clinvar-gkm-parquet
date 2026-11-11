"""Negative cases for `validate`'s engine equivalence, generated from real records and the schema set, not listed
by hand.

Every case is a real record of the excerpts (tests/fixtures/real-1.1.0 and real-1.1.0-1031) with one change,
wrapped as the one-section mini-bundle `validate` checks. `kind` names the change and `path` the JSON pointer it
was made at, relative to the record ("" for the record itself, or for its key). A case says nothing about the
expected verdict: the tests require both engines to give the same one, and require a rejection where the schema
set clearly forbids the change (`must_reject`).
"""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Iterator
from dataclasses import dataclass

from _fixture import FIXTURE, FIXTURES
from _input_helpers import fixture_sections

from clinvar_gkm_parquet.bundle import decode_key
from clinvar_gkm_parquet.schemas import SchemaSet

VERSION = "1.1.0"
UNDECLARED = "notInTheSchema"
_POINTER = re.compile(r"^#/([A-Za-z][A-Za-z0-9]*)/(.+)$")


@dataclass
class Case:
    kind: str
    section: str
    key: str
    path: str
    doc: dict
    must_reject: bool = False

    @property
    def name(self) -> str:
        return f"{self.kind}:{self.section}/{self.key}{self.path}"


# The suite's fixture (a 2026-10-18 excerpt), then a 2026-10-31 excerpt seeded with what the first lacks: the
# propositionClinvarCustom section (one record of each type), a penetranceQualifier and a prognostic proposition.
EXCERPTS = (FIXTURE, FIXTURES / "real-1.1.0-1031" / "clinvar-gkm_excerpt.json.gz")


def records() -> list[tuple[str, str, dict]]:
    """(section, key, record) for every record of the excerpts, in their order."""
    return [
        (section, decode_key(k.decode()), json.loads(r))
        for path in EXCERPTS
        for section, items in fixture_sections(path).items()
        for k, r in items
    ]


def first_per_section() -> list[tuple[str, str, dict]]:
    seen, out = set(), []
    for section, key, rec in records():
        if section not in seen:
            seen.add(section)
            out.append((section, key, rec))
    return out


def mini(section: str, key: str, record) -> dict:
    return {"schema_version": VERSION, section: {key: record}}


def _ptr(parts) -> str:
    return "".join("/" + str(p).replace("~", "~0").replace("/", "~1") for p in parts)


def nodes(value, parts=()) -> Iterator[tuple[tuple, object]]:
    """Every value in a record with its path, parents first."""
    yield parts, value
    if isinstance(value, dict):
        for k, v in value.items():
            yield from nodes(v, (*parts, k))
    elif isinstance(value, list):
        for i, v in enumerate(value):
            yield from nodes(v, (*parts, i))


def _set(record, parts, value):
    out = copy.deepcopy(record)
    if not parts:
        return value
    node = out
    for p in parts[:-1]:
        node = node[p]
    node[parts[-1]] = value
    return out


def _delete(record, parts):
    out = copy.deepcopy(record)
    node = out
    for p in parts[:-1]:
        node = node[p]
    del node[parts[-1]]
    return out


def _case(kind, section, key, parts, record, must_reject=False) -> Case:
    return Case(kind, section, key, _ptr(parts), mini(section, key, record), must_reject)


# --- cases from the schema: every property a class narrows to `false` --------------------------------------------


def banned_properties(schema_set: SchemaSet, files: str = "/clinvar-gkm/") -> dict[str, list[str]]:
    """`<$id>#<pointer>/properties` -> the property names that schema object declares `false`, for every schema
    object in the set's files whose `$id` contains `files`."""
    out: dict[str, list[str]] = {}

    def walk(node, ptr: str, sid: str) -> None:
        if isinstance(node, dict):
            props = node.get("properties")
            if isinstance(props, dict):
                banned = [k for k, v in props.items() if v is False]
                if banned:
                    out[f"{sid}#{ptr}/properties"] = banned
            for k, v in node.items():
                if k not in ("const", "enum", "default", "examples"):
                    walk(v, f"{ptr}/{k}", sid)
        elif isinstance(node, list):
            for i, v in enumerate(node):
                walk(v, f"{ptr}/{i}", sid)

    for sid, f in schema_set.files.items():
        if files in sid:
            walk(f.doc, "", sid)
    return out


def ban_cases(schema_set: SchemaSet, evaluate) -> tuple[list[Case], set[str]]:
    """For each schema object that bans a property, each record node it applies to gets that property. Where it
    applies comes from the engine's annotations: the `properties` keyword of a schema object annotates each node
    the object validated. `evaluate(doc)` returns those annotations. Returns the cases and the schema objects no
    fixture record reaches."""
    banned = banned_properties(schema_set)
    cases, reached, done = [], set(), set()
    for section, key, rec in records():
        doc = mini(section, key, rec)
        for ann in evaluate(doc):
            where = ann.get("absoluteKeywordLocation") or ""
            if where not in banned:
                continue
            reached.add(where)
            parts = [p.replace("~1", "/").replace("~0", "~") for p in ann["instanceLocation"].split("/")[3:]]
            parts = tuple(
                int(p) if p.isdigit() and _is_list_step(rec, parts, i) else p for i, p in enumerate(parts)
            )
            for prop in banned[where]:
                if (where, prop) in done:
                    continue
                done.add((where, prop))
                node = _get(rec, parts)
                if not isinstance(node, dict):
                    continue
                cases.append(
                    _case("banned-property", section, key, (*parts, prop), _set(rec, (*parts, prop), "x"), True)
                )
    return cases, set(banned) - reached


def _get(record, parts):
    node = record
    for p in parts:
        node = node[p]
    return node


def _is_list_step(record, parts, i) -> bool:
    return isinstance(_get(record, [int(p) if p.isdigit() else p for p in parts[:i]]), list)


# --- cases from the records --------------------------------------------------------------------------------------


def record_cases(sections: list[str]) -> list[Case]:
    """For the first record of each section: every kind of change at every place it applies."""
    by_key = {(s, k): r for s, k, r in records()}
    out: list[Case] = []
    for section, key, rec in first_per_section():
        out.append(Case("key-breaks-pattern", section, "not a key", "", mini(section, "not a key", rec), True))
        out.append(_case("undeclared-top-level", section, key, (UNDECLARED,), {**rec, UNDECLARED: 1}, True))
        for parts, value in nodes(rec):
            if isinstance(value, dict):
                if parts:
                    out.append(
                        _case(
                            "undeclared-nested",
                            section,
                            key,
                            (*parts, UNDECLARED),
                            _set(rec, (*parts, UNDECLARED), 1),
                        )
                    )
                    out.append(_case("empty-object", section, key, parts, _set(rec, parts, {})))
                for k in value:
                    out.append(_case("missing-property", section, key, (*parts, k), _delete(rec, (*parts, k))))
            elif isinstance(value, list):
                out.append(_case("empty-array", section, key, parts, _set(rec, parts, [])))
            elif isinstance(value, str):
                out.append(_case("string-changed", section, key, parts, _set(rec, parts, value + "_x")))
                out.append(_case("string-to-integer", section, key, parts, _set(rec, parts, 1)))
                m = _POINTER.match(value)
                if m and m.group(1) in sections:
                    other = next(s for s in sections if s != m.group(1))
                    out.append(
                        _case(
                            "pointer-wrong-section",
                            section,
                            key,
                            parts,
                            _set(rec, parts, f"#/{other}/{m.group(2)}"),
                        )
                    )
                    target = by_key.get((m.group(1), m.group(2)))
                    if target is not None:
                        out.append(_case("pointer-inlined", section, key, parts, _set(rec, parts, target)))
            elif isinstance(value, bool):
                out.append(_case("boolean-to-string", section, key, parts, _set(rec, parts, "true")))
            elif isinstance(value, int):
                out.append(_case("integer-to-float", section, key, parts, _set(rec, parts, float(value))))
                out.append(_case("integer-to-fraction", section, key, parts, _set(rec, parts, value + 0.5)))
                out.append(_case("integer-to-string", section, key, parts, _set(rec, parts, str(value))))
            elif isinstance(value, float):
                out.append(_case("number-to-string", section, key, parts, _set(rec, parts, str(value))))
        for parts, value in nodes(rec):
            if parts and parts[-1] == "extensions" and isinstance(value, list):
                out.append(
                    _case(
                        "extension-unknown-name",
                        section,
                        key,
                        (*parts, len(value)),
                        _set(rec, parts, [*value, {"name": "notAnExtension", "value": "x"}]),
                    )
                )
                for i, ext in enumerate(value):
                    if isinstance(ext, dict) and "value" in ext:
                        wrong = 12345 if isinstance(ext["value"], str) else "x"
                        out.append(
                            _case(
                                "extension-value-type",
                                section,
                                key,
                                (*parts, i, "value"),
                                _set(rec, (*parts, i, "value"), wrong),
                            )
                        )
    return out


def list_item_cases(section: str, extension: str) -> list[Case]:
    """An undeclared property in the first item of each `<extension>` extension's array value, in every record of
    `section` that has one."""
    out = []
    for sec, key, rec in records():
        if sec != section:
            continue
        for i, ext in enumerate(rec.get("extensions", [])):
            if ext.get("name") == extension and isinstance(ext.get("value"), list) and ext["value"]:
                parts = ("extensions", i, "value", 0, UNDECLARED)
                out.append(_case("undeclared-list-item", section, key, parts, _set(rec, parts, 1), True))
    return out
