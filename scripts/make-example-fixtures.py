#!/usr/bin/env python3
"""Build the PROVISIONAL examples-1.1.0 test fixture from ClinVar-GKM's examples/.

Real 1.1.0 bundles are not published yet, so this script makes a stand-in: a small
mini-bundle in the exact ClinVar-GKM 1.1.0 bundle line format, built from the example
records in the ClinVar-GKM repo's examples/ folder. Replace it with a real bundle excerpt
once the 2026-10-18 1.1.0 bundle is staged.

The examples were made by src/scripts/extract-example.py, which takes one bundle record
and resolves its `#/<section>/<key>` pointers inline. This script reverses that: wherever
an inlined object stands where a pointer was, it emits the object as its own record in
the right section and puts the pointer back. Pointer sites, sections and keys follow the
ClinVar-GKM 1.0 branch code (see SITES and classify() below). Anything it cannot place
exactly stops the build with an error.

Inputs are read from a local ClinVar-GKM clone with `git show` / `git ls-tree` at a fixed
commit, so the working tree is never touched. No network.

Usage:
    scripts/make-example-fixtures.py [--clinvar-gks PATH] [--ref COMMIT]   # write
    scripts/make-example-fixtures.py --check [...]                          # verify

--check rebuilds in memory and fails if the files on disk differ.
"""

from __future__ import annotations

import argparse
import copy
import gzip
import hashlib
import io
import json
import re
import subprocess
import sys
from pathlib import Path

DEFAULT_CLINVAR_GKS = Path.home() / "Development/gks/clinvar-gks"
# origin/1.0 at the time the fixture was made: the release line with the 1.1.0 code.
DEFAULT_REF = "25344ba6edb639bacbdd8af38bd90a2e6b9f4240"
SOURCE_REPOSITORY = "https://github.com/clingen-data-model/clinvar-gkm"

SCHEMA_VERSION = "1.1.0"
BUNDLE_SCHEMA_PATH = "schema/clinvar-gkm/clinvar-gkm-bundle.schema.json"
EXTRACTOR_PATH = "src/scripts/extract-example.py"
ASSEMBLER_PATH = "src/scripts/assemble-gkm-dicts.py"

OUT_DIR = Path(__file__).resolve().parents[1] / "tests/fixtures/examples-1.1.0"
BUNDLE_NAME = "clinvar-gkm_examples.json.gz"
GZIP_LEVEL = 3  # assembler GZIP_COMPRESS_LEVEL

PROVISIONAL_NOTE = (
    "de-inlined from ClinVar-GKM examples/, not published bundle bytes; record key order and "
    "number formatting may differ from a real bundle; replace with a real excerpt after the "
    "2026-10-18 1.1.0 bundle is staged"
)

# Section output order: assemble-gkm-dicts.py SECTIONS (checked against the bundle schema).
SECTIONS = [
    "sequenceReference",
    "location",
    "allele",
    "copyNumberCount",
    "copyNumberChange",
    "gene",
    "variation",
    "condition",
    "conditionSet",
    "therapy",
    "therapyGroup",
    "submitter",
    "propositionPathogenicity",
    "propositionOncogenicity",
    "propositionSomaticClinicalImpact",
    "propositionClinvarCustom",
    "evidenceLine",
    "scv",
    "vcv",
    "rcv",
]

# examples/<dir>/ -> section of the top-level record in each file.
EXAMPLE_DIRS = {"cat-vrs": "variation", "scv": "scv", "vcv": "vcv", "rcv": "rcv"}

# src/procedures/gkm-proposition-section-func.sql (gkm_proposition_section).
PROPOSITION_SECTION = {
    "VariantPathogenicityProposition": "propositionPathogenicity",
    "VariantOncogenicityProposition": "propositionOncogenicity",
    **dict.fromkeys(
        [
            "VariantClinicalSignificanceProposition",
            "VariantDiagnosticProposition",
            "VariantPrognosticProposition",
            "VariantTherapeuticResponseProposition",
        ],
        "propositionSomaticClinicalImpact",
    ),
    **dict.fromkeys(
        [
            "ClinvarNotProvidedProposition",
            "ClinvarDrugResponseProposition",
            "ClinvarOtherProposition",
            "ClinvarRiskFactorProposition",
            "ClinvarAssociationProposition",
            "ClinvarAffectsProposition",
            "ClinvarConflictingDataFromSubmitterProposition",
            "ClinvarProtectiveProposition",
            "ClinvarConfersSensitivityProposition",
            "ClinvarUndefinedProposition",
        ],
        "propositionClinvarCustom",
    ),
}
PROPOSITION_SECTIONS = frozenset(PROPOSITION_SECTION.values())

POINTER_RE = re.compile(r"^#/(?P<section>[^/]+)/(?P<key>.+)$")
CONDITION_SET_ID_RE = re.compile(r"^clinvar\.(trait[sS]et|conditionset):.+")
# Drug names whose BigQuery TO_JSON_STRING encoding is known to equal json.dumps.
SAFE_DRUG_NAME_RE = re.compile(r"^[A-Za-z0-9 ._,()+/-]+$")


class FixtureError(Exception):
    pass


# ---------------------------------------------------------------------------
# Reading the ClinVar-GKM repo (read-only, through git objects)
# ---------------------------------------------------------------------------


def git(repo: Path, *args: str) -> bytes:
    proc = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, check=False)
    if proc.returncode != 0:
        raise FixtureError(f"git {' '.join(args)} failed: {proc.stderr.decode().strip()}")
    return proc.stdout


def blob_sha(repo: Path, ref: str, path: str) -> str:
    out = git(repo, "ls-tree", ref, "--", path).decode().split()
    if len(out) != 4 or out[1] != "blob":
        raise FixtureError(f"{path} is not a file at {ref}")
    return out[2]


def list_examples(repo: Path, ref: str) -> list[tuple[str, str]]:
    """(path, blob sha) for every example record file, sorted by path."""
    found = []
    for line in git(repo, "ls-tree", "-r", ref, "--", "examples").decode().splitlines():
        meta, path = line.split("\t", 1)
        _mode, kind, sha = meta.split()
        parts = path.split("/")
        if kind != "blob" or path == "examples/readme.md":
            continue
        if len(parts) != 3 or parts[1] not in EXAMPLE_DIRS or not re.search(r"\.jsonc?$", path):
            raise FixtureError(f"unexpected file under examples/: {path}")
        found.append((path, sha))
    if not found:
        raise FixtureError(f"no examples found at {ref}")
    return sorted(found)


def examples_dataset(repo: Path, ref: str) -> dict:
    """The release dataset the examples came from, as stated by the commit that last refreshed them."""
    sha = git(repo, "log", "-1", "--format=%H", ref, "--", "examples").decode().strip()
    message = git(repo, "show", "-s", "--format=%B", sha).decode()
    names = sorted(set(re.findall(r"clinvar_\d{4}_\d{2}_\d{2}_v\d+_\d+_\d+", message)))
    if len(names) != 1:
        raise FixtureError(f"cannot read the examples' dataset from commit {sha}: {names}")
    return {
        "dataset": names[0],
        "stated_in": f"message of commit {sha}, the last commit touching examples/ at the source ref",
        "examples_commit": sha,
        "examples_commit_subject": message.splitlines()[0],
    }


# ---------------------------------------------------------------------------
# De-inlining
# ---------------------------------------------------------------------------


def therapy_key(obj: dict, where: str) -> str:
    """clinvar.therapy:<sha256 of TO_JSON_STRING(STRUCT(name, type, conceptType))>.

    gkm-scv-statement-proc.sql Step 5b: the therapy value is
    STRUCT(drug AS name, 'MappableConcept' AS type, 'Drug' AS conceptType), and the key is
    the hex SHA-256 of its BigQuery TO_JSON_STRING (compact, fields in STRUCT order).
    """
    if set(obj) != {"name", "type", "conceptType"}:
        raise FixtureError(f"{where}: therapy object has unexpected fields {sorted(obj)}")
    name = obj["name"]
    if not isinstance(name, str) or not SAFE_DRUG_NAME_RE.match(name):
        raise FixtureError(f"{where}: drug name {name!r} needs BigQuery JSON escaping this script does not model")
    canonical = f'{{"name":{json.dumps(name)},"type":"MappableConcept","conceptType":"Drug"}}'
    return "clinvar.therapy:" + hashlib.sha256(canonical.encode()).hexdigest()


def classify(obj: dict, where: str) -> tuple[str, str] | None:
    """(section, key) of an object that is a bundle record, or None for an embedded object."""
    t, i = obj.get("type"), obj.get("id")
    has_id = isinstance(i, str)
    if t == "SequenceReference":
        return "sequenceReference", obj["refgetAccession"]
    if t == "SequenceLocation" and has_id:
        return "location", i
    if t == "Allele" and has_id:
        return "allele", i
    if t == "CopyNumberCount" and has_id:
        return "copyNumberCount", i
    if t == "CopyNumberChange" and has_id:
        return "copyNumberChange", i
    if t == "CategoricalVariant" and has_id:
        return "variation", i
    if t == "MappableConcept" and has_id and i.startswith("ncbigene:"):
        return "gene", i
    if t == "MappableConcept" and has_id and i.startswith("clinvar.trait:"):
        return "condition", i
    if t == "MappableConcept" and not has_id and obj.get("conceptType") == "Drug":
        return "therapy", therapy_key(obj, where)
    if t == "ConceptSet" and has_id and CONDITION_SET_ID_RE.match(i):
        return "conditionSet", i
    if t == "ConceptSet" and not has_id:
        # A therapyGroup (content-addressed, no id). No example has one; its key is a
        # digest over its pointer-form concepts, which this script does not reproduce.
        raise FixtureError(f"{where}: id-less ConceptSet (therapyGroup?) is not supported")
    if t == "Agent" and has_id and i.startswith("clinvar.submitter:"):
        return "submitter", i
    if isinstance(t, str) and t.endswith("Proposition") and has_id:
        if t not in PROPOSITION_SECTION:
            raise FixtureError(f"{where}: proposition type {t!r} has no bundle section")
        return PROPOSITION_SECTION[t], i
    if t == "EvidenceLine" and has_id:
        return "evidenceLine", i
    if t == "Statement" and has_id:
        if i.startswith("clinvar.submission:SCV"):
            return "scv", i
        if i.startswith("VCV"):
            return "vcv", i
        if i.startswith("RCV"):
            return "rcv", i
        raise FixtureError(f"{where}: Statement id {i!r} matches no statement section")
    return None


# Pointer sites per section: (path, allowed target sections). A path is a tuple of steps;
# "[]" means every element of a list. These are the `#/…` FORMAT sites in the 1.0 branch
# procedures (gkm-catvar, gkm-scv-condition, gkm-scv/vcv/rcv-statement).
_PROPS = PROPOSITION_SECTIONS
SITES: dict[str, list[tuple[tuple[str, ...], frozenset[str]]]] = {
    "sequenceReference": [],
    "location": [(("sequenceReference",), frozenset({"sequenceReference"}))],
    "allele": [(("location",), frozenset({"location"}))],
    "copyNumberCount": [(("location",), frozenset({"location"}))],
    "copyNumberChange": [(("location",), frozenset({"location"}))],
    "gene": [],
    "variation": [
        (("constraints", "[]", "allele"), frozenset({"allele"})),
        (("constraints", "[]", "location"), frozenset({"location"})),
        (("members", "[]"), frozenset({"allele", "copyNumberCount", "copyNumberChange"})),
        (("extensions", "[clinvarGeneList]", "value", "[]", "gene"), frozenset({"gene"})),
    ],
    "condition": [],
    "conditionSet": [(("concepts", "[]"), frozenset({"condition", "conditionSet"}))],
    "therapy": [],
    "therapyGroup": [(("concepts", "[]"), frozenset({"therapy"}))],
    "submitter": [],
    **{
        s: [
            (("subject",), frozenset({"variation"})),
            (("object",), frozenset({"condition", "conditionSet", "therapy", "therapyGroup"})),
            (("conditionQualifier",), frozenset({"condition", "conditionSet"})),
        ]
        for s in PROPOSITION_SECTIONS
    },
    "evidenceLine": [
        (("targetProposition",), _PROPS),
        (("hasEvidenceItems", "[]"), frozenset({"scv", "vcv", "rcv"})),
    ],
    "scv": [
        (("proposition",), _PROPS),
        (("contributions", "[]", "contributor"), frozenset({"submitter"})),
        (("hasEvidenceLines", "[]"), frozenset({"evidenceLine"})),
    ],
    "vcv": [
        (("proposition",), _PROPS),
        (("hasEvidenceLines", "[]"), frozenset({"evidenceLine"})),
    ],
    "rcv": [
        (("proposition",), _PROPS),
        (("hasEvidenceLines", "[]"), frozenset({"evidenceLine"})),
    ],
}


def _slots(node, path: tuple[str, ...]):
    """Yield (container, index) for every value at `path` under node."""
    if not path:
        return
    step, rest = path[0], path[1:]
    if step.startswith("[") and step != "[]":  # extensions entry selected by name
        if not isinstance(node, list):
            return
        name = step[1:-1]
        for item in node:
            if isinstance(item, dict) and item.get("name") == name:
                yield from _slots(item, rest)
        return
    if step == "[]":
        if not isinstance(node, list):
            return
        for idx in range(len(node)):
            if rest:
                yield from _slots(node[idx], rest)
            else:
                yield node, idx
        return
    if not isinstance(node, dict) or step not in node:
        return
    if rest:
        yield from _slots(node[step], rest)
    else:
        yield node, step


class Fixture:
    def __init__(self) -> None:
        self.records: dict[str, dict[str, dict]] = {s: {} for s in SECTIONS}
        self.derived_keys: dict[tuple[str, str], str] = {}

    def add(self, section: str, key: str, record: dict, where: str) -> None:
        have = self.records[section].get(key)
        if have is None:
            self.records[section][key] = record
        elif canonical(have) != canonical(record):
            raise FixtureError(
                f"{where}: {section}/{key} appears with two different contents; the reversal is ambiguous"
            )

    def deinline(self, record: dict, section: str, where: str) -> dict:
        """Return a copy of `record` (a `section` record) with inlined records made pointers."""
        record = copy.deepcopy(record)
        for path, allowed in SITES[section]:
            for container, idx in list(_slots(record, path)):
                value = container[idx]
                here = f"{where} {section}.{'.'.join(path)}"
                if isinstance(value, str):
                    m = POINTER_RE.match(value)
                    if m and m.group("section") not in allowed:
                        raise FixtureError(f"{here}: pointer {value} to an unexpected section")
                    continue  # a kept pointer, or a bare CURIE (iriReference)
                if not isinstance(value, dict):
                    raise FixtureError(f"{here}: expected a pointer or an object, got {value!r}")
                found = classify(value, here)
                if found is None or found[0] not in allowed:
                    raise FixtureError(
                        f"{here}: object at a pointer site is not a {'/'.join(sorted(allowed))} record: {found}"
                    )
                child_section, child_key = found
                if child_section == "therapy":
                    self.derived_keys[found] = "sha256 of BigQuery TO_JSON_STRING(STRUCT(name, type, conceptType))"
                child = self.deinline(value, child_section, where)
                self.add(child_section, child_key, child, where)
                container[idx] = f"#/{child_section}/{child_key}"
        _assert_no_inlined_records(record, section, where, top=True)
        return record


def reinline(records: dict[str, dict[str, dict]], value, trail: tuple[str, ...], nested: bool = False):
    """extract-example.py Extractor.resolve() over the fixture: the inverse of deinline().

    variation/scv pointers are kept; vcv/rcv are unwound and mark what is below them as
    nested; condition/conditionSet pointers are kept once nested; a cycle or a target that
    is not in the fixture keeps the pointer.
    """
    if isinstance(value, str):
        m = POINTER_RE.match(value)
        if not m or value in trail:
            return value
        section = m.group("section")
        if section in ("variation", "scv") or (section in ("condition", "conditionSet") and nested):
            return value
        target = records.get(section, {}).get(m.group("key"))
        if target is None:
            return value
        return reinline(records, target, (*trail, value), nested or section in ("vcv", "rcv"))
    if isinstance(value, dict):
        return {k: reinline(records, v, trail, nested) for k, v in value.items()}
    if isinstance(value, list):
        return [reinline(records, v, trail, nested) for v in value]
    return value


def _assert_no_inlined_records(node, section: str, where: str, top: bool = False) -> None:
    """Fail if a record-shaped object is left anywhere but at a pointer site."""
    if isinstance(node, dict):
        if not top:
            found = classify(node, where)
            if found is not None:
                raise FixtureError(
                    f"{where}: a {found[0]} record is embedded in a {section} record "
                    "outside any known pointer site; the reversal is ambiguous"
                )
        for v in node.values():
            _assert_no_inlined_records(v, section, where)
    elif isinstance(node, list):
        for v in node:
            _assert_no_inlined_records(v, section, where)


# ---------------------------------------------------------------------------
# Serialization (assemble-gkm-dicts.py layout)
# ---------------------------------------------------------------------------


def canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def dumps(obj) -> str:
    """orjson.dumps equivalent for these records: compact, UTF-8, no floats."""
    _assert_no_floats(obj)
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _assert_no_floats(node) -> None:
    if isinstance(node, float):
        raise FixtureError(f"float {node!r}: stdlib and orjson formatting may differ")
    if isinstance(node, dict):
        for v in node.values():
            _assert_no_floats(v)
    elif isinstance(node, list):
        for v in node:
            _assert_no_floats(v)


def render_bundle(records: dict[str, dict[str, dict]]) -> bytes:
    """The assembler's assemble() layout: sections opened lazily, one record per line."""
    buf = ["{\n", f'  "schema_version": "{SCHEMA_VERSION}"']
    for section in SECTIONS:
        entries = records[section]
        if not entries:
            continue
        buf.append(",\n")
        buf.append(f'  "{section}": {{\n')
        buf.append(",\n".join(f"    {dumps(k)}: {dumps(entries[k])}" for k in sorted(entries)))
        buf.append("\n  }")
    buf.append("\n}\n")
    return "".join(buf).encode()


def gzip_bytes(data: bytes) -> bytes:
    out = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=out, compresslevel=GZIP_LEVEL, mtime=0) as gz:
        gz.write(data)
    return out.getvalue()


# ---------------------------------------------------------------------------
# Strict line reader (design plan §4.2): the self-test of the written layout
# ---------------------------------------------------------------------------

_SV_RE = re.compile(r'^  "schema_version": "([^"\\]+)"(,?)$')
_OPEN_RE = re.compile(r'^  "([A-Za-z]+)": \{$')
_RECORD_RE = re.compile(r'^    ("(?:[^"\\]|\\.)*"): (\{.*\})(,?)$')
_CLOSE_RE = re.compile(r"^  \}(,?)$")


def read_bundle_strict(text: str) -> tuple[str, dict[str, dict[str, dict]]]:
    """Parse the bundle line by line; fail on any line that does not classify."""
    if not text.endswith("}\n") or text.endswith("\n\n"):
        raise FixtureError("bundle must end with '}' and one newline")
    lines = text[:-1].split("\n")
    if lines[0] != "{" or lines[-1] != "}":
        raise FixtureError("bundle must open with '{' and close with '}' on their own lines")
    m = _SV_RE.match(lines[1])
    if not m:
        raise FixtureError(f"line 2 is not the schema_version line: {lines[1]!r}")
    schema_version, more = m.group(1), m.group(2) == ","
    body = lines[2:-1]
    if more != bool(body):
        raise FixtureError("schema_version comma does not match the sections that follow")
    sections: dict[str, dict[str, dict]] = {}
    order = []
    i = 0
    while i < len(body):
        n = i + 3  # 1-based line number
        m = _OPEN_RE.match(body[i])
        if not m:
            raise FixtureError(f"line {n}: expected a section opening, got {body[i][:80]!r}")
        name = m.group(1)
        if name not in SECTIONS or name in sections:
            raise FixtureError(f"line {n}: unknown or repeated section {name!r}")
        order.append(name)
        entries: dict[str, dict] = {}
        i += 1
        while True:
            if i >= len(body):
                raise FixtureError(f"section {name} is not closed")
            line, n = body[i], i + 3
            c = _CLOSE_RE.match(line)
            if c:
                if not entries:
                    raise FixtureError(f"line {n}: empty section {name}")
                if (c.group(1) == ",") != (i + 1 < len(body)):
                    raise FixtureError(f"line {n}: section close comma is wrong")
                i += 1
                break
            r = _RECORD_RE.match(line)
            if not r:
                raise FixtureError(f"line {n}: not a record line: {line[:80]!r}")
            key = json.loads(r.group(1))
            record = json.loads(r.group(2))
            if not isinstance(key, str) or not isinstance(record, dict):
                raise FixtureError(f"line {n}: key must be a string and record an object")
            if key in entries:
                raise FixtureError(f"line {n}: duplicate key {key!r} in {name}")
            last = _CLOSE_RE.match(body[i + 1]) is not None if i + 1 < len(body) else True
            if (r.group(3) == ",") == last:
                raise FixtureError(f"line {n}: record comma is wrong")
            entries[key] = record
            i += 1
        sections[name] = entries
    if order != [s for s in SECTIONS if s in sections]:
        raise FixtureError(f"sections out of assembler order: {order}")
    return schema_version, sections


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def pointers_in(node, out: set[str]) -> None:
    if isinstance(node, str):
        if POINTER_RE.match(node):
            out.add(node)
    elif isinstance(node, dict):
        for v in node.values():
            pointers_in(v, out)
    elif isinstance(node, list):
        for v in node:
            pointers_in(v, out)


def check_against_schema(records: dict[str, dict[str, dict]], schema: dict) -> None:
    props = schema["properties"]
    if props.get("schema_version", {}).get("const") != SCHEMA_VERSION:
        raise FixtureError("bundle schema const schema_version is not " + SCHEMA_VERSION)
    if [p for p in props if p != "schema_version"] != SECTIONS:
        raise FixtureError("SECTIONS differ from the bundle schema's section list")
    for section, entries in records.items():
        patterns = [re.compile(p) for p in props[section]["patternProperties"]]
        for key in entries:
            if not any(p.search(key) for p in patterns):
                raise FixtureError(f"{section}/{key}: key does not match the bundle schema")


def build(repo: Path, ref: str) -> dict[str, bytes]:
    commit = git(repo, "rev-parse", "--verify", f"{ref}^{{commit}}").decode().strip()
    schema = json.loads(git(repo, "show", f"{commit}:{BUNDLE_SCHEMA_PATH}"))
    fixture = Fixture()
    examples = []
    originals: dict[str, tuple[str, str, dict]] = {}
    for path, sha in list_examples(repo, commit):
        text = git(repo, "cat-file", "blob", sha).decode("utf-8")
        try:
            top = json.loads(text)
        except json.JSONDecodeError as e:
            raise FixtureError(f"{path}: not plain JSON ({e}); comments are not supported") from e
        section = EXAMPLE_DIRS[path.split("/")[1]]
        found = classify(top, path)
        if found is None or found[0] != section:
            raise FixtureError(f"{path}: top-level record is not a {section} record: {found}")
        key = found[1]
        # examples/readme.md naming: scv/{SCV}.{version}-{code}; the others are the record id.
        stem = Path(path).name.rsplit(".", 1)[0]
        scv_id = key.removeprefix("clinvar.submission:")
        if stem != key and not (section == "scv" and stem.startswith(scv_id + "-")):
            raise FixtureError(f"{path}: file name does not match record id {key!r}")
        fixture.add(section, key, fixture.deinline(top, section, path), path)
        examples.append({"path": path, "blob": sha, "section": section, "key": key})
        originals[path] = (section, key, top)

    # Round trip: resolving each example's record from the fixture with the extractor's
    # policy must give back the example exactly (values and key order).
    for path, (section, key, top) in originals.items():
        pointer = f"#/{section}/{key}"
        again = reinline(fixture.records, fixture.records[section][key], (pointer,))
        if json.dumps(again, ensure_ascii=False) != json.dumps(top, ensure_ascii=False):
            raise FixtureError(f"{path}: re-inlining the fixture does not reproduce the example")

    check_against_schema(fixture.records, schema)
    raw = render_bundle(fixture.records)

    # Self-test: strict line reader, whole-file JSON parse, and round trip.
    schema_version, parsed = read_bundle_strict(raw.decode("utf-8"))
    whole = json.loads(raw)
    expected = {s: e for s, e in fixture.records.items() if e}
    if schema_version != SCHEMA_VERSION or parsed != expected:
        raise FixtureError("strict reader round trip differs from the built records")
    if whole != {"schema_version": SCHEMA_VERSION, **expected}:
        raise FixtureError("json.loads of the bundle differs from the built records")

    present = {f"#/{s}/{k}" for s, e in expected.items() for k in e}
    referenced: set[str] = set()
    pointers_in(expected, referenced)
    for p in referenced:
        if POINTER_RE.match(p).group("section") not in SECTIONS:
            raise FixtureError(f"pointer {p} names an unknown section")
    dangling = sorted(referenced - present)

    gz = gzip_bytes(raw)
    digest = hashlib.sha256(gz).hexdigest()
    provenance = {
        "fixture": BUNDLE_NAME,
        "sha256": digest,
        "schema_version": SCHEMA_VERSION,
        "provisional": PROVISIONAL_NOTE,
        "generator": "scripts/make-example-fixtures.py",
        "source": {
            "repository": SOURCE_REPOSITORY,
            "commit": commit,
            "release_line": "1.0",
            **examples_dataset(repo, commit),
            "extractor": {"path": EXTRACTOR_PATH, "blob": blob_sha(repo, commit, EXTRACTOR_PATH)},
            "assembler": {"path": ASSEMBLER_PATH, "blob": blob_sha(repo, commit, ASSEMBLER_PATH)},
            "bundle_schema": {"path": BUNDLE_SCHEMA_PATH, "blob": blob_sha(repo, commit, BUNDLE_SCHEMA_PATH)},
            "examples": examples,
        },
        "layout": (
            "assemble-gkm-dicts.py assemble() line format; sections in its SECTIONS order; keys "
            "sorted within a section (the assembler keeps export order); records serialized with "
            "stdlib json, separators (',', ':'), ensure_ascii=False (orjson-equivalent here: no "
            "floats); record key order as in the examples; gzip level 3, mtime 0, no file name"
        ),
        "counts": {s: len(e) for s, e in expected.items()},
        "derived_keys": [
            {
                "section": s,
                "key": k,
                "derivation": how,
                "note": "recomputed from the record; not checked against a published key",
            }
            for (s, k), how in sorted(fixture.derived_keys.items())
        ],
        "dangling_pointers": dangling,
    }
    return {
        BUNDLE_NAME: gz,
        BUNDLE_NAME + ".sha256": f"{digest}  {BUNDLE_NAME}\n".encode(),
        "PROVENANCE.json": (json.dumps(provenance, indent=2, ensure_ascii=False) + "\n").encode(),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument(
        "--clinvar-gks",
        type=Path,
        default=DEFAULT_CLINVAR_GKS,
        help=f"local ClinVar-GKM clone (default {DEFAULT_CLINVAR_GKS})",
    )
    ap.add_argument("--ref", default=DEFAULT_REF, help=f"commit to read (default {DEFAULT_REF})")
    ap.add_argument("--check", action="store_true", help="rebuild in memory and fail if the files on disk differ")
    args = ap.parse_args()
    try:
        files = build(args.clinvar_gks, args.ref)
    except FixtureError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    if args.check:
        stale = [
            n for n, data in files.items() if not (OUT_DIR / n).is_file() or (OUT_DIR / n).read_bytes() != data
        ]
        for n in stale:
            print(f"differs: {OUT_DIR / n}", file=sys.stderr)
        print("OK" if not stale else "FAIL")
        return 1 if stale else 0
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, data in files.items():
        (OUT_DIR / name).write_bytes(data)
        print(f"wrote {OUT_DIR / name} ({len(data):,d} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
