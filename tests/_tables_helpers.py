"""Load the bundle fixture (tests/_fixture.py) into DuckDB `bundle_<section>` tables and build spec tables
over them.

The fixture is tiny, so it is parsed with the stdlib here (the bundle line format of design plan §4.2) rather
than through the reader, which is another work package. `record` keeps the exact bytes of the bundle line.
"""

from __future__ import annotations

import gzip
import json
import re
from pathlib import Path

import duckdb
from _fixture import FIXTURE

from clinvar_gkm_parquet.spec import Table, load_spec
from clinvar_gkm_parquet.sqltemplate import macros_sql, render

SPEC = load_spec("1.0")
# The 20 sections of the 1.1.0 bundle schema (design plan §4.2).
SECTIONS = (
    "sequenceReference location allele copyNumberCount copyNumberChange gene variation condition conditionSet "
    "therapy therapyGroup submitter propositionPathogenicity propositionOncogenicity "
    "propositionSomaticClinicalImpact propositionClinvarCustom evidenceLine scv vcv rcv"
).split()

_SECTION = re.compile(r'^  "(\w+)": \{$')
_RECORD = re.compile(r'^    ("(?:[^"\\]|\\.)*"): (.*?),?$')


def read_fixture(path: Path = FIXTURE) -> dict[str, list[tuple[str, str]]]:
    """{section: [(key, record text)]} in bundle order."""
    out: dict[str, list[tuple[str, str]]] = {s: [] for s in SECTIONS}
    section = None
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if m := _SECTION.match(line):
                section = m.group(1)
                assert section in out, f"unknown section {section}"
            elif section and (m := _RECORD.match(line)):
                out[section].append((json.loads(m.group(1)), m.group(2)))
            elif line.strip() in ("}", "},"):
                section = None
    return out


_PROPOSITION_SECTIONS = (
    "propositionPathogenicity",
    "propositionOncogenicity",
    "propositionSomaticClinicalImpact",
    "propositionClinvarCustom",
)


def _condition_pointer(curie: str) -> str:
    """gkm-vcv-statement-proc.sql @ 1.0 25344ba6, lines 529 and 643: traitset / minted set -> #/conditionSet/,
    trait -> #/condition/."""
    if curie.startswith("#/"):
        return curie
    if curie.startswith(("clinvar.traitset:", "clinvar.conditionset:")):
        return f"#/conditionSet/{curie}"
    if curie.startswith("clinvar.trait:"):
        return f"#/condition/{curie}"
    raise AssertionError(f"not a condition CURIE: {curie}")


def current_shape(sections: dict[str, list[tuple[str, str]]]) -> dict[str, list[tuple[str, str]]]:
    """The fixture moved to the current 1.0 code's shape, where the 2026-10-04 examples are older (fixture
    README): VCV/RCV proposition `object`s and minted condition-set `concepts` written as bare CURIEs become
    #/condition/ or #/conditionSet/ pointers by the proc's rule. Only those values change; every other record
    keeps its bytes. Used so the tables whose fail-loud pointer check trips on the old shape still get built."""
    out = dict(sections)

    def dump(rec: dict) -> str:
        return json.dumps(rec, separators=(",", ":"), ensure_ascii=False)

    for s in _PROPOSITION_SECTIONS:
        new = []
        for key, text in sections[s]:
            rec = json.loads(text)
            if isinstance(rec.get("object"), str) and not rec["object"].startswith("#/"):
                rec["object"] = _condition_pointer(rec["object"])
                text = dump(rec)
            new.append((key, text))
        out[s] = new
    new = []
    for key, text in sections["conditionSet"]:
        rec = json.loads(text)
        if any(not c.startswith("#/") for c in rec.get("concepts", [])):
            rec["concepts"] = [_condition_pointer(c) for c in rec["concepts"]]
            text = dump(rec)
        new.append((key, text))
    out["conditionSet"] = new
    return out


def write_bundle(sections: dict[str, list[tuple[str, str]]], path: Path, schema_version: str = "1.1.0") -> Path:
    """Write sections as a gzip bundle in the assembler's line format (design plan §4.2; empty sections are
    omitted), keeping each record's text."""
    lines = ["{", f'  "schema_version": "{schema_version}"']
    for s in SECTIONS:
        recs = sections.get(s, [])
        if not recs:
            continue
        lines[-1] += ","
        lines.append(f'  "{s}": {{')
        lines += [f"    {json.dumps(k)}: {r}," for k, r in recs]
        lines[-1] = lines[-1][:-1]
        lines.append("  }")
    lines.append("}")
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return path


def connect(sections: dict[str, list[tuple[str, str]]] | None = None) -> duckdb.DuckDBPyConnection:
    """A connection with the macros loaded and one bundle_<section>(key, record, ordinal, key_json) table per
    section, as the build stages it (build.load_section): ordinal is the record's position, key_json the key as
    write_bundle writes it."""
    sections = read_fixture() if sections is None else sections
    con = duckdb.connect()
    con.execute(macros_sql())
    for s in SECTIONS:
        con.execute(f'CREATE TABLE "bundle_{s}" (key VARCHAR, record VARCHAR, ordinal BIGINT, key_json VARCHAR)')
        rows = [(k, r, i, json.dumps(k)) for i, (k, r) in enumerate(sections.get(s, []))]
        if rows:
            con.executemany(f'INSERT INTO "bundle_{s}" VALUES (?, ?, ?, ?)', rows)
    return con


def build(con: duckdb.DuckDBPyConnection, table: Table, fast: bool = True) -> str:
    """Build the table as build.py will (SELECT * FROM (<sql>) ORDER BY <sort key>); returns its name. fast=False
    builds from the slow reference SQL (render(table, fast=False))."""
    name = f"out_{table.name}" if fast else f"out_{table.name}_slow"
    sql = render(table, fast=fast)
    con.execute(f'CREATE OR REPLACE TABLE "{name}" AS SELECT * FROM ({sql}) ORDER BY "{table.sort_key}"')
    return name


def describe(con: duckdb.DuckDBPyConnection, name: str) -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in con.execute(f'DESCRIBE "{name}"').fetchall()]


def rows(con: duckdb.DuckDBPyConnection, name: str) -> list[dict]:
    cur = con.execute(f'SELECT * FROM "{name}"')
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
