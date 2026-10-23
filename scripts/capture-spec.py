#!/usr/bin/env python3
"""Capture the table spec from a published Parquet set and diff it against spec/<version>/tables.yaml.

Design plan §3.2: spec 1.0 is captured from ClinVar-GKM's first 1.1.0 latest Parquet set. For every table:
DESCRIBE gives column names, order and types; a scan of every extension struct (a struct with `name` and
`value_*` fields, at any depth of a column) gives the (extension name, non-null slot) pairs, i.e. the slot map.

Usage:
    scripts/capture-spec.py <parquet dir> [--spec-version 1.0] [--write OUT.yaml]

Prints one line per difference (missing or extra table or column, order, type, slot map) and exits 1 when
there are differences, 0 when the set matches the spec. --write writes a copy of tables.yaml with the
captured column types put in (slot maps and everything else are left for review by hand). A slot-map name the
spec lists but the data doesn't show is reported as "not seen", which is not a difference by itself: a release
need not use every extension. Reads local files only.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import duckdb

from clinvar_gkm_parquet.spec import Spec, load_spec, spec_dir


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _ext_positions(t, lst: str, rel: str = ""):
    """Yield (rel, lst, [(slot, slot type)]) for every extension struct type inside t, where lst is a SQL list
    of values of type t (lists are flattened on the way down, so each position is one list of structs)."""
    if t.id == "list":
        yield from _ext_positions(t.children[0][1], f"flatten({lst})", rel + "[]")
    elif t.id == "struct":
        names = [f for f, _ in t.children]
        slots = [(f, ft) for f, ft in t.children if f.startswith("value_")]
        if "name" in names and slots:
            yield rel, lst, slots
        for f, ft in t.children:
            if ft.id in ("struct", "list"):
                yield from _ext_positions(ft, f"list_transform({lst}, lambda x: x.{_q(f)})", f"{rel}.{f}")


def capture(parquet_dir: Path, spec: Spec, con: duckdb.DuckDBPyConnection | None = None) -> dict:
    """{table: {"describe": [(col, type)], "slots": {col: {(rel, name): set(slots)}}}} for each file found."""
    con = con or duckdb.connect()
    out = {}
    names = {t.name for t in spec.tables} | {p.stem for p in parquet_dir.glob("*.parquet")}
    for name in sorted(names):
        path = parquet_dir / f"{name}.parquet"
        if not path.exists():
            continue
        src = f"read_parquet('{str(path).replace(chr(39), chr(39) * 2)}')"
        cols = [(r[0], r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
        slots: dict[str, dict[tuple[str, str], set[str]]] = {}
        for col, typ in cols:
            t = duckdb.sqltype(typ)
            for rel, lst, slot_fields in _ext_positions(t, f"[{_q(col)}]"):
                # A slot is filled when non-NULL, and for a list slot non-empty ([] is BigQuery's "absent").
                nonnull = ", ".join(
                    f"CASE WHEN {f'len(e.{_q(s)}) > 0' if st.id == 'list' else f'e.{_q(s)} IS NOT NULL'} "
                    f"THEN '{s}' END"
                    for s, st in slot_fields
                )
                sql = (
                    f"SELECT DISTINCT e.name, list_filter([{nonnull}], lambda s: s IS NOT NULL) "
                    f"FROM (SELECT unnest({lst}) AS e FROM {src}) WHERE e IS NOT NULL"
                )
                for ext_name, filled in con.execute(sql).fetchall():
                    slots.setdefault(col, {}).setdefault((rel, ext_name), set()).update(filled)
        out[name] = {"describe": cols, "slots": slots}
    return out


def diff(spec: Spec, captured: dict) -> tuple[list[str], list[str]]:
    """(differences, notes)."""
    problems: list[str] = []
    notes: list[str] = []
    spec_names = [t.name for t in spec.tables]
    for name in spec_names:
        if name not in captured:
            problems.append(f"{name}: no {name}.parquet")
    for name in captured:
        if name not in spec_names:
            problems.append(f"{name}: file not in the spec")
    for t in spec.tables:
        if t.name not in captured:
            continue
        got = captured[t.name]["describe"]
        want = t.describe()
        got_names, want_names = [c for c, _ in got], [c for c, _ in want]
        for c in want_names:
            if c not in got_names:
                problems.append(f"{t.name}.{c}: column missing from the file")
        for c in got_names:
            if c not in want_names:
                problems.append(f"{t.name}.{c}: column not in the spec")
        common = [c for c in got_names if c in want_names]
        if common != [c for c in want_names if c in got_names]:
            problems.append(f"{t.name}: column order {got_names} != spec {want_names}")
        gt, wt = dict(got), dict(want)
        for c in common:
            if gt[c] != wt[c]:
                problems.append(f"{t.name}.{c}: type\n    file: {gt[c]}\n    spec: {wt[c]}")
        seen_by_col = captured[t.name]["slots"]
        for col in sorted(set(seen_by_col) | set(t.extension_slots)):
            spec_map = t.extension_slots.get(col, {})
            seen_names = set()
            for (rel, ext_name), filled in sorted(seen_by_col.get(col, {}).items()):
                seen_names.add(ext_name)
                where = f"{t.name}.{col} at {rel}: extension {ext_name!r}"
                if len(filled) > 1:
                    problems.append(f"{where}: more than one slot filled: {sorted(filled)}")
                if ext_name not in spec_map:
                    problems.append(f"{where} (slots {sorted(filled)}) not in the spec's slot map")
                elif filled and spec_map[ext_name] not in filled:
                    problems.append(f"{where}: file slot {sorted(filled)} != spec {spec_map[ext_name]}")
            for ext_name in sorted(set(spec_map) - seen_names):
                notes.append(f"{t.name}.{col}: slot-map name {ext_name!r} not seen in the file")
    return problems, notes


def write_types(spec_yaml: Path, captured: dict, out: Path) -> int:
    """Copy spec_yaml to out with each captured column type in place of the spec's. Returns types changed.
    Columns shared through a YAML anchor are changed where the anchor is defined."""
    lines = spec_yaml.read_text(encoding="utf-8").splitlines(keepends=True)
    table = None
    column = None
    changed = 0
    for i, line in enumerate(lines):
        if m := re.match(r"^  - name: (\S+)\s*$", line):
            table, column = m.group(1), None
        elif m := re.match(r"^      - name: (\S+)\s*$", line):
            column = m.group(1)
        elif (m := re.match(r"^(        type: )(.*)$", line)) and table in captured and column:
            got = dict(captured[table]["describe"]).get(column)
            if got is not None:
                plain = re.fullmatch(r"[A-Z]+(\[\])?", got)
                new = m.group(1) + (got if plain else "'" + got.replace("'", "''") + "'")
                if new != line.rstrip("\n"):
                    lines[i] = new + "\n"
                    changed += 1
    out.write_text("".join(lines), encoding="utf-8")
    return changed


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("parquet_dir", type=Path)
    ap.add_argument("--spec-version", default="1.0")
    ap.add_argument("--write", type=Path, help="write a copy of tables.yaml with the captured types")
    args = ap.parse_args(argv)
    spec = load_spec(args.spec_version)
    captured = capture(args.parquet_dir, spec)
    problems, notes = diff(spec, captured)
    for p in problems:
        print(p)
    for n in notes:
        print(f"note: {n}")
    if args.write:
        n = write_types(spec_dir(args.spec_version) / "tables.yaml", captured, args.write)
        print(f"wrote {args.write} ({n} type(s) changed)")
    print(f"{len(captured)} table(s) read, {len(problems)} difference(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
