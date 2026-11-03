#!/usr/bin/env python3
"""Time each spec table's rendered SELECT on replicated real bundle records (rows/s).

Usage:
    scripts/bench-tables.py [--rows 200000] [--threads N] [--repeat 3] [--tables scv,variation] [--json out.json]

Each table's source section(s) are loaded into an in-memory DuckDB as `bundle_<section>(key, record)` from the
test fixture's records (tests/_fixture.py: the real 1.1.0 excerpt, or CLINVAR_GKM_FIXTURE), replicated to about
--rows rows per section. Each copy's key gets a fixed-width copy number inserted before the key's first digit, so
keys stay unique and keep their shape (the evidence-line split reads `clinvar.submission:SCV<n>.<v>`,
`VCV<n>.<v>-...` and `RCV<n>.<v>-...`); the record keeps its bytes except its "id", which takes the new key, so the
records are distinct too (the `distinct` column: DuckDB evaluates some things once per distinct value).
The timed statement is `SELECT count(*), count(<column>)... FROM (<sql>)`, which computes every column of every row
without keeping it (no ORDER BY and no Parquet write: those are build.py's and cost the same for any SQL); the
fastest of --repeat runs counts. rows/s is input rows (the section's) per second, on --threads threads (default:
DuckDB's); `full est.` projects it to the section sizes of the 2026-10-18 weekly full, from the fixture's
PROVENANCE.json. DuckDB spills, if it must, to a temporary directory that is removed at the end.

Run against another copy of the package (e.g. main's, to compare) by putting its `src` first on PYTHONPATH.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from _fixture import PROVENANCE  # noqa: E402
from _tables_helpers import SECTIONS, read_fixture  # noqa: E402

from clinvar_gkm_parquet.spec import load_spec  # noqa: E402
from clinvar_gkm_parquet.sqltemplate import macros_sql, render  # noqa: E402


def load(
    con: duckdb.DuckDBPyConnection, section: str, records: list[tuple[str, str]], rows: int
) -> tuple[int, int]:
    """Load the replicated section; returns (rows, distinct records)."""
    con.execute(f'CREATE OR REPLACE TABLE "bundle_{section}" (key VARCHAR, record VARCHAR)')
    if not records:
        return 0, 0
    copies = max(1, -(-rows // len(records)))
    width = len(str(copies - 1))
    con.execute("CREATE OR REPLACE TEMP TABLE _src (key VARCHAR, record VARCHAR)")
    con.executemany("INSERT INTO _src VALUES (?, ?)", records)
    # The copy number goes before the key's first digit (or after the key when it has none). The record's own
    # "id" takes the new key too, so the copies are distinct records: DuckDB runs a lateral table function such
    # as json_tree once per distinct input, which identical copies would make look free.
    con.execute(
        f"""INSERT INTO "bundle_{section}"
        SELECT k, replace(record, '"id":' || to_json(key), '"id":' || to_json(k)) FROM (
          SELECT CASE WHEN regexp_matches(key, '[0-9]')
                   THEN regexp_replace(key, '^([^0-9]*)', '\\1' || lpad(i::VARCHAR, {width}, '0'))
                   ELSE key || '-' || lpad(i::VARCHAR, {width}, '0') END AS k, key, record
          FROM _src, range({copies}) r(i))"""
    )
    n, keys, recs = con.execute(
        f'SELECT count(*), count(DISTINCT key), count(DISTINCT record) FROM "bundle_{section}"'
    ).fetchone()
    if n != keys:
        raise SystemExit(f"{section}: replicated keys are not unique")
    return n, recs


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rows", type=int, default=200_000, help="rows per section (default 200000)")
    ap.add_argument("--threads", type=int, default=None)
    ap.add_argument("--tables", default="", help="comma-separated table names (default: all)")
    ap.add_argument("--repeat", type=int, default=3, help="runs per table; the fastest counts (default 3)")
    ap.add_argument("--json", type=Path, help="also write the results here")
    args = ap.parse_args()

    spec = load_spec("1.0")
    names = [t for t in args.tables.split(",") if t] or [t.name for t in spec.tables]
    sections = read_fixture()
    full = PROVENANCE.get("source", {}).get("section_records", {})

    spill = tempfile.mkdtemp(prefix="bench-tables-")
    con = duckdb.connect(config={"temp_directory": spill})
    if args.threads:
        con.execute(f"SET threads = {args.threads}")
    con.execute(macros_sql())
    for s in SECTIONS:
        con.execute(f'CREATE TABLE "bundle_{s}" (key VARCHAR, record VARCHAR)')

    loaded: dict[str, tuple[int, int]] = {}
    results = []
    heads = ("in rows", "distinct", "out rows", "seconds", "rows/s", "full est.")
    print(f"{'table':34} " + " ".join(f"{h:>{w}}" for h, w in zip(heads, (9, 9, 9, 8, 10, 10), strict=True)))
    for name in names:
        table = spec.table(name)
        for s in table.sections:
            if s not in loaded:
                for old in list(loaded):  # keep one table's sections in memory at a time
                    if old not in table.sections:
                        con.execute(f'CREATE OR REPLACE TABLE "bundle_{old}" (key VARCHAR, record VARCHAR)')
                        del loaded[old]
                loaded[s] = load(con, s, sections[s], args.rows)
        sql = render(table)
        n_in = sum(loaded[s][0] for s in table.sections)
        n_distinct = sum(loaded[s][1] for s in table.sections)
        counts = ", ".join(f'count("{c.name}")' for c in table.columns)
        secs = float("inf")
        for _ in range(args.repeat):
            t0 = time.perf_counter()
            n_out = con.execute(f"SELECT count(*), {counts} FROM ({sql})").fetchone()[0]
            secs = min(secs, time.perf_counter() - t0)
        rate = n_in / secs if secs else float("inf")
        n_full = sum(full.get(s, 0) for s in table.sections)
        est = n_full / rate if rate else 0.0
        results.append(
            {
                "table": name,
                "rows_in": n_in,
                "distinct_records": n_distinct,
                "rows_out": n_out,
                "seconds": secs,
                "rows_per_s": rate,
                "full_s": est,
            }
        )
        print(f"{name:34} {n_in:9d} {n_distinct:9d} {n_out:9d} {secs:8.2f} {rate:10.0f} {est:9.0f}s", flush=True)
    total = sum(r["full_s"] for r in results)
    print(f"projected full-release total (input rows / rows/s, summed): {total:.0f}s")
    if args.json:
        args.json.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    con.close()
    shutil.rmtree(spill, ignore_errors=True)


if __name__ == "__main__":
    main()
