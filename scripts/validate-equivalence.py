#!/usr/bin/env python3
"""Run `validate` with both engines on one bundle and each schema set, and check every record's verdict agrees.

For each schema set, `clinvar-gkm-parquet validate` runs once with `--engine rs` and once with `--engine python`,
each writing its counts (`--json`) and its failing records (`--failures`). The verdicts agree when both runs read
the same records per section and fail the same records (section and key). Any difference is printed with both
engines' errors, and the script exits 1. Timings and counts go to <out>/summary.json and a Markdown table.

Usage:
    python scripts/validate-equivalence.py <bundle> <out dir> --schema-set NAME=DIR [--schema-set ...]
        [--index FILE] [--workers N] [--engines rs,python]
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path


def run(bundle: str, schema: str, engine: str, out: Path, tag: str, args) -> dict:
    cmd = [sys.executable, "-m", "clinvar_gkm_parquet.cli", "validate", bundle, "--bundle-schema", schema]
    cmd += [
        "--engine",
        engine,
        "--json",
        str(out / f"{tag}.json"),
        "--failures",
        str(out / f"{tag}.failures.jsonl"),
    ]
    if args.index:
        cmd += ["--index", args.index]
    if args.workers:
        cmd += ["--workers", str(args.workers)]
    t = time.time()
    with open(out / f"{tag}.txt", "w") as log:
        code = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT).returncode
    wall = time.time() - t
    if code not in (0, 1):
        sys.exit(f"{tag}: validate stopped with exit {code}; see {out / f'{tag}.txt'}")
    result = json.loads((out / f"{tag}.json").read_text())
    result["wall_seconds"] = round(wall, 1)
    result["exit"] = code
    failures = {}
    with open(out / f"{tag}.failures.jsonl") as f:
        for line in f:
            rec = json.loads(line)
            failures[(rec["section"], rec["key"])] = rec["errors"]
    result["_failures"] = failures
    return result


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("bundle")
    p.add_argument("out", type=Path)
    p.add_argument("--schema-set", action="append", required=True, help="NAME=DIR")
    p.add_argument("--index")
    p.add_argument("--workers", type=int)
    p.add_argument("--engines", default="rs,python")
    args = p.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    engines = args.engines.split(",")
    summary = {
        "bundle": args.bundle,
        "machine": f"{platform.platform()} {platform.machine()}, {os.cpu_count()} CPUs",
        "python": platform.python_version(),
        "runs": {},
    }
    ok = True
    for spec in args.schema_set:
        name, _, directory = spec.partition("=")
        results = {e: run(args.bundle, directory, e, args.out, f"{name}-{e}", args) for e in engines}
        for e, r in results.items():
            summary["runs"][f"{name}-{e}"] = {k: v for k, v in r.items() if not k.startswith("_")}
        if len(engines) < 2:
            continue
        a, b = (results[e] for e in engines[:2])
        counts_a = {s: v["records"] for s, v in a["sections"].items()}
        counts_b = {s: v["records"] for s, v in b["sections"].items()}
        if counts_a != counts_b:
            ok = False
            print(f"{name}: the engines read different records per section: {counts_a} vs {counts_b}")
        fa, fb = a["_failures"], b["_failures"]
        only_a, only_b = sorted(set(fa) - set(fb)), sorted(set(fb) - set(fa))
        for key in only_a[:20]:
            print(f"{name}: {key} fails under {engines[0]} only: {fa[key][:3]}")
        for key in only_b[:20]:
            print(f"{name}: {key} fails under {engines[1]} only: {fb[key][:3]}")
        agree = not only_a and not only_b and counts_a == counts_b
        ok = ok and agree
        summary["runs"][f"{name}-agreement"] = {
            "records": sum(counts_a.values()),
            "failing": [len(fa), len(fb)],
            "disagreements": len(only_a) + len(only_b),
            "agree": agree,
        }
        print(
            f"{name}: {sum(counts_a.values()):,d} records; failing {len(fa)} / {len(fb)}; "
            f"{'AGREE' if agree else 'DISAGREE'}"
        )
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    rows = ["| Run | Records | Failing | Wall s | Workers |", "| --- | ---: | ---: | ---: | ---: |"]
    for tag, r in summary["runs"].items():
        if "sections" in r:
            rows.append(
                f"| {tag} | {r['records']:,d} | {r['failing']:,d} | {r['wall_seconds']:,.1f} | {r['workers']} |"
            )
    (args.out / "summary.md").write_text("\n".join(rows) + "\n")
    print("\n".join(rows))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
