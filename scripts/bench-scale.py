#!/usr/bin/env python3
"""Benchmark `clinvar-gkm-parquet build` on a scaled bundle made from the real excerpt's records.

Makes a full bundle from the records of tests/fixtures/real-1.1.0/clinvar-gkm_excerpt.json.gz. Each section's
records are replicated in proportion to that section's record count in the real full the excerpt was cut from
(PROVENANCE.json `source.section_records`, the 2026-10-18 weekly full: 91.5 M records), scaled so that the bundle
holds about --scale-mb of uncompressed JSON. So the sections keep the real full's mix by record count; their
record sizes are the excerpt's, which picks unusual shapes and runs larger than average (the excerpt averages
about 550 bytes a line, a real full about 290).

Every replica gets a UNIQUE key whose shape still matches what the table SQL expects: the replica number is
appended to the key's first run of digits, so an evidence-line key keeps its `clinvar.submission:SCV<n>.<v>`,
`VCV<n>.<v>-...` or `RCV<n>.<v>-...` shape; a key with no digit gets a `-r<i>` suffix. Each record is copied
with its own key's text (e.g. its `id`) replaced the same way, so replicas differ as real records do; other `#/`
pointers are kept and need not resolve, since the tool never resolves them. The bundle keeps the assembler's line
layout (design plan §4.2) and its gzip level 3.

With --deltas it also writes 2 synthetic deltas (+ manifests in build-delta-manifest.py's shape): each updates,
adds and deletes about 0.5 % of each section's keys (an update carries another record of the same section and,
for evidence lines, of the same id shape).

Then it runs the build as a subprocess (this interpreter, `-m clinvar_gkm_parquet.cli build`) and records: wall
time; peak RSS of the child (os.wait4); peak disk, sampled every 0.5 s as the allocated bytes under the build's
work and output directories (the input bundle is not counted); and the output size. Scratch data lives in a new
temporary directory under --dir and is deleted afterwards unless --keep.

Usage:
    scripts/bench-scale.py --scale-mb 300 [--deltas] [--dir DIR] [--json OUT] [--keep] [-- build args...]

Run it with the interpreter of the environment whose tool you want to measure (e.g. `uv run python
scripts/bench-scale.py ...`).
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests/fixtures/real-1.1.0/clinvar-gkm_excerpt.json.gz"
PROVENANCE = FIXTURE.parent / "PROVENANCE.json"
FULL_SOURCE_RELEASE = "2026-10-18"
DELTA_RELEASES = ("2026-10-25", "2026-11-01")
GZIP_LEVEL = 3  # the assembler's level

_SECTION = re.compile(rb'^  "([^"]+)": \{$')
_RECORD = re.compile(rb'^    ("(?:[^"\\]|\\.)*"): (.*?),?$')
_DIGITS = re.compile(rb"[0-9]+")
_EL_SHAPES = (
    re.compile(rb'^"clinvar\.submission:SCV[0-9]+\.[0-9]+"$'),
    re.compile(rb'^"VCV[0-9]+\.[0-9]+-'),
    re.compile(rb'^"RCV[0-9]+\.[0-9]+-'),
)


def read_sections(path: Path) -> dict[str, list[tuple[bytes, bytes]]]:
    """{section: [(key as JSON bytes, record bytes)]} in bundle order."""
    out: dict[str, list[tuple[bytes, bytes]]] = {}
    section = None
    for line in gzip.decompress(path.read_bytes()).split(b"\n"):
        if m := _SECTION.match(line):
            section = m.group(1).decode()
            out[section] = []
        elif section and (m := _RECORD.match(line)):
            out[section].append((m.group(1), m.group(2)))
    return out


def replicas_for(sections, real_counts: dict[str, int], target_bytes: float) -> dict[str, int]:
    """Replicas per section: proportional to real_count / excerpt_count, scaled to about target_bytes."""
    size = {s: sum(len(k) + len(r) + 7 for k, r in recs) for s, recs in sections.items()}
    weight = {s: real_counts.get(s, len(recs)) / len(recs) for s, recs in sections.items()}
    f = target_bytes / sum(weight[s] * size[s] for s in sections)
    return {s: max(1, round(f * weight[s])) for s in sections}


def replica_key(key: bytes, i: int) -> bytes:
    if i == 0:
        return key
    m = _DIGITS.search(key)
    if m:
        return key[: m.end()] + b"%06d" % i + key[m.end() :]
    return key[:-1] + b"-r%d" % i + b'"'


def replica(key: bytes, record: bytes, i: int) -> tuple[bytes, bytes]:
    rk = replica_key(key, i)
    return rk, (record.replace(key[1:-1], rk[1:-1]) if i else record)


def write_bundle(path: Path, sections, reps: dict[str, int]) -> int:
    """Write the scaled full; returns the uncompressed size."""
    size = 0
    with open(path, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=GZIP_LEVEL, mtime=0) as gz:

        def w(b: bytes) -> None:
            nonlocal size
            gz.write(b)
            size += len(b)

        w(b'{\n  "schema_version": "1.1.0"')
        for name, recs in sections.items():
            w(b',\n  "' + name.encode() + b'": {\n')
            first = True
            for i in range(reps[name]):
                buf = bytearray()
                for k, r in recs:
                    if not first:
                        buf += b",\n"
                    first = False
                    rk, rr = replica(k, r, i)
                    buf += b"    " + rk + b": " + rr
                w(bytes(buf))
            w(b"\n  }")
        w(b"\n}\n")
    return size


def _shape(section: str, key: bytes) -> int:
    if section != "evidenceLine":
        return 0
    return next(i for i, p in enumerate(_EL_SHAPES) if p.match(key))


def write_deltas(out: Path, sections, reps: dict[str, int]) -> list[Path]:
    """Two synthetic deltas; returns the delta bundle paths, oldest first."""
    paths = []
    baseline = FULL_SOURCE_RELEASE
    for d, release in enumerate(DELTA_RELEASES, start=1):
        upserts: dict[str, list[tuple[bytes, bytes]]] = {}
        deleted: dict[str, list[str]] = {}
        for name, recs in sections.items():
            n = reps[name]
            by_shape: dict[int, list[bytes]] = {}
            for k, r in recs:
                by_shape.setdefault(_shape(name, k), []).append(r)
            m = max(1, n // 200)  # replicas touched per change kind: about 0.5 % of the keys each
            ups, dels = [], []
            for i in range(m):
                rep_upd = (d * 3 * m + i) % n
                rep_del = (d * 3 * m + m + i) % n
                for j, (k, _) in enumerate(recs):
                    pool = by_shape[_shape(name, k)]
                    ups.append((replica_key(k, rep_upd), pool[(j + d) % len(pool)]))  # update
                    ups.append(replica(k, recs[j][1], n + (d - 1) * m + i))  # add
                    if rep_del != rep_upd:
                        dels.append(json.loads(replica_key(k, rep_del)))
            upserts[name], deleted[name] = ups, dels
        compact = release[:7] + release[8:]
        bpath = out / f"clinvar-gkm-delta_{compact}.json.gz"
        text = bytearray(b'{\n  "schema_version": "1.1.0"')
        for name, recs in upserts.items():
            text += b',\n  "' + name.encode() + b'": {\n'
            text += b",\n".join(b"    " + k + b": " + r for k, r in recs)
            text += b"\n  }"
        text += b"\n}\n"
        bpath.write_bytes(gzip.compress(bytes(text), compresslevel=GZIP_LEVEL, mtime=0))
        manifest = {
            "release": release,
            "baseline_release": baseline,
            "compare_release": release,
            "pipeline_version": "synthetic (bench-scale.py)",
            "checkpoint_full": None,
            "sections": {s: {"added": 0, "updated": 0, "deleted": keys} for s, keys in deleted.items()},
        }
        (out / f"clinvar-gkm-delta_{compact}.manifest.json").write_text(json.dumps(manifest))
        paths.append(bpath)
        baseline = release
    return paths


def _allocated(dirs: list[Path]) -> int:
    total = 0
    for d in dirs:
        for root, _, files in os.walk(d):
            for f in files:
                try:
                    st = os.lstat(os.path.join(root, f))
                except OSError:
                    continue
                total += st.st_blocks * 512
    return total


def run_build(cmd: list[str], watch: list[Path]) -> dict:
    peak = 0
    stop = threading.Event()

    def sample() -> None:
        nonlocal peak
        while not stop.is_set():
            peak = max(peak, _allocated(watch))
            stop.wait(0.5)

    t0 = time.monotonic()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    t = threading.Thread(target=sample, daemon=True)
    t.start()
    output = proc.stdout.read()
    _, status, ru = os.wait4(proc.pid, 0)
    wall = time.monotonic() - t0
    proc.returncode = os.waitstatus_to_exitcode(status)
    stop.set()
    t.join()
    peak = max(peak, _allocated(watch))
    rss = ru.ru_maxrss if sys.platform == "darwin" else ru.ru_maxrss * 1024  # bytes on macOS, KiB on Linux
    return {"rc": proc.returncode, "wall_s": round(wall, 1), "peak_rss": rss, "peak_disk": peak, "log": output}


def _mb(n: int) -> str:
    return f"{n / 1e6:,.0f} MB"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--scale-mb", type=float, required=True, help="uncompressed JSON size of the full, MB")
    p.add_argument("--deltas", action="store_true", help="also write and replay 2 synthetic deltas")
    p.add_argument("--dir", type=Path, help="where to make the scratch directory (default: the system temp dir)")
    p.add_argument("--json", type=Path, help="write the result here as JSON")
    p.add_argument("--keep", action="store_true", help="keep the scratch data")
    p.add_argument("build_args", nargs="*", help="extra build arguments, after --")
    args = p.parse_args()

    scratch = Path(tempfile.mkdtemp(prefix="bench-scale-", dir=args.dir))
    try:
        sections = read_sections(FIXTURE)
        real = json.loads(PROVENANCE.read_text())["source"]["section_records"]
        reps = replicas_for(sections, real, args.scale_mb * 1e6)
        inp = scratch / "in"
        inp.mkdir()
        full = inp / "clinvar-gkm_bench.json.gz"
        t0 = time.monotonic()
        raw_size = write_bundle(full, sections, reps)
        deltas = write_deltas(inp, sections, reps) if args.deltas else []
        records = sum(reps[s] * len(r) for s, r in sections.items())
        print(
            f"bundle: {records} records, {_mb(raw_size)} JSON, {_mb(full.stat().st_size)} gzip "
            f"({time.monotonic() - t0:.0f} s to write); {len(deltas)} delta(s)",
            flush=True,
        )
        out, work = scratch / "out", scratch / "work"
        cmd = [sys.executable, "-m", "clinvar_gkm_parquet.cli", "build", "--full", str(full)]
        cmd += ["--full-source-release", FULL_SOURCE_RELEASE, "--out", str(out), "--work-dir", str(work)]
        for d in deltas:
            cmd += ["--delta", str(d)]
        cmd += args.build_args
        res = run_build(cmd, [out, work])
        if res["rc"] != 0:
            print(res["log"].decode("utf-8", "replace")[-4000:], file=sys.stderr)
            print(f"build failed (exit {res['rc']})", file=sys.stderr)
            return 1
        out_size = sum(f.stat().st_size for f in out.glob("*.parquet"))
        result = {
            "records": records,
            "section_records": {s: reps[s] * len(r) for s, r in sections.items()},
            "json_bytes": raw_size,
            "gzip_bytes": full.stat().st_size,
            "deltas": len(deltas),
            "wall_s": res["wall_s"],
            "peak_rss": res["peak_rss"],
            "peak_disk": res["peak_disk"],
            "output_bytes": out_size,
            "build_args": args.build_args,
        }
        print(
            f"build: {res['wall_s']} s, peak RSS {_mb(res['peak_rss'])}, peak disk (work + out) "
            f"{_mb(res['peak_disk'])}, output {_mb(out_size)}"
        )
        if args.json:
            args.json.write_text(json.dumps(result, indent=2) + "\n")
        return 0
    finally:
        if not args.keep:
            shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
