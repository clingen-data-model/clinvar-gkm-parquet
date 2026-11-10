#!/usr/bin/env python3
"""Write a deterministic bundle sample: every Nth record of each section, plus the first K of each record shape.

A record's shape is the set of its paths, with array positions folded to `[]`, each object tagged with its `type`
when it has one and each `extensions` element with its `name`: two records of one shape hold the same classes,
properties and extensions, whatever their values. The bundle is streamed with the tool's reader (its sha256 checked
against the line index when given); record shapes are computed in worker processes. The sample is written in the
line layout, each kept record's key and bytes verbatim, in source order, with the source's version, and
`<out>.json` records what was kept.

Usage:
    python scripts/sample-bundle.py <bundle> <out.json.gz> [--every N] [--per-shape K] [--index FILE] [--workers N]
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
from multiprocessing import get_context

from clinvar_gkm_parquet import sources
from clinvar_gkm_parquet.bundle import BundleShape, RootRules, stream_bundle
from clinvar_gkm_parquet.compiler import root_parts
from clinvar_gkm_parquet.default_pairing import schema_source, version_property
from clinvar_gkm_parquet.schemas import load_schema_set

BATCH = 2000


def _paths(node, path: str, out: set) -> None:
    if isinstance(node, dict):
        tag = f"<{node['type']}>" if isinstance(node.get("type"), str) else ""
        out.add(path + tag)
        for k, v in node.items():
            if k == "extensions" and isinstance(v, list):
                for e in v:
                    name = e.get("name") if isinstance(e, dict) else None
                    _paths(e, f"{path}{tag}.extensions[{name}]", out)
            else:
                _paths(v, f"{path}{tag}.{k}", out)
    elif isinstance(node, list):
        for v in node:
            _paths(v, path + "[]", out)
    else:
        out.add(f"{path}:{type(node).__name__}")


def shapes(records: list[bytes]) -> list[bytes]:
    out = []
    for raw in records:
        paths: set = set()
        _paths(json.loads(raw), "", paths)
        out.append(hashlib.blake2b("\n".join(sorted(paths)).encode(), digest_size=12).digest())
    return out


class Sampler:
    def __init__(self, out, every: int, per_shape: int, pool):
        self.out, self.every, self.per_shape, self.pool = out, every, per_shape, pool
        self.version = ""
        self.first_section = True
        self.section = ""
        self.batch: list[tuple[bytes, bytes]] = []
        self.pending: list = []
        self.lines: list[bytes] = []
        self.stats: dict[str, dict] = {}
        self.seen: dict[bytes, int] = {}
        self.index = 0

    def begin(self, version: str) -> None:
        self.version = version
        self.out.write(b"{\n")
        if version:
            self.out.write(b'  "' + version_property().encode() + b'": ' + json.dumps(version).encode())

    def open_section(self, section: str):
        self.section, self.seen, self.index, self.lines = section, {}, 0, []
        self.stats[section] = {"records": 0, "every_nth": 0, "by_shape": 0, "shapes": 0}

        def add(key: bytes, record: bytes) -> None:
            self.batch.append((key, record))
            if len(self.batch) >= BATCH:
                self.flush()

        return add

    def flush(self) -> None:
        if self.batch:
            batch, self.batch = self.batch, []
            self.pending.append((batch, self.pool.apply_async(shapes, ([r for _, r in batch],))))
        while self.pending and (len(self.pending) > 40 or self.pending[0][1].ready()):
            self._take(*self.pending.pop(0))

    def _take(self, batch, result) -> None:
        st = self.stats[self.section]
        for (key, record), shape in zip(batch, result.get(), strict=True):
            n = self.seen.get(shape, 0)
            self.seen[shape] = n + 1
            keep_nth = self.index % self.every == 0
            keep_shape = n < self.per_shape
            if keep_nth or keep_shape:
                st["every_nth" if keep_nth else "by_shape"] += 1
                self.lines.append(b"    " + key + b": " + record)
            self.index += 1
            st["records"] += 1

    def close_section(self, section: str) -> None:
        self.flush()
        for item in self.pending:
            self._take(*item)
        self.pending = []
        self.stats[section]["shapes"] = len(self.seen)
        if self.version or not self.first_section:
            self.out.write(b",\n")
        self.first_section = False
        name = json.dumps(section).encode()
        if not self.lines:
            self.out.write(b"  " + name + b": {}")
            return
        self.out.write(b"  " + name + b": {\n" + b",\n".join(self.lines) + b"\n  }")

    def root_value(self, name: str, value: bytes) -> None:
        raise SystemExit(f"root value {name!r}: this sampler writes collections only")

    def end(self) -> None:
        self.out.write(b"\n}\n")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("bundle")
    p.add_argument("out")
    p.add_argument("--every", type=int, default=100)
    p.add_argument("--per-shape", type=int, default=2)
    p.add_argument("--index")
    p.add_argument("--workers", type=int, default=os.cpu_count())
    a = p.parse_args()
    index = sources.load_index(a.index) if a.index else None
    inp = sources.resolve_input(a.bundle, index=index)

    def shape_of(version: str) -> BundleShape:
        parts = root_parts(load_schema_set(schema_source(version)), version_property())
        return BundleShape(parts.collections, parts.other_values)

    rules = RootRules(version_property(), shape_of, version_required=True)
    with get_context("spawn").Pool(a.workers) as pool, gzip.open(a.out, "wb", compresslevel=1) as out:
        sampler = Sampler(out, a.every, a.per_shape, pool)
        streamed = stream_bundle(inp, sampler, rules)
        sampler.end()
    kept = sum(s["every_nth"] + s["by_shape"] for s in sampler.stats.values())
    report = {
        "source": inp.source,
        "source_sha256": streamed.sha256,
        "sha256_checked_against": inp.sha256_source,
        "every": a.every,
        "per_shape": a.per_shape,
        "records": streamed.counts if hasattr(streamed, "counts") else None,
        "sections": sampler.stats,
        "kept": kept,
    }
    with open(a.out + ".json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(sampler.stats, indent=1))
    print(f"kept {kept:,d} records; source sha256 {streamed.sha256} ({inp.sha256_source})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
