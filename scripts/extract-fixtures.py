#!/usr/bin/env python3
"""Extract a small, connected excerpt of a real ClinVar-GKM 1.1.0 full bundle as a test fixture.

Design plan §8.1. The input is one published full bundle (a local path, an https:// URL, or `-` for stdin). The
output is a mini-bundle in the exact §4.2 line layout, holding a few records per shape the table tests need,
plus every record they point to, its `.sha256` sidecar and PROVENANCE.json. Record bytes are copied verbatim
from the source lines, in the source's section order and the source's key order: nothing is re-serialized.

How it reads (streaming only; nothing the size of a bundle is held in memory or written to disk):

- Every pass streams the gzip bundle line by line. Only the key of a line is looked at, unless the line is
  wanted. Pointers (`"#/<section>/<key>"` string values) are found by a regex over the wanted record's text;
  only records that pass a shape's substring prefilter are json-parsed, to confirm the shape.
- Pass 1 reads the whole stream: it computes the sha256 of the compressed bytes (and fails if `--sha256` is
  given and differs), checks schema_version, picks the shape candidates and the `--seed`s, and captures the
  records they point to that come later in the file.
- A record mostly points to EARLIER sections (vcv -> proposition -> variation -> allele -> location ->
  sequenceReference), so each later pass captures the records the previous pass found it needs, and the ones
  they point to further on. A later pass stops at the first section with nothing left to find, so the last
  passes read only the start of the file. The passes end when nothing is pending. A pointer still unresolved
  after a pass that was looking for it from the start is dangling (listed; there should be none in a full).
- Passes 2+ RE-OPEN the source; nothing is staged. A local path or URL is re-opened as is. A stdin stream
  can't be read twice, so `-` needs `--reopen PATH_OR_URL` or `--reopen-cmd "gsutil cat gs://..."` (run
  once per later pass). Each re-opened stream is checked against pass 1: the sha256 of its first k * 64 MiB
  must equal pass 1's at the same offsets, and a pass that stops early reads on to the next checkpoint, so
  every byte a later pass took records from is verified. This keeps disk use at the size of the output.
  Staging the bytes later passes need is not possible in a bounded way: which earlier records are needed is
  only known after the later records that point to them have been read.

Selection: for each built-in shape, pass 1 keeps the first `--candidates` matching records. After the passes,
each candidate's closure (the record plus everything it points to, transitively) is measured, and the first
`--per-shape` candidates whose closure is at most `--max-seed-bytes` become seeds. The extension shapes (one
per extension name in the table spec's `extension_slots`) are then covered greedily: a name already present in
the excerpt adds nothing; otherwise the first candidate under the budget is added. Shapes with no match, or
whose every candidate is over the budget, are reported and recorded in PROVENANCE.json. `--seed section:key`
adds a record unconditionally (it must exist).

After writing, the output is re-read with the tool's own `bundle.stage_bundle` to prove it is a valid bundle,
and each staged record is compared with the source bytes.

Usage (see tests/fixtures/real-1.1.0/README.md):
    scripts/extract-fixtures.py SOURCE --source-url URL --release YYYY-MM-DD [--sha256 HEX] [options]
    gsutil cat gs://.../clinvar-gkm_2026-1018.json.gz | scripts/extract-fixtures.py - \\
        --reopen-cmd "gsutil cat gs://.../clinvar-gkm_2026-1018.json.gz" --source-url ... --release ...
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import re
import shlex
import subprocess
import sys
import tempfile
import time
import zlib
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from clinvar_gkm_parquet import __version__ as TOOL_VERSION
from clinvar_gkm_parquet.bundle import SECTIONS, stage_bundle
from clinvar_gkm_parquet.compat import ACCEPTED_SCHEMA_VERSIONS, TABLE_SPEC_VERSION
from clinvar_gkm_parquet.errors import ToolError
from clinvar_gkm_parquet.model import InputFile
from clinvar_gkm_parquet.sources import open_stream
from clinvar_gkm_parquet.spec import load_spec

SCRIPT_VERSION = "1.0"
GENERATOR = "scripts/extract-fixtures.py"

REPO = Path(__file__).resolve().parents[1]
DEFAULT_OUT = REPO / "tests/fixtures/real-1.1.0"
DEFAULT_NAME = "clinvar-gkm_excerpt.json.gz"
DEFAULT_MAX_BYTES = 1_000_000  # the compressed mini-bundle (design plan §8.1: under 1 MB, committed)
DEFAULT_MAX_SEED_BYTES = 128 * 1024  # one seed's closure, uncompressed record lines
DEFAULT_PER_SHAPE = 2
DEFAULT_CANDIDATES = 4
GZIP_LEVEL = 9
CHECKPOINT_BYTES = 64 << 20  # re-opened streams are checked against pass 1 at every multiple of this
_READ_BUFFER = 1 << 20

SEC_INDEX = {name: i for i, name in enumerate(SECTIONS)}
PROPOSITION_SECTIONS = tuple(s for s in SECTIONS if s.startswith("proposition"))

# The assembler's layout (design plan §4.2; bundle.py has the strict reader the output is checked with).
_SCHEMA_LINE = re.compile(rb'  "schema_version": "([^"\\]*)"(,?)\n')
_SECTION_OPEN = re.compile(rb'  "([^"\\]*)": \{\n')
# A string VALUE that starts with "#/": after ':' (object member), '[' or ',' (array element). An escaped
# quote inside a string is preceded by a backslash, so free text can't match.
_POINTER = re.compile(rb'(?<=[:\[,])"#/([^"/\\]+)/((?:[^"\\]|\\.)*)"')


class ExtractError(Exception):
    pass


# ---------------------------------------------------------------------------------------------------------------
# Shapes


@dataclass(frozen=True)
class Shape:
    name: str
    description: str
    sections: tuple[str, ...]
    key: Callable[[bytes], bool] | None = None  # on the JSON-encoded key
    pre: Callable[[bytes], bool] | None = None  # substring prefilter on the line text
    confirm: Callable[[dict], bool] | None = None  # on the parsed record, prefilter hits only
    extension: bool = False  # an extension-name shape: covered greedily, one seed at most

    def matches_key_and_text(self, key: bytes, line: bytes) -> bool:
        return (self.key is None or self.key(key)) and (self.pre is None or self.pre(line))


def _ext(obj: object, name: str):
    if isinstance(obj, dict):
        for e in obj.get("extensions") or ():
            if isinstance(e, dict) and e.get("name") == name:
                return e.get("value")
    return None


def ext_names(node: object, out: set[str], in_ext: bool = False) -> set[str]:
    """Every extension name at any depth of `node` (elements of any list under an `extensions` key)."""
    if isinstance(node, dict):
        if in_ext and isinstance(node.get("name"), str):
            out.add(node["name"])
        for k, v in node.items():
            ext_names(v, out, k == "extensions")
    elif isinstance(node, list):
        for v in node:
            ext_names(v, out, in_ext)
    return out


def _has(*needles: bytes) -> Callable[[bytes], bool]:
    return lambda line: all(n in line for n in needles)


def _ext_is(name: str, value: str) -> Callable[[dict], bool]:
    return lambda obj: _ext(obj, name) == value


def _key_starts(prefix: str) -> Callable[[bytes], bool]:
    p = json.dumps(prefix)[:-1].encode()  # the opening quote and the escaped prefix
    return lambda key: key.startswith(p)


def _key_has(part: str) -> Callable[[bytes], bool]:
    p = part.encode()
    return lambda key: p in key


def _is_range(obj: dict) -> bool:
    return isinstance(obj.get("start"), list) or isinstance(obj.get("end"), list)


def _has_ext_names(*names: str) -> Callable[[dict], bool]:
    return lambda obj: set(names) <= ext_names(obj, set())


def _gene_context_placeholder(obj: dict) -> bool:
    return _ext(obj.get("geneContextQualifier"), "geneContextSource") == "submittedNotNormalized"


def shapes(spec_version: str = TABLE_SPEC_VERSION) -> list[Shape]:
    """The shapes design plan §8.1 names, then one shape per extension name in the spec's slot maps."""
    var = ("variation",)
    out = [
        Shape(
            "allele",
            "a variation with categoricalVariationType CanonicalAllele (with its allele, location, sequence)",
            var,
            pre=_has(b'"CanonicalAllele"'),
            confirm=_ext_is("categoricalVariationType", "CanonicalAllele"),
        ),
        Shape(
            "copy-number-count",
            "a variation with categoricalVariationType CategoricalCnvCount (with its copyNumberCount)",
            var,
            pre=_has(b'"CategoricalCnvCount"'),
            confirm=_ext_is("categoricalVariationType", "CategoricalCnvCount"),
        ),
        Shape(
            "copy-number-change",
            "a variation with categoricalVariationType CategoricalCnvChange (with its copyNumberChange)",
            var,
            pre=_has(b'"CategoricalCnvChange"'),
            confirm=_ext_is("categoricalVariationType", "CategoricalCnvChange"),
        ),
        Shape(
            "uncertain-range-location",
            "a location whose start or end is a Range [min, max]",
            ("location",),
            pre=lambda line: b'"start":[' in line or b'"end":[' in line,
            confirm=_is_range,
        ),
        Shape(
            "haplotype",
            "a variation with clinvarSubclassType Haplotype",
            var,
            pre=_has(b'"Haplotype"'),
            confirm=_ext_is("clinvarSubclassType", "Haplotype"),
        ),
        Shape(
            "genotype",
            "a variation with clinvarSubclassType Genotype",
            var,
            pre=_has(b'"Genotype"'),
            confirm=_ext_is("clinvarSubclassType", "Genotype"),
        ),
        Shape(
            "germline-vcv", "a germline VCV aggregate statement (key VCV...-G-...)", ("vcv",), key=_key_has("-G-")
        ),
        Shape(
            "somatic-vcv", "a somatic VCV aggregate statement (key VCV...-S-...)", ("vcv",), key=_key_has("-S-")
        ),
        Shape("rcv", "an RCV aggregate statement", ("rcv",)),
        Shape(
            "therapy-group",
            "a proposition pointing to a #/therapyGroup/ (with the group and its therapies)",
            PROPOSITION_SECTIONS,
            pre=_has(b'"#/therapyGroup/'),
        ),
        Shape(
            "gene-context-placeholder",
            "a proposition whose geneContextQualifier has geneContextSource submittedNotNormalized",
            PROPOSITION_SECTIONS,
            pre=_has(b'"submittedNotNormalized"'),
            confirm=_gene_context_placeholder,
        ),
        Shape(
            "hgvs-repair",
            "a variation with a repaired hgvs expression (expressionRepaired + originalClinvarExpression)",
            var,
            pre=_has(b'"expressionRepaired"', b'"originalClinvarExpression"'),
            confirm=_has_ext_names("expressionRepaired", "originalClinvarExpression"),
        ),
        Shape(
            "evidence-line-scv",
            "an evidence line keyed clinvar.submission:SCV<n>.<v>",
            ("evidenceLine",),
            key=_key_starts("clinvar.submission:SCV"),
        ),
        Shape(
            "evidence-line-vcv", "an evidence line keyed VCV<n>.<v>-...", ("evidenceLine",), key=_key_starts("VCV")
        ),
        Shape(
            "evidence-line-rcv", "an evidence line keyed RCV<n>.<v>-...", ("evidenceLine",), key=_key_starts("RCV")
        ),
    ]
    spec = load_spec(spec_version)
    for t in spec.tables:
        for col, mapping in t.extension_slots.items():
            for name in mapping:
                needle = b'"name":' + json.dumps(name, ensure_ascii=False).encode()
                out.append(
                    Shape(
                        f"ext:{t.name}.{col}:{name}",
                        f"a record in {'/'.join(t.sections)} with extension {name} under {col}",
                        tuple(t.sections),
                        pre=_has(needle),
                        confirm=(lambda c, n: lambda obj: n in ext_names({c: obj.get(c)}, set()))(col, name),
                        extension=True,
                    )
                )
    for s in out:
        unknown = set(s.sections) - set(SECTIONS)
        if unknown:
            raise ExtractError(f"shape {s.name}: unknown section(s) {sorted(unknown)}")
    return out


# ---------------------------------------------------------------------------------------------------------------
# Streams


class Tap(io.RawIOBase):
    """Passes compressed bytes through, hashing them. Pass 1 records the sha256 of every CHECKPOINT_BYTES-long
    prefix; a re-opened stream (`reference` given) must give the same digests at the same offsets."""

    def __init__(self, raw: BinaryIO, every: int, reference: Tap | None = None):
        self._raw = raw
        self.every = every
        self.reference = reference
        self.sha = hashlib.sha256()
        self.pos = 0
        self.digests: list[str] = []
        self.eof = False

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        n = self._raw.readinto(b) or 0
        if not n:
            self.eof = True
            return 0
        mv = memoryview(b)[:n]
        off = 0
        while off < n:
            take = min(self.every - self.pos % self.every, n - off)
            self.sha.update(mv[off : off + take])
            self.pos += take
            off += take
            if self.pos % self.every == 0:
                self._checkpoint()
        return n

    def _checkpoint(self) -> None:
        digest = self.sha.copy().hexdigest()
        self.digests.append(digest)
        ref = self.reference
        if ref is not None:
            i = len(self.digests) - 1
            if i >= len(ref.digests) or ref.digests[i] != digest:
                raise ExtractError(
                    f"the re-opened source differs from the pass-1 stream within its first {self.pos:,d} bytes "
                    "(a different or changed file); nothing was written"
                )

    def drain(self) -> None:
        buf = bytearray(_READ_BUFFER)
        while self.readinto(buf):
            pass
        self.check_end()

    def settle(self) -> None:
        """After an early stop: read on to the next checkpoint (or the end), so every byte read is verified."""
        buf = bytearray(_READ_BUFFER)
        while self.pos % self.every:
            n = self.readinto(memoryview(buf)[: min(len(buf), self.every - self.pos % self.every)])
            if not n:
                self.check_end()
                return

    def check_end(self) -> None:
        ref = self.reference
        if ref is not None and (self.pos != ref.pos or self.sha.hexdigest() != ref.sha.hexdigest()):
            raise ExtractError(
                f"the re-opened source differs from the pass-1 stream: {self.pos:,d} bytes, sha256 "
                f"{self.sha.hexdigest()}; pass 1 read {ref.pos:,d} bytes, sha256 {ref.sha.hexdigest()}"
            )


def _is_url(s: str) -> bool:
    return s.startswith("https://")  # as sources.open_stream: local paths and https:// URLs only


class Source:
    """Opens the bundle for each pass: pass 1 from SOURCE (path, URL or stdin), later passes re-opened."""

    def __init__(self, source: str, reopen: str | None, reopen_cmd: str | None):
        self.source = source
        self.reopen = reopen
        self.reopen_cmd = reopen_cmd

    def describe(self, pass_no: int) -> str:
        if pass_no == 1:
            return "stdin" if self.source == "-" else self.source
        if self.reopen_cmd:
            return f"`{self.reopen_cmd}`"
        return self.reopen or self.source

    def open(self, pass_no: int) -> tuple[BinaryIO, Callable[[bool], None]]:
        """(raw stream, close(read_to_end))."""
        if pass_no == 1 and self.source == "-":
            return sys.stdin.buffer, lambda complete: None
        if pass_no > 1 and self.reopen_cmd:
            try:
                proc = subprocess.Popen(shlex.split(self.reopen_cmd), stdout=subprocess.PIPE)
            except OSError as e:
                raise ExtractError(f"--reopen-cmd {self.reopen_cmd!r}: {e.strerror or e}") from e

            def close(complete: bool) -> None:
                proc.stdout.close()
                if proc.poll() is None and not complete:
                    proc.terminate()
                rc = proc.wait()
                if complete and rc != 0:
                    raise ExtractError(f"--reopen-cmd exited with status {rc}")

            return proc.stdout, close
        where = self.source if pass_no == 1 else (self.reopen or self.source)
        if where == "-":
            raise ExtractError(
                "pass 2 is needed (records point to earlier sections), but stdin can't be read twice: give "
                "--reopen PATH_OR_URL or --reopen-cmd COMMAND for the same bundle"
            )
        if _is_url(where):
            raw = open_stream(
                InputFile(
                    source=where,
                    name=where.rsplit("/", 1)[-1],
                    release=None,
                    expected_sha256=None,
                    sha256_source=None,
                )
            )
        else:
            try:
                raw = open(where, "rb")
            except OSError as e:
                raise ExtractError(f"{where}: {e.strerror or e}") from e
        return raw, lambda complete: raw.close()


# ---------------------------------------------------------------------------------------------------------------
# Passes


@dataclass
class Captured:
    lineno: int
    key: bytes  # JSON-encoded, verbatim
    record: bytes  # verbatim
    pointers: tuple[tuple[int, bytes], ...]  # (section index, JSON-encoded key), in first-seen order

    @property
    def line_bytes(self) -> int:
        return 4 + len(self.key) + 2 + len(self.record) + 2


@dataclass
class PassStats:
    number: int
    seconds: float
    compressed_bytes: int
    lines: int
    captured: int
    stopped_before: str | None  # the section the pass stopped at, or None if it read to the end


@dataclass
class State:
    shapes: list[Shape]
    candidates_wanted: int
    schema_version: str | None = None
    section_order: list[str] = field(default_factory=list)
    captured: dict[tuple[int, bytes], Captured] = field(default_factory=dict)
    pending: list[set[bytes]] = field(default_factory=lambda: [set() for _ in SECTIONS])
    dangling: set[tuple[int, bytes]] = field(default_factory=set)
    candidates: dict[str, list[tuple[int, bytes]]] = field(default_factory=dict)
    records_total: int = 0
    section_records: dict[str, int] = field(default_factory=dict)


def _key_end(line: bytes) -> int:
    """Closing quote of the JSON key that starts at line[4], for keys with escapes (as bundle.py)."""
    i, n = 5, len(line)
    while i < n:
        c = line[i]
        if c == 0x5C:
            i += 2
        elif c == 0x22:
            return i
        else:
            i += 1
    return -1


def _short(line: bytes) -> str:
    text = line.decode("utf-8", "replace")
    return repr(text if len(text) <= 120 else text[:120] + "...")


def _record_of(line: bytes, q: int, lineno: int) -> bytes:
    if q < 0 or line[q + 1 : q + 3] != b": ":
        raise ExtractError(f"line {lineno}: malformed record line {_short(line)}")
    if line.endswith(b",\n"):
        rec = line[q + 3 : -2]
    elif line.endswith(b"\n"):
        rec = line[q + 3 : -1]
    else:
        raise ExtractError(f"line {lineno}: truncated (no newline)")
    if not (rec.startswith(b"{") and rec.endswith(b"}")):
        raise ExtractError(f"line {lineno}: the record is not a JSON object on one line")
    return rec


def _pointers(rec: bytes, lineno: int) -> tuple[tuple[int, bytes], ...]:
    out: dict[tuple[int, bytes], None] = {}
    for m in _POINTER.finditer(rec):
        sec = m.group(1).decode("utf-8", "replace")
        if sec not in SEC_INDEX:
            raise ExtractError(f"line {lineno}: pointer to unknown section {sec!r}: {m.group(0).decode()}")
        out[(SEC_INDEX[sec], b'"' + m.group(2) + b'"')] = None
    return tuple(out)


def run_pass(state: State, source: Source, pass_no: int, reference: Tap | None) -> tuple[PassStats, Tap]:
    t0 = time.monotonic()
    raw, close = source.open(pass_no)
    tap = Tap(raw, CHECKPOINT_BYTES, reference)
    complete = False
    try:
        gz = gzip.GzipFile(fileobj=tap, mode="rb")
        f = io.BufferedReader(gz, buffer_size=_READ_BUFFER)
        lines, captured, stopped = _scan(state, f, pass_no)
        if stopped is None:
            tap.drain()
            complete = True
        else:
            tap.settle()
            complete = tap.eof
    except (EOFError, gzip.BadGzipFile, zlib.error) as e:
        raise ExtractError(f"pass {pass_no}: not a complete gzip stream: {e}") from e
    finally:
        close(complete)
    stats = PassStats(pass_no, time.monotonic() - t0, tap.pos, lines, captured, stopped)
    return stats, tap


def _scan(state: State, f: BinaryIO, pass_no: int) -> tuple[int, int, str | None]:
    """One pass over the decompressed lines. Returns (lines read, records captured, section stopped at)."""
    first = pass_no == 1
    pending = state.pending
    captured = state.captured
    n_sections = len(SECTIONS)

    line = f.readline()
    if line != b"{\n":
        raise ExtractError(f"line 1: expected '{{', got {_short(line)}")
    line = f.readline()
    m = _SCHEMA_LINE.fullmatch(line)
    if not m:
        raise ExtractError(f'line 2: expected \'  "schema_version": "<v>",\', got {_short(line)}')
    version = m.group(1).decode()
    if version not in ACCEPTED_SCHEMA_VERSIONS:
        raise ExtractError(
            f"schema_version {version!r} is not one this tool reads ({', '.join(ACCEPTED_SCHEMA_VERSIONS)})"
        )
    if first:
        state.schema_version = version
    elif version != state.schema_version:
        raise ExtractError(f"pass {pass_no}: schema_version {version} differs from pass 1")

    shapes_by_sec: dict[int, list[Shape]] = {}
    if first:
        for s in state.shapes:
            state.candidates.setdefault(s.name, [])
            for sec in s.sections:
                shapes_by_sec.setdefault(SEC_INDEX[sec], []).append(s)

    lineno = 2
    n_captured = 0
    sec = -1
    sec_name = None
    pend: set[bytes] = set()
    act: list[Shape] = []
    ended = False
    sec_records = 0
    seen_sections: list[str] = []

    def capture(key: bytes, line: bytes, q: int) -> None:
        nonlocal n_captured
        node = (sec, key)
        if node in captured:
            return
        rec = _record_of(line, q, lineno)
        ptrs = _pointers(rec, lineno)
        captured[node] = Captured(lineno, key, rec, ptrs)
        pend.discard(key)
        n_captured += 1
        for p in ptrs:
            if p not in captured and p not in state.dangling:
                pending[p[0]].add(p[1])

    for line in f:
        lineno += 1
        if line.startswith(b'    "'):
            if sec_name is None:
                raise ExtractError(f"line {lineno}: record line outside a section")
            sec_records += 1
            if not (pend or act):
                continue
            q = line.find(b'"', 5)
            if q > 0 and line.find(b"\\", 5, q) >= 0:
                q = _key_end(line)
            key = line[4 : q + 1]
            if pend and key in pend:
                capture(key, line, q)
            if act:
                hits = [s for s in act if s.matches_key_and_text(key, line)]
                if hits:
                    if any(s.confirm for s in hits):
                        obj = json.loads(_record_of(line, q, lineno))
                        hits = [s for s in hits if s.confirm is None or s.confirm(obj)]
                    for s in hits:
                        cands = state.candidates[s.name]
                        cands.append((sec, key))
                        if len(cands) >= state.candidates_wanted:
                            act = [a for a in act if a is not s]
                    if hits:
                        capture(key, line, q)
            continue
        if ended:
            raise ExtractError(f"line {lineno}: data after the closing '}}': {_short(line)}")
        if sec_name is not None:
            if line in (b"  },\n", b"  }\n"):
                if first:
                    state.section_records[sec_name] = sec_records
                    state.records_total += sec_records
                sec_name, pend, act = None, set(), []
                continue
            raise ExtractError(f"line {lineno}, section {sec_name}: unexpected line {_short(line)}")
        m = _SECTION_OPEN.fullmatch(line)
        if m:
            name = m.group(1).decode("utf-8", "replace")
            if name not in SEC_INDEX:
                raise ExtractError(f"line {lineno}: unknown section {name!r}")
            if name in seen_sections:
                raise ExtractError(f"line {lineno}: section {name!r} appears twice")
            seen_sections.append(name)
            sec = SEC_INDEX[name]
            if not first and not any(pending[i] for i in range(sec, n_sections)):
                return lineno, n_captured, name
            sec_name = name
            sec_records = 0
            pend = pending[sec]
            act = [
                s for s in shapes_by_sec.get(sec, ()) if len(state.candidates[s.name]) < state.candidates_wanted
            ]
            continue
        if line == b"}\n":
            ended = True
            continue
        raise ExtractError(f"line {lineno}: unexpected line {_short(line)}")
    if not ended:
        raise ExtractError(f"truncated: the bundle ends at line {lineno} without its closing '}}'")
    if first:
        state.section_order = seen_sections
    return lineno, n_captured, None


# ---------------------------------------------------------------------------------------------------------------
# Selection


def _label(node: tuple[int, bytes]) -> str:
    return f"{SECTIONS[node[0]]}:{json.loads(node[1])}"


def closure(captured: dict, seeds) -> set[tuple[int, bytes]]:
    seen: set[tuple[int, bytes]] = set()
    stack = list(seeds)
    while stack:
        node = stack.pop()
        if node in seen or node not in captured:
            continue
        seen.add(node)
        stack.extend(captured[node].pointers)
    return seen


@dataclass
class Selection:
    seeds: dict[tuple[int, bytes], list[str]]  # seed -> shapes it covers, in insertion order
    nodes: set[tuple[int, bytes]]
    shape_report: list[dict]
    closure_bytes: dict[tuple[int, bytes], int]


def select(state: State, explicit: list[tuple[int, bytes]], per_shape: int, max_seed_bytes: int) -> Selection:
    captured = state.captured
    sizes: dict[tuple[int, bytes], int] = {}

    def size_of(node):
        if node not in sizes:
            sizes[node] = sum(captured[n].line_bytes for n in closure(captured, [node]))
        return sizes[node]

    seeds: dict[tuple[int, bytes], list[str]] = {}
    for node in explicit:
        seeds.setdefault(node, []).append("explicit")
    report = []
    for s in state.shapes:
        if s.extension:
            continue
        cands = state.candidates.get(s.name, [])
        picks = [c for c in cands if size_of(c) <= max_seed_bytes][:per_shape]
        for c in picks:
            seeds.setdefault(c, []).append(s.name)
        status = "selected" if picks else ("over-budget" if cands else "not-found")
        report.append(_shape_entry(s, status, cands, picks, sizes))

    nodes = closure(captured, seeds)
    parsed: dict[tuple[int, bytes], dict] = {}

    def covering(s: Shape):
        secs = {SEC_INDEX[x] for x in s.sections}
        for n in sorted((n for n in nodes if n[0] in secs), key=lambda n: captured[n].lineno):
            if n not in parsed:
                parsed[n] = json.loads(captured[n].record)
            if s.confirm(parsed[n]):
                return n
        return None

    for s in state.shapes:
        if not s.extension:
            continue
        cands = state.candidates.get(s.name, [])
        by = covering(s)
        if by is not None:
            report.append(_shape_entry(s, "covered", cands, [], sizes, covered_by=by))
            continue
        picks = [c for c in cands if size_of(c) <= max_seed_bytes][:1]
        for c in picks:
            seeds.setdefault(c, []).append(s.name)
            nodes |= closure(captured, [c])
        status = "selected" if picks else ("over-budget" if cands else "not-found")
        report.append(_shape_entry(s, status, cands, picks, sizes))
    for node in seeds:
        size_of(node)
    return Selection(seeds, nodes, report, sizes)


def _shape_entry(s: Shape, status: str, cands, picks, sizes, covered_by=None) -> dict:
    entry = {"shape": s.name, "description": s.description, "status": status, "candidates": len(cands)}
    if picks:
        entry["seeds"] = [_label(c) for c in picks]
    if covered_by is not None:
        entry["covered_by"] = _label(covered_by)
    if status == "over-budget":
        entry["smallest_closure_bytes"] = min(sizes[c] for c in cands)
    return entry


# ---------------------------------------------------------------------------------------------------------------
# Output


def render(state: State, nodes: set[tuple[int, bytes]]) -> tuple[bytes, dict[str, list[Captured]]]:
    by_sec: dict[str, list[Captured]] = {}
    for node in nodes:
        by_sec.setdefault(SECTIONS[node[0]], []).append(state.captured[node])
    buf = bytearray(b'{\n  "schema_version": "' + state.schema_version.encode() + b'"')
    out: dict[str, list[Captured]] = {}
    for name in state.section_order:
        recs = sorted(by_sec.get(name, ()), key=lambda c: c.lineno)
        if not recs:
            continue
        out[name] = recs
        buf += b',\n  "' + name.encode() + b'": {\n'
        buf += b",\n".join(b"    " + c.key + b": " + c.record for c in recs)
        buf += b"\n  }"
    buf += b"\n}\n"
    return bytes(buf), out


def gzip_bytes(data: bytes) -> bytes:
    out = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=out, compresslevel=GZIP_LEVEL, mtime=0) as gz:
        gz.write(data)
    return out.getvalue()


def validate(gz: bytes, name: str, release: str, sections: dict[str, list[Captured]]) -> None:
    """Re-read the output with the tool's own reader; compare every staged record with the source bytes."""
    sha = hashlib.sha256(gz).hexdigest()
    with tempfile.TemporaryDirectory(prefix="extract-fixtures.") as tmp:
        path = Path(tmp) / name
        path.write_bytes(gz)
        inp = InputFile(str(path), name, release, sha, "explicit", len(gz))
        try:
            staged = stage_bundle(inp, Path(tmp) / "work")
        except Exception as e:  # noqa: BLE001 (any reader failure means invalid output)
            raise ExtractError(f"the written excerpt does not pass bundle.stage_bundle: {e}") from e
        if staged.sha256 != sha or set(staged.sections) != set(sections):
            raise ExtractError("stage_bundle staged different sections or bytes than written")
        for name_, recs in sections.items():
            with gzip.open(staged.sections[name_].path, "rb") as fh:
                got = [tuple(line.rstrip(b"\n").split(b"\t", 1)) for line in fh]
            want = [(c.key, c.record) for c in recs]
            if got != want:
                raise ExtractError(f"section {name_}: staged records differ from the source bytes")


def check_closed(state: State, nodes: set[tuple[int, bytes]]) -> list[str]:
    """Every pointer in the excerpt resolves inside it, or is a known dangling pointer. Returns the dangling."""
    dangling = set()
    for n in nodes:
        for p in state.captured[n].pointers:
            if p in nodes:
                continue
            if p not in state.dangling:
                raise ExtractError(
                    f"{_label(n)} points to {_label(p)}, which is neither in the excerpt nor dangling"
                )
            dangling.add(p)
    return sorted(f"#/{SECTIONS[s]}/{json.loads(k)}" for s, k in dangling)


# ---------------------------------------------------------------------------------------------------------------
# Main


def _peak_rss_bytes() -> int:
    import resource

    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss if sys.platform == "darwin" else rss * 1024


def _parse_seed(text: str) -> tuple[int, bytes]:
    section, sep, key = text.partition(":")
    if not sep or not key or section not in SEC_INDEX:
        raise ExtractError(f"--seed {text!r}: expected section:key with a 1.1.0 section ({', '.join(SECTIONS)})")
    return SEC_INDEX[section], json.dumps(key, ensure_ascii=False).encode()


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("source", help="the full bundle: a local path, an https:// URL, or - for stdin")
    ap.add_argument("--source-url", required=True, help="where the bundle is published (recorded in provenance)")
    ap.add_argument("--release", required=True, help="the release the bundle represents, YYYY-MM-DD")
    ap.add_argument("--sha256", help="the published sha256 of the bundle; a different computed one fails")
    ap.add_argument("--reopen", help="path or URL of the same bundle, for passes 2+ (needed with -)")
    ap.add_argument(
        "--reopen-cmd", help='command whose stdout is the same bundle, for passes 2+ (e.g. "gsutil cat gs://...")'
    )
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT, help=f"output folder (default {DEFAULT_OUT})")
    ap.add_argument("--name", default=DEFAULT_NAME, help=f"mini-bundle file name (default {DEFAULT_NAME})")
    ap.add_argument("--seed", action="append", default=[], help="section:key to include (repeatable)")
    ap.add_argument(
        "--per-shape", type=int, default=DEFAULT_PER_SHAPE, help="seeds per shape, 1-3 (default %(default)s)"
    )
    ap.add_argument(
        "--candidates",
        type=int,
        default=DEFAULT_CANDIDATES,
        help="candidates kept per shape (default %(default)s)",
    )
    ap.add_argument(
        "--max-seed-bytes",
        type=int,
        default=DEFAULT_MAX_SEED_BYTES,
        help="largest closure of one shape seed, uncompressed (default %(default)s)",
    )
    ap.add_argument(
        "--max-bytes",
        type=int,
        default=DEFAULT_MAX_BYTES,
        help="largest compressed mini-bundle (default %(default)s)",
    )
    args = ap.parse_args(argv)
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", args.release):
        ap.error("--release must be YYYY-MM-DD")
    if args.sha256 is not None and not re.fullmatch(r"[0-9a-fA-F]{64}", args.sha256):
        ap.error("--sha256 must be 64 hex digits")
    if not 1 <= args.per_shape <= 3:
        ap.error("--per-shape must be 1, 2 or 3")
    if args.candidates < args.per_shape:
        ap.error("--candidates must be at least --per-shape")
    if args.reopen and args.reopen_cmd:
        ap.error("give --reopen or --reopen-cmd, not both")
    return args


def extract(args: argparse.Namespace, log=print) -> dict:
    """Run the passes, select, write and validate. Returns the provenance dict (and logs the run report)."""
    t_start = time.monotonic()
    explicit = [_parse_seed(s) for s in args.seed]
    state = State(shapes=shapes(), candidates_wanted=args.candidates)
    for sec, key in explicit:
        state.pending[sec].add(key)
    source = Source(args.source, args.reopen, args.reopen_cmd)

    passes: list[PassStats] = []
    reference: Tap | None = None
    while True:
        pass_no = len(passes) + 1
        if pass_no > 1 and not any(state.pending):
            break
        looking = {(s, k) for s, keys in enumerate(state.pending) for k in keys}
        stats, tap = run_pass(state, source, pass_no, reference)
        passes.append(stats)
        if pass_no == 1:
            reference = tap
            computed = tap.sha.hexdigest()
            if args.sha256 and computed != args.sha256.lower():
                raise ExtractError(
                    f"sha256 mismatch: computed {computed}, published {args.sha256.lower()}; the file is corrupt, "
                    "incomplete or not the published one. Nothing was written."
                )
            missing = [(s, k) for s, k in explicit if (s, k) not in state.captured]
            if missing:
                raise ExtractError(f"--seed not in the bundle: {', '.join(_label(n) for n in missing)}")
        stop = f"stopped before section {stats.stopped_before}" if stats.stopped_before else "read to the end"
        n_pending = sum(len(p) for p in state.pending)
        log(
            f"pass {pass_no} ({source.describe(pass_no)}): {stats.seconds:.1f} s, "
            f"{stats.compressed_bytes:,d} compressed bytes, "
            f"{stats.lines:,d} lines ({stop}); captured {stats.captured:,d}, pending {n_pending:,d}"
        )
        # Pointers the pass looked for from its start and didn't find are dangling.
        for s, keys in enumerate(state.pending):
            for k in [k for k in keys if (s, k) in looking]:
                keys.discard(k)
                state.dangling.add((s, k))

    sel = select(state, explicit, args.per_shape, args.max_seed_bytes)
    dangling = check_closed(state, sel.nodes)
    raw, sections = render(state, sel.nodes)
    gz = gzip_bytes(raw)
    if len(gz) > args.max_bytes:
        biggest = sorted(sel.seeds, key=lambda n: -sel.closure_bytes[n])[:10]
        detail = "; ".join(f"{_label(n)} {sel.closure_bytes[n]:,d} B" for n in biggest)
        raise ExtractError(
            f"the excerpt is {len(gz):,d} bytes compressed, over --max-bytes {args.max_bytes:,d}. Largest seed "
            f"closures (uncompressed): {detail}. Lower --per-shape or --max-seed-bytes. Nothing was written."
        )
    validate(gz, args.name, args.release, sections)

    digest = hashlib.sha256(gz).hexdigest()
    provenance = {
        "fixture": args.name,
        "sha256": digest,
        "schema_version": state.schema_version,
        "generator": GENERATOR,
        "script_version": SCRIPT_VERSION,
        "tool_version": TOOL_VERSION,
        "table_spec_version": TABLE_SPEC_VERSION,
        "source": {
            "url": args.source_url,
            "release": args.release,
            "schema_version": state.schema_version,
            "sha256": reference.sha.hexdigest(),
            "sha256_published": args.sha256.lower() if args.sha256 else None,
            "size": reference.pos,
            "records": state.records_total,
            "section_records": state.section_records,
        },
        "layout": (
            "design plan §4.2 line format; every key and record verbatim from the source line; source section "
            f"order; source key order within a section; gzip level {GZIP_LEVEL}, mtime 0, no file name"
        ),
        "selection": {
            "per_shape": args.per_shape,
            "candidates": args.candidates,
            "max_seed_bytes": args.max_seed_bytes,
            "explicit_seeds": [_label(n) for n in explicit],
        },
        "passes": len(passes),
        "seeds": [
            {
                "section": SECTIONS[n[0]],
                "key": json.loads(n[1]),
                "shapes": shp,
                "closure_bytes": sel.closure_bytes[n],
            }
            for n, shp in sorted(sel.seeds.items(), key=lambda kv: state.captured[kv[0]].lineno)
        ],
        "shapes": sel.shape_report,
        "shapes_not_found": [e["shape"] for e in sel.shape_report if e["status"] == "not-found"],
        "shapes_over_budget": [e["shape"] for e in sel.shape_report if e["status"] == "over-budget"],
        "counts": {name: len(recs) for name, recs in sections.items()},
        "dangling_pointers": dangling,
    }

    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / args.name).write_bytes(gz)
    (args.out / (args.name + ".sha256")).write_text(f"{digest}  {args.name}\n")
    (args.out / "PROVENANCE.json").write_text(json.dumps(provenance, indent=2, ensure_ascii=False) + "\n")

    log(
        f"wrote {args.out / args.name}: {len(gz):,d} bytes ({len(raw):,d} uncompressed), "
        f"{len(sel.nodes):,d} records in {len(sections)} sections, {len(sel.seeds)} seeds"
    )
    log(
        f"{len(passes)} passes in {time.monotonic() - t_start:.1f} s; peak memory "
        f"{_peak_rss_bytes() / (1 << 20):,.0f} MiB (RSS)"
    )
    for e in sel.shape_report:
        if e["status"] in ("not-found", "over-budget"):
            extra = (
                f" (smallest closure {e['smallest_closure_bytes']:,d} B)" if e["status"] == "over-budget" else ""
            )
            log(f"WARNING: shape {e['shape']}: {e['status']}{extra}: {e['description']}")
    if dangling:
        log(f"WARNING: {len(dangling)} dangling pointer(s) (a full should have none); see PROVENANCE.json")
    log("validated: bundle.stage_bundle reads the excerpt and stages every record byte for byte")
    return provenance


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        extract(args, log=lambda msg: print(msg, file=sys.stderr))
    except (ExtractError, ToolError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
