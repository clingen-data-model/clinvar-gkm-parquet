"""Driver-based selection: a verbatim mini-bundle of driver records plus the records they reach (ADR 0004).

A selective build names a driver section and its starting keys (the seeds). This module follows the `#/` pointers
between records from the seeds and writes the records it reaches as a mini-bundle in the exact bundle line layout
(design plan §4.2): every selected record's bytes copied verbatim, in source section order and source key order.
The normal build then runs on the mini-bundle, with any spec. A build with no driver never comes here.

Integration API:

    select(source, seeds, out_path, *, follow="out", max_depth=None, sections=None, provenance=None,
           work_dir=None, validate=True, compresslevel=6) -> SelectionResult

- `source` is an InputFile (read with BundleFileSource) or any RecordSource. A RecordSource gives one stream of
  `(section, key, record)` per pass, in section order and key order; the same stream on every pass. A bundle
  file is one (BundleFileSource); a replayed delta state is another (wrap a re-iterable producer with
  IterRecordSource). Keys are the JSON-encoded key bytes exactly as on a bundle line (or a staging line), and
  records are the record bytes exactly as on the line, without the trailing comma.
- `seeds` maps a section to its starting keys (decoded strings). Every seed must exist, or SelectionError lists
  the missing ones.
- `follow`: "out" adds every record a selected record points to; "in" adds every record that points to a
  selected record; "both" does both from every reached record.
- `max_depth` counts pointer hops from a seed (seeds are depth 0); None is unlimited.
- `sections` lists the sections that may be traversed and written (None: all). A pointer into a section not
  listed is not followed and is not dangling; the record that holds it keeps it in its bytes. Every seed
  section must be listed.
- `provenance` holds the fields only integration knows (`driver`, `where`, `ids_sha256`); they are merged into
  SelectionResult.parameters, which integration writes as the `clinvar_gkm_parquet.selection` Parquet key-value
  entry (SELECTION_METADATA_KEY; canonical JSON from SelectionResult.metadata_json()) and into the manifest.

How it runs (streaming and multi-pass; memory holds the selected keys and the pending pointers, never records;
disk holds the output plus a compressed spool of the selected records, which shrinks as the output is written):

- Each pass streams the source once. Outgoing pointers are found with a regex over a selected record's text;
  incoming pointers by the same regex over the records of each section that can point into the frontier (a
  section-to-section pointer map learned in pass 1 tells which) that hold a `"#/<frontier section>/` substring.
  Records are never JSON-parsed.
- Depth is the shortest hop count, exactly as a breadth-first search gives it. A pass may assign depth d to a
  record only when every record of depth d - 2 or less is already selected (then no shorter path can exist), so
  a pass completes one level and part of the next. Pointers mostly point to EARLIER sections (vcv -> proposition
  -> variation -> allele -> location -> sequenceReference), so a hop back costs about one pass.
- Pass 1 reads the whole source: it checks the sha256 (BundleFileSource, when an expected one is known), the
  schema_version against compat.ACCEPTED_SCHEMA_VERSIONS, the line grammar, and the seeds. A later pass stops
  at the first section after which nothing is wanted, and skips sections with nothing wanted without parsing
  their keys. A re-opened bundle is checked against pass 1 (Tap), and every later pass's per-section record
  counts must equal pass 1's.
- A pointer to a key its section doesn't hold is dangling: recorded in the result, never a failure.
- At the end, the spool is merged per section by source position into the mini-bundle (gzip, mtime 0, no file
  name, so the same inputs give the same bytes), which is re-read with bundle.stage_bundle to prove it valid.
"""

from __future__ import annotations

import enum
import gzip
import hashlib
import heapq
import io
import json
import logging
import os
import re
import shutil
import tempfile
import time
import zlib
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Protocol

from .bundle import SECTIONS, stage_bundle
from .compat import ACCEPTED_SCHEMA_VERSIONS
from .errors import BundleFormatError, InputError, SchemaVersionError, ToolError
from .model import InputFile
from .sources import open_stream

log = logging.getLogger(__name__)

SELECTION_METADATA_KEY = "clinvar_gkm_parquet.selection"
CHECKPOINT_BYTES = 64 << 20  # a re-opened stream is checked against pass 1 at every multiple of this
DEFAULT_COMPRESSLEVEL = 6
SPOOL_COMPRESSLEVEL = 1
MAX_LISTED = 10  # missing seeds named in an error
_READ_BUFFER = 1 << 20
_WRITE_CHUNK = 1 << 20

SEC_INDEX: dict[str, int] = {name: i for i, name in enumerate(SECTIONS)}
_INF = float("inf")

# The assembler's line layout (design plan §4.2; bundle.py holds the strict reader the output is checked with).
SCHEMA_LINE = re.compile(rb'  "schema_version": "([^"\\]*)"(,?)\n')
SECTION_OPEN = re.compile(rb'  "([^"\\]*)": \{\n')
# A string VALUE that starts with "#/": its opening quote follows ':' (object member), '[' or ',' (array
# element); iter_pointers checks that byte. (A lookbehind in the regex would stop `re` from searching for the
# literal prefix, about 15 times slower.) An escaped quote inside a string is preceded by a backslash, so free
# text can't match, and a match starting at one runs only to the end of that string, which holds no pointer.
_POINTER = re.compile(rb'"#/([^"/\\]+)/((?:[^"\\]|\\.)*)"')
_POINTER_SECTION = re.compile(rb'"#/([^"/\\]+)/')  # section names only: the pass-1 section map
_VALUE_START = frozenset(b":[,")


class SelectionError(ToolError):
    """A selection can't run: bad parameters, a seed the driver section doesn't hold, an inconsistent source."""


class SourceChangedError(SelectionError):
    """A later pass read a different stream than pass 1 (a changed or different file)."""


class Follow(str, enum.Enum):
    OUT = "out"
    IN = "in"
    BOTH = "both"


# ---------------------------------------------------------------------------------------------------------------
# Results


@dataclass(frozen=True)
class PassStats:
    number: int
    depth_limit: int  # the deepest level this pass could assign
    seconds: float
    bytes_read: int | None  # compressed bytes for a bundle file; None for a source without bytes
    records_read: int  # record lines streamed (parsed or skipped)
    selected: int  # records this pass added
    scanned_sections: tuple[str, ...]  # sections scanned for incoming pointers
    stopped_before: str | None  # the section the pass stopped at, or None if it read to the end


@dataclass(frozen=True)
class DanglingPointer:
    pointer: str  # "#/<section>/<key>", the target its section doesn't hold
    section: str  # the first selected record found holding it
    key: str


@dataclass(frozen=True)
class SelectionResult:
    path: Path
    size: int
    sha256: str
    schema_version: str
    source_name: str
    source_sha256: str | None  # of the bytes pass 1 read (BundleFileSource); None for other sources
    source_records: dict[str, int]  # per section, in source order
    section_counts: dict[str, int]  # selected records per section, in source order
    depth_counts: dict[int, int]  # selected records per depth
    passes: tuple[PassStats, ...]
    dangling: tuple[DanglingPointer, ...]
    parameters: dict  # what integration records; see metadata_json()
    seconds: float

    @property
    def records(self) -> int:
        return sum(self.section_counts.values())

    def metadata_json(self) -> str:
        """The value of the `clinvar_gkm_parquet.selection` key-value entry: canonical JSON, no timestamps."""
        return canonical_json(self.parameters)


def canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def ids_sha256(keys: Iterable[str]) -> str:
    """sha256 of a key set: the sorted, distinct keys, each followed by a newline, UTF-8."""
    h = hashlib.sha256()
    for k in sorted(set(keys)):
        h.update(k.encode("utf-8") + b"\n")
    return h.hexdigest()


# ---------------------------------------------------------------------------------------------------------------
# Record sources


class SectionRecords(Protocol):
    """One section of one pass. Iterate it for `(key, record)`, or skip() it; either consumes it to its end."""

    name: str
    count: int  # records consumed so far

    def __iter__(self) -> Iterator[tuple[bytes, bytes]]: ...

    def skip(self) -> None: ...


class RecordPass(Protocol):
    schema_version: str
    bytes_read: int | None

    def sections(self) -> Iterator[SectionRecords]: ...


class RecordSource(Protocol):
    """Gives the same record stream on every pass. `open_pass(n)` is a context manager; the engine may stop
    iterating early on passes after the first (never on pass 1)."""

    name: str
    sha256: str | None

    def open_pass(self, number: int) -> AbstractContextManager[RecordPass]: ...


class IterRecordSource:
    """A RecordSource over a re-iterable producer: `records(pass_number)` returns an iterable of
    `(section, key JSON bytes, record bytes)`, each section's records together, in section order and key order,
    and the same sequence every time. The input for a replayed state (base full plus deltas)."""

    sha256 = None

    def __init__(
        self, name: str, schema_version: str, records: Callable[[int], Iterable[tuple[str, bytes, bytes]]]
    ):
        self.name = name
        self.schema_version = schema_version
        self._records = records

    def open_pass(self, number: int) -> AbstractContextManager[RecordPass]:
        return _IterPass(self, self._records(number))


class _IterPass:
    bytes_read = None

    def __init__(self, src: IterRecordSource, records: Iterable[tuple[str, bytes, bytes]]):
        self.schema_version = src.schema_version
        self._name = src.name
        self._it = iter(records)
        self.head: tuple[str, bytes, bytes] | None = None

    def __enter__(self) -> _IterPass:
        return self

    def __exit__(self, *exc) -> bool:
        close = getattr(self._it, "close", None)
        if close is not None:
            close()
        return False

    def sections(self) -> Iterator[_IterSection]:
        self.head = next(self._it, None)
        seen: set[str] = set()
        while self.head is not None:
            name = self.head[0]
            if name in seen:
                raise BundleFormatError(
                    f"{self._name}: section {name!r} appears twice (a record source gives each section's records "
                    "together)"
                )
            seen.add(name)
            sec = _IterSection(self, name)
            yield sec
            sec.skip()


class _IterSection:
    def __init__(self, p: _IterPass, name: str):
        self._p = p
        self.name = name
        self.count = 0
        self._done = False

    def __iter__(self) -> Iterator[tuple[bytes, bytes]]:
        p = self._p
        while not self._done:
            head = p.head
            if head is None or head[0] != self.name:
                self._done = True
                return
            self.count += 1
            p.head = next(p._it, None)
            yield head[1], head[2]

    def skip(self) -> None:
        for _ in self:
            pass


class Tap(io.RawIOBase):
    """Passes compressed bytes through, hashing them. Pass 1 records the sha256 of every `every`-long prefix; a
    re-opened stream (`reference` given) must give the same digests at the same offsets."""

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
                raise SourceChangedError(
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
            raise SourceChangedError(
                f"the re-opened source differs from the pass-1 stream: {self.pos:,d} bytes, sha256 "
                f"{self.sha.hexdigest()}; pass 1 read {ref.pos:,d} bytes, sha256 {ref.sha.hexdigest()}"
            )


def key_end(line: bytes) -> int:
    """Index of the closing quote of the JSON key that starts at line[4] (for keys with escapes)."""
    i, n = 5, len(line)
    while i < n:
        c = line[i]
        if c == 0x5C:  # backslash
            i += 2
        elif c == 0x22:  # quote
            return i
        else:
            i += 1
    return -1


class BundleFileSource:
    """A bundle file (local path or https:// URL) as a RecordSource. Pass 1 opens `inp`; later passes call
    `reopen(pass_number)` for a fresh stream of the same bytes (default: open `inp` again), and are checked
    against pass 1 with a Tap. After pass 1, `sha256` and `size` are those of the bytes read; a mismatch with
    `inp.expected_sha256` raises InputError."""

    def __init__(
        self,
        inp: InputFile,
        *,
        reopen: Callable[[int], BinaryIO] | None = None,
        checkpoint_bytes: int = CHECKPOINT_BYTES,
    ):
        self.input = inp
        self.name = inp.name
        self.sha256: str | None = None
        self.size: int | None = None
        self._reopen = reopen
        self._every = checkpoint_bytes
        self._reference: Tap | None = None

    def open_pass(self, number: int) -> AbstractContextManager[RecordPass]:
        return _FilePass(self, number)


class _FilePass:
    def __init__(self, src: BundleFileSource, number: int):
        self._src = src
        self.number = number
        self.lineno = 0
        self.complete = False
        self.schema_version = ""
        self._more = False

    @property
    def bytes_read(self) -> int:
        return self._tap.pos

    def __enter__(self) -> _FilePass:
        src = self._src
        if self.number == 1 or src._reopen is None:
            raw = open_stream(src.input)
        else:
            raw = src._reopen(self.number)
        self._raw = raw
        try:
            self._tap = Tap(raw, src._every, src._reference if self.number > 1 else None)
            self._f = io.BufferedReader(gzip.GzipFile(fileobj=self._tap, mode="rb"), buffer_size=_READ_BUFFER)
            with self._errors():
                self._header()
        except BaseException:
            raw.close()
            raise
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        try:
            if exc_type is None and not self.complete:
                if self.number == 1:
                    raise SelectionError("internal: pass 1 must read the whole bundle")
                self._tap.settle()
        finally:
            self._raw.close()
        return False

    def fail(self, msg: str, section: str | None = None) -> BundleFormatError:
        where = f"{self._src.name}: line {self.lineno}"
        if section:
            where += f", section {section}"
        return BundleFormatError(f"{where}: {msg}")

    @contextmanager
    def _errors(self):
        try:
            yield
        except (EOFError, gzip.BadGzipFile, zlib.error) as e:
            self._checksum_first(e)
            raise BundleFormatError(f"{self._src.name}: not a complete gzip stream: {e}") from e
        except (BundleFormatError, SchemaVersionError) as e:
            self._checksum_first(e)
            raise

    def _checksum_first(self, e: Exception) -> None:
        """A wrong or corrupt file can fail the grammar or gzip first; a known checksum mismatch is the error."""
        inp = self._src.input
        if self.number != 1 or inp.expected_sha256 is None:
            return
        try:
            self._tap.drain()
        except (OSError, EOFError, zlib.error, ToolError):
            pass
        _check_sha(inp, self._tap.sha.hexdigest(), cause=f"; the content also fails to read ({e})")

    def _header(self) -> None:
        f = self._f
        line = f.readline()
        self.lineno = 1
        if line != b"{\n":
            raise self.fail(f"expected '{{', got {_short(line)}")
        line = f.readline()
        self.lineno = 2
        m = SCHEMA_LINE.fullmatch(line)
        if not m:
            raise SchemaVersionError(
                f'{self._src.name}: line 2 must be \'  "schema_version": "<version>",\', got {_short(line)}'
            )
        self.schema_version = m.group(1).decode("utf-8", "replace")
        _check_schema_version(self._src.name, self.schema_version)
        self._more = bool(m.group(2))

    def sections(self) -> Iterator[_FileSection]:
        f = self._f
        seen: set[str] = set()
        with self._errors():
            while self._more:
                line = f.readline()
                self.lineno += 1
                m = SECTION_OPEN.fullmatch(line)
                if not m:
                    if line in (b"}\n", b"}"):
                        raise self.fail("trailing ',' after the last section")
                    raise self.fail(f"expected a section to open, got {_short(line)}")
                name = m.group(1).decode("utf-8", "replace")
                if name not in SEC_INDEX:
                    raise self.fail(f"unknown section {name!r} (the 1.1.0 bundle has: {', '.join(SECTIONS)})")
                if name in seen:
                    raise self.fail(f"section {name!r} appears twice")
                seen.add(name)
                sec = _FileSection(self, name)
                yield sec
                sec.skip()
            line = f.readline()
            self.lineno += 1
            if line != b"}\n":
                raise self.fail(f"expected the closing '}}' of the bundle, got {_short(line)}")
            if f.read(1):
                raise self.fail("trailing data after the closing '}'")
            self._tap.drain()
        self.complete = True
        src = self._src
        if self.number == 1:
            src.sha256 = self._tap.sha.hexdigest()
            src.size = self._tap.pos
            src._reference = self._tap
            _check_sha(src.input, src.sha256)


class _FileSection:
    def __init__(self, p: _FilePass, name: str):
        self._p = p
        self.name = name
        self.count = 0
        self._comma = True  # the previous record line ended with ',' (true before the first)
        self._done = False

    def __iter__(self) -> Iterator[tuple[bytes, bytes]]:
        return self._run(True)

    def skip(self) -> None:
        for _ in self._run(False):
            pass

    def _run(self, parse: bool) -> Iterator[tuple[bytes, bytes]]:
        if self._done:
            return
        p = self._p
        with p._errors():
            for line in p._f:
                p.lineno += 1
                if line.startswith(b'    "'):
                    if not self._comma:
                        raise p.fail("missing ',' after the previous record", self.name)
                    if line.endswith(b",\n"):
                        end, self._comma = -2, True
                    elif line.endswith(b"\n"):
                        end, self._comma = -1, False
                    else:
                        raise p.fail("truncated: the last line has no newline", self.name)
                    self.count += 1
                    if parse:
                        q = line.find(b'"', 5)
                        if q > 0 and line.find(b"\\", 5, q) >= 0:
                            q = key_end(line)
                        if q < 0 or line[q + 1 : q + 3] != b": ":
                            raise p.fail(f"malformed record line {_short(line)}", self.name)
                        record = line[q + 3 : end]
                        if not (record.startswith(b"{") and record.endswith(b"}")):
                            raise p.fail("the record is not a JSON object on one line", self.name)
                        yield line[4 : q + 1], record
                    continue
                if line == b"  },\n" or line == b"  }\n":
                    if self.count == 0:
                        raise p.fail("empty section (the assembler never writes one)", self.name)
                    if self._comma:
                        raise p.fail("trailing ',' after the section's last record", self.name)
                    p._more = line == b"  },\n"
                    self._done = True
                    return
                raise p.fail(f"expected a record line or the section close, got {_short(line)}", self.name)
            raise p.fail("truncated: the file ends inside the section", self.name)


def _short(line: bytes) -> str:
    text = line.decode("utf-8", "replace")
    return repr(text if len(text) <= 120 else text[:120] + "...")


def _check_schema_version(name: str, version: str) -> None:
    if version not in ACCEPTED_SCHEMA_VERSIONS:
        raise SchemaVersionError(
            f"{name}: bundle schema_version {version!r} is not supported; this tool reads "
            f"{', '.join(ACCEPTED_SCHEMA_VERSIONS)}. See the compatibility matrix in the README for the tool "
            "release that reads it."
        )


def _check_sha(inp: InputFile, sha: str, cause: str = "") -> None:
    if inp.expected_sha256 is None:
        if not cause:
            log.warning(
                "%s: no published sha256 found; computed sha256 %s (recorded, not verified)", inp.name, sha
            )
        return
    if sha != inp.expected_sha256.lower():
        raise InputError(
            f"{inp.name}: sha256 mismatch: computed {sha}, expected {inp.expected_sha256} "
            f"(from {inp.sha256_source}){cause}. The file is corrupt, incomplete or not the published one; "
            "nothing was selected."
        )


# ---------------------------------------------------------------------------------------------------------------
# Keys and pointers


def _canon(inner: bytes) -> bytes:
    """The canonical form of a key's JSON string body (between the quotes): json.dumps(ensure_ascii=False).
    A body with no backslash is already canonical; one with escapes is decoded and re-encoded."""
    if b"\\" not in inner:
        return inner
    try:
        text = json.loads(b'"' + inner + b'"')
    except ValueError as e:
        raise BundleFormatError(f"malformed JSON string {inner[:120]!r}: {e}") from e
    return json.dumps(text, ensure_ascii=False).encode("utf-8")[1:-1]


def _key_body(key: str) -> bytes:
    return json.dumps(key, ensure_ascii=False).encode("utf-8")[1:-1]


def _key_text(body: bytes) -> str:
    return json.loads(b'"' + body + b'"')


def iter_pointers(record: bytes) -> Iterator[tuple[bytes, bytes]]:
    """(section name, key body as written) for every `"#/<section>/<key>"` string value in a record, in order."""
    for m in _POINTER.finditer(record):
        start = m.start()
        if start and record[start - 1] in _VALUE_START:
            yield m.group(1), m.group(2)


def parse_pointers(record: bytes) -> list[tuple[int, bytes]]:
    """Every `"#/<section>/<key>"` string value in a record: (section index, canonical key body), in order.
    A pointer to a section the bundle schema doesn't have fails."""
    out = []
    for section, body in iter_pointers(record):
        si = SEC_INDEX.get(section.decode("utf-8", "replace"))
        if si is None:
            text = (section + b"/" + body).decode("utf-8", "replace")
            raise BundleFormatError(f'pointer to an unknown section: "#/{text}"')
        out.append((si, _canon(body)))
    return out


# ---------------------------------------------------------------------------------------------------------------
# Spool


class _Spool:
    """Selected records, per (pass, section), in source order: `<ordinal>\\t<key>\\t<record>\\n`, gzip level 1.
    Each pass appends a sorted run per section; the output merges the runs by ordinal."""

    def __init__(self, root: Path):
        self.root = root
        self.runs: dict[int, list[Path]] = {}
        self._writers: dict[int, tuple[gzip.GzipFile, bytearray]] = {}
        self._pass = 0

    def start_pass(self, number: int) -> None:
        self._pass = number

    def add(self, si: int, ordinal: int, key: bytes, record: bytes) -> None:
        w = self._writers.get(si)
        if w is None:
            path = self.root / f"p{self._pass:03d}-{SECTIONS[si]}.tsv.gz"
            self.runs.setdefault(si, []).append(path)
            w = self._writers[si] = (
                gzip.GzipFile(path, "wb", compresslevel=SPOOL_COMPRESSLEVEL, mtime=0),
                bytearray(),
            )
        gz, buf = w
        buf += b"%d\t" % ordinal
        buf += key
        buf += b"\t"
        buf += record
        buf += b"\n"
        if len(buf) >= _WRITE_CHUNK:
            gz.write(buf)
            buf.clear()

    def end_pass(self) -> None:
        for gz, buf in self._writers.values():
            if buf:
                gz.write(buf)
            gz.close()
        self._writers.clear()

    def merged(self, si: int) -> Iterator[tuple[int, bytes, bytes]]:
        return heapq.merge(*(self._read(p) for p in self.runs.get(si, ())))

    @staticmethod
    def _read(path: Path) -> Iterator[tuple[int, bytes, bytes]]:
        with gzip.open(path, "rb") as fh:
            for line in fh:
                o, key, record = line[:-1].split(b"\t", 2)
                yield int(o), key, record

    def drop(self, si: int) -> None:
        for p in self.runs.pop(si, ()):
            p.unlink()


class _HashingWriter(io.RawIOBase):
    def __init__(self, fh: BinaryIO):
        self._fh = fh
        self.sha = hashlib.sha256()
        self.size = 0

    def writable(self) -> bool:
        return True

    def write(self, b) -> int:
        self.sha.update(b)
        self.size += len(b)
        return self._fh.write(b)


# ---------------------------------------------------------------------------------------------------------------
# The engine


def select(
    source: RecordSource | InputFile,
    seeds: Mapping[str, Iterable[str]],
    out_path: Path | str,
    *,
    follow: Follow | str = Follow.OUT,
    max_depth: int | None = None,
    sections: Iterable[str] | None = None,
    provenance: Mapping[str, object] | None = None,
    work_dir: Path | str | None = None,
    validate: bool = True,
    compresslevel: int = DEFAULT_COMPRESSLEVEL,
) -> SelectionResult:
    """Select the seeds and the records they reach, and write them as a verbatim mini-bundle at `out_path`.
    See the module docstring. Writes nothing at `out_path` unless the whole selection succeeds."""
    t0 = time.monotonic()
    try:
        follow = Follow(follow)
    except ValueError as e:
        raise SelectionError(f"follow must be out, in or both, not {follow!r}") from e
    if max_depth is not None and (not isinstance(max_depth, int) or max_depth < 0):
        raise SelectionError(f"max_depth must be a non-negative integer or None, not {max_depth!r}")
    allowed_list = _allowed(sections)
    seed_bodies = _seeds(seeds, allowed_list)
    if isinstance(source, InputFile):
        source = BundleFileSource(source)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    root = Path(work_dir) if work_dir is not None else out_path.parent
    root.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=".select-", dir=root))
    partial = out_path.parent / f".{out_path.name}.partial"
    try:
        engine = _Engine(source, seed_bodies, follow, max_depth, allowed_list, _Spool(work))
        engine.run()
        size, sha, digests = engine.write(partial, compresslevel)
        if validate:
            _validate(partial, out_path.name, sha, size, engine.schema_version, digests, work / "validate")
        os.replace(partial, out_path)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    finally:
        shutil.rmtree(work, ignore_errors=True)

    params: dict = {
        "follow": follow.value,
        "max_depth": max_depth,
        "sections": None if sections is None else [SECTIONS[i] for i in allowed_list],
        "seeds": {
            SECTIONS[si]: {"count": len(keys), "sha256": ids_sha256(_key_text(k) for k in keys)}
            for si, keys in sorted(seed_bodies.items())
        },
        "selected_records": engine.selected,
        "selected_sha256": sha,
    }
    for k, v in (provenance or {}).items():
        if k in params:
            raise SelectionError(f"provenance key {k!r} is set by the engine")
        params[k] = v
    order = [SECTIONS[si] for si in engine.order]
    result = SelectionResult(
        path=out_path,
        size=size,
        sha256=sha,
        schema_version=engine.schema_version,
        source_name=source.name,
        source_sha256=source.sha256,
        source_records={s: engine.counts[SEC_INDEX[s]] for s in order},
        section_counts={s: len(engine.depth[SEC_INDEX[s]]) for s in order if engine.depth[SEC_INDEX[s]]},
        depth_counts=dict(sorted(Counter(d for sec in engine.depth for d, _ in sec.values()).items())),
        passes=tuple(engine.passes),
        dangling=tuple(engine.dangling_list()),
        parameters=params,
        seconds=time.monotonic() - t0,
    )
    log.info(
        "selected %d records in %d sections (%d passes, %.1f s): %s, %d bytes",
        result.records,
        len(result.section_counts),
        len(result.passes),
        result.seconds,
        out_path,
        size,
    )
    if result.dangling:
        log.warning("%d dangling pointer(s) (a full bundle should have none)", len(result.dangling))
    return result


def _allowed(sections: Iterable[str] | None) -> list[int]:
    if sections is None:
        return list(range(len(SECTIONS)))
    if isinstance(sections, str):
        raise SelectionError("sections must be a list of section names, not a string")
    names = list(sections)
    unknown = [s for s in names if s not in SEC_INDEX]
    if unknown:
        raise SelectionError(
            f"unknown section(s) {', '.join(map(repr, unknown))}; the bundle has: {', '.join(SECTIONS)}"
        )
    if not names:
        raise SelectionError("sections is empty")
    return sorted({SEC_INDEX[s] for s in names})


def _seeds(seeds: Mapping[str, Iterable[str]], allowed: list[int]) -> dict[int, set[bytes]]:
    out: dict[int, set[bytes]] = {}
    for section, keys in seeds.items():
        si = SEC_INDEX.get(section)
        if si is None:
            raise SelectionError(f"unknown driver section {section!r}; the bundle has: {', '.join(SECTIONS)}")
        if si not in allowed:
            raise SelectionError(f"the driver section {section!r} must be one of the selected sections")
        if isinstance(keys, (str, bytes)):
            raise SelectionError(f"the keys for {section!r} must be a collection of strings, not one string")
        bodies = set()
        for k in keys:
            if not isinstance(k, str):
                raise SelectionError(f"driver key {k!r} in {section!r} is not a string")
            bodies.add(_key_body(k))
        if bodies:
            out[si] = bodies
    if not out:
        raise SelectionError("no driver keys: nothing to select")
    return out


class _Engine:
    """The traversal state. Nodes are (section index, canonical key body).

    - depth[si][key] = (depth, ordinal): selected records, their shortest hop count and position in the section.
    - want[si][key] = [depth, referrer, visit]: out-pointer targets not selected yet, with the shortest depth
      known so far, the selected record that points to it (None for a seed) and the section visit it was last
      lowered in (an entry present for a whole visit of its section and not found there is dangling).
    - in_done: levels whose incoming pointers have all been scanned for while the level was complete.
    """

    def __init__(self, source, seeds, follow, max_depth, allowed, spool):
        n = len(SECTIONS)
        self.source = source
        self.seeds = [seeds.get(i, set()) for i in range(n)]
        self.follow_out = follow in (Follow.OUT, Follow.BOTH)
        self.follow_in = follow in (Follow.IN, Follow.BOTH)
        self.max_depth = max_depth
        self.allowed = frozenset(allowed)
        self.spool = spool
        self.depth: list[dict[bytes, tuple[int, int]]] = [{} for _ in range(n)]
        self.want: list[dict[bytes, list]] = [{} for _ in range(n)]
        for si, keys in seeds.items():
            for k in keys:
                self.want[si][k] = [0, None, 0]
        self.level_secs: dict[int, set[int]] = {}
        self.in_done: set[int] = set()
        self.points_to: list[set[int]] = [set() for _ in range(n)]  # learned in pass 1 (in/both)
        self.learned: list[set[bytes]] = [set() for _ in range(n)]  # section names pass 1 saw pointers to
        self.order: list[int] = []
        self.counts: dict[int, int] = {}
        self.schema_version = ""
        self.passes: list[PassStats] = []
        self.dangling: dict[tuple[int, bytes], tuple[int, bytes]] = {}
        self.selected = 0
        self.visit = 0

    # -- planning ---------------------------------------------------------------------------------------------

    def _cap(self, d: int) -> int:
        return d if self.max_depth is None else min(d, self.max_depth)

    def _plan(self):
        """(depth limit, scan sections, required in-level) for the next pass, or None when nothing is left.

        A = the deepest level all of whose records are selected: no pending out-target at depth <= A, and (in/both)
        every level below A scanned for incoming pointers. Depth A + 2 is then exact for anything found."""
        while True:
            min_want = min((e[0] for w in self.want for e in w.values()), default=_INF)
            pending_in = _INF
            if self.follow_in:
                for j in sorted(self.level_secs):
                    if j not in self.in_done and (self.max_depth is None or j + 1 <= self.max_depth):
                        pending_in = j
                        break
            if min_want == _INF and pending_in == _INF:
                return None
            closed = min(min_want - 1, pending_in)
            limit = self._cap(int(closed) + 2)
            required = int(pending_in) if pending_in <= closed else None
            scan: set[int] = set()
            if required is not None:
                targets = self.level_secs[required]
                scan = {s for s in self.allowed if self.points_to[s] & targets}
                if not scan:  # nothing can point into this level
                    self.in_done.add(required)
                    continue
            return limit, scan, required

    def _needles(self, first: bool, limit: int, required: int | None) -> tuple[bytes, ...]:
        """Substrings a record must hold to point to a record of depth < limit not yet scanned for (pass 1: the
        seeds)."""
        secs: set[int] = {si for si, keys in enumerate(self.seeds) if keys} if first else set()
        for j in (required, limit - 1):
            if j is not None:
                secs |= self.level_secs.get(j, set())
        return tuple(b'"#/' + SECTIONS[s].encode() + b"/" for s in sorted(secs))

    # -- passes -----------------------------------------------------------------------------------------------

    def run(self) -> None:
        # Pass 1 reads everything: checksum, grammar, counts and seeds. It scans every allowed section for
        # pointers into the seeds (known up front) when following incoming pointers.
        first_scan = set(self.allowed) if self.follow_in and self._cap(1) >= 1 else set()
        self._pass(1, self._cap(1), first_scan, None)
        missing = [(si, k) for si, keys in enumerate(self.seeds) for k in keys if k not in self.depth[si]]
        if missing:
            listed = ", ".join(f"{SECTIONS[si]}:{_key_text(k)}" for si, k in sorted(missing)[:MAX_LISTED])
            more = f" and {len(missing) - MAX_LISTED} more" if len(missing) > MAX_LISTED else ""
            raise SelectionError(
                f"{len(missing)} driver key(s) not in {self.source.name}: {listed}{more}; nothing was written"
            )
        if first_scan:
            self.in_done.add(0)
            for si, names in enumerate(self.learned):
                found = (x.decode("utf-8", "replace") for x in names)
                self.points_to[si] = {SEC_INDEX[n] for n in found if n in SEC_INDEX}
        while True:
            plan = self._plan()
            if plan is None:
                return
            limit, scan, required = plan
            self._pass(len(self.passes) + 1, limit, scan, required)
            if required is not None:
                self.in_done.add(required)

    def _needed_from(self, pos: int, limit: int, scan: set[int]) -> bool:
        for si in self.order[pos:]:
            if si in scan or any(e[0] <= limit for e in self.want[si].values()):
                return True
        return False

    def _pass(self, number: int, limit: int, scan: set[int], required: int | None) -> None:
        t0 = time.monotonic()
        first = number == 1
        needles = self._needles(first, limit, required)
        selected_before = self.selected
        records_read = 0
        stopped = None
        self.spool.start_pass(number)
        try:
            with self.source.open_pass(number) as p:
                version = p.schema_version
                if first:
                    _check_schema_version(self.source.name, version)
                    self.schema_version = version
                elif version != self.schema_version:
                    raise SourceChangedError(
                        f"{self.source.name}: pass {number} reads schema_version {version}, pass 1 read "
                        f"{self.schema_version}"
                    )
                pos = 0
                for sec in p.sections():
                    si = SEC_INDEX.get(sec.name)
                    if si is None:
                        raise BundleFormatError(f"{self.source.name}: unknown section {sec.name!r}")
                    if first:
                        if si in self.counts:
                            raise BundleFormatError(f"{self.source.name}: section {sec.name!r} appears twice")
                        self.order.append(si)
                        self.counts[si] = 0
                    elif pos >= len(self.order) or self.order[pos] != si:
                        raise SourceChangedError(
                            f"{self.source.name}: pass {number} reads section {sec.name} where pass 1 read "
                            f"{SECTIONS[self.order[pos]] if pos < len(self.order) else 'the end'}"
                        )
                    if not first and not self._needed_from(pos, limit, scan):
                        stopped = sec.name
                        break
                    pos += 1
                    if si in scan or any(e[0] <= limit for e in self.want[si].values()):
                        self._section(sec, si, first, limit, si in scan, needles)
                    else:
                        sec.skip()
                    records_read += sec.count
                    if first:
                        self.counts[si] = sec.count
                    elif sec.count != self.counts[si]:
                        raise SourceChangedError(
                            f"{self.source.name}: pass {number} read {sec.count:,d} records in section "
                            f"{sec.name}, pass 1 read {self.counts[si]:,d}"
                        )
                if stopped is None and not first and pos != len(self.order):
                    raise SourceChangedError(f"{self.source.name}: pass {number} ends before pass 1 did")
                bytes_read = p.bytes_read
        finally:
            self.spool.end_pass()
        stats = PassStats(
            number=number,
            depth_limit=limit,
            seconds=time.monotonic() - t0,
            bytes_read=bytes_read,
            records_read=records_read,
            selected=self.selected - selected_before,
            scanned_sections=tuple(SECTIONS[s] for s in sorted(scan)),
            stopped_before=stopped,
        )
        self.passes.append(stats)
        log.info(
            "pass %d (depth <= %d): %.1f s, %s bytes, %d records read%s; +%d selected, %d pending",
            number,
            limit,
            stats.seconds,
            f"{bytes_read:,d}" if bytes_read is not None else "-",
            records_read,
            f", stopped before {stopped}" if stopped else "",
            stats.selected,
            sum(len(w) for w in self.want),
        )

    def _section(self, sec, si: int, first: bool, limit: int, do_scan: bool, needles: tuple[bytes, ...]) -> None:
        self.visit += 1
        visit = self.visit
        depth = self.depth
        depth_s = depth[si]
        want_s = self.want[si]
        seeds = self.seeds
        follow_in = self.follow_in
        learn = self.learned[si] if first and do_scan else None
        ordinal = -1
        for key, record in sec:
            ordinal += 1
            k = key[1:-1]
            if b"\\" in k:
                k = _canon(k)
            have = depth_s.get(k)
            if have is not None:
                if have[1] != ordinal:
                    raise BundleFormatError(
                        f"{self.source.name}: section {sec.name}: key {_key_text(k)} appears twice, or the source "
                        "changed between passes"
                    )
                continue
            e = want_s.get(k) if want_s else None
            d = e[0] if e is not None else _INF
            if d > limit and not do_scan:
                continue
            ptrs = None
            if do_scan or (follow_in and d <= limit):
                if learn is not None:
                    learn.update(_POINTER_SECTION.findall(record))
                if e is None and not any(n in record for n in needles):
                    continue
                ptrs = parse_pointers(record)
                for ti, tk in ptrs:
                    dd = depth[ti].get(tk)
                    if dd is not None:
                        if dd[0] + 1 < d:
                            d = dd[0] + 1
                    elif first and tk in seeds[ti] and d > 1:
                        d = 1
            if d > limit:
                continue
            self._select(si, k, key, record, ordinal, int(d), ptrs, visit)
        # Out-targets wanted for this whole visit and not found: dangling (seeds are checked after pass 1).
        for k in [k for k, e in want_s.items() if e[0] <= limit and e[2] < visit and e[1] is not None]:
            e = want_s.pop(k)
            self.dangling.setdefault((si, k), e[1])

    def _select(self, si, k, key, record, ordinal, d, ptrs, visit) -> None:
        self.depth[si][k] = (d, ordinal)
        self.want[si].pop(k, None)
        self.level_secs.setdefault(d, set()).add(si)
        self.spool.add(si, ordinal, key, record)
        self.selected += 1
        if not self.follow_out or (self.max_depth is not None and d + 1 > self.max_depth):
            return
        if ptrs is None:
            ptrs = parse_pointers(record)
        node = (si, k)
        for ti, tk in ptrs:
            if ti not in self.allowed or tk in self.depth[ti]:
                continue
            w = self.want[ti]
            e = w.get(tk)
            if e is None:
                w[tk] = [d + 1, node, visit]
            elif d + 1 < e[0]:
                e[0], e[1], e[2] = d + 1, node, visit

    def dangling_list(self) -> list[DanglingPointer]:
        out = []
        for (ti, tk), (si, sk) in sorted(self.dangling.items()):
            out.append(DanglingPointer(f"#/{SECTIONS[ti]}/{_key_text(tk)}", SECTIONS[si], _key_text(sk)))
        return out

    # -- output -----------------------------------------------------------------------------------------------

    def write(self, path: Path, compresslevel: int) -> tuple[int, str, dict[str, tuple[int, str]]]:
        """Merge the spool into the mini-bundle. Returns (size, sha256, {section: (records, sha256 of the
        staging lines `key\\trecord\\n`)}) for the validation."""
        digests: dict[str, tuple[int, str]] = {}
        with open(path, "wb") as fh:
            hw = _HashingWriter(fh)
            with gzip.GzipFile(filename="", mode="wb", fileobj=hw, compresslevel=compresslevel, mtime=0) as gz:
                buf = bytearray(b'{\n  "schema_version": "' + self.schema_version.encode() + b'"')
                for si in self.order:
                    if not self.depth[si]:
                        continue
                    name = SECTIONS[si]
                    buf += b',\n  "' + name.encode() + b'": {\n'
                    h = hashlib.sha256()
                    n = 0
                    for _, key, record in self.spool.merged(si):
                        if n:
                            buf += b",\n"
                        buf += b"    " + key + b": " + record
                        h.update(key + b"\t" + record + b"\n")
                        n += 1
                        if len(buf) >= _WRITE_CHUNK:
                            gz.write(buf)
                            buf.clear()
                    buf += b"\n  }"
                    self.spool.drop(si)
                    if n != len(self.depth[si]):
                        raise SelectionError(
                            f"internal: section {name}: spooled {n} records, selected {len(self.depth[si])}"
                        )
                    digests[name] = (n, h.hexdigest())
                buf += b"\n}\n"
                gz.write(buf)
        return hw.size, hw.sha.hexdigest(), digests


def _validate(
    path: Path, name: str, sha: str, size: int, version: str, digests: dict[str, tuple[int, str]], work: Path
) -> None:
    """Re-read the mini-bundle with stage_bundle: each staged section must hold exactly what was written."""
    inp = InputFile(str(path), name, None, sha, "explicit", size)
    try:
        staged = stage_bundle(inp, work)
    except ToolError as e:
        raise SelectionError(f"the written mini-bundle does not pass bundle.stage_bundle: {e}") from e
    try:
        if staged.schema_version != version or set(staged.sections) != set(digests):
            raise SelectionError("bundle.stage_bundle staged different sections than were written")
        for sec, (n, digest) in digests.items():
            st = staged.sections[sec]
            h = hashlib.sha256()
            with gzip.open(st.path, "rb") as fh:
                for chunk in iter(lambda: fh.read(_READ_BUFFER), b""):
                    h.update(chunk)
            if st.records != n or h.hexdigest() != digest:
                raise SelectionError(
                    f"section {sec}: bundle.stage_bundle staged different records than were written"
                )
    finally:
        shutil.rmtree(work, ignore_errors=True)
