"""Stream one bundle once, section by section (design plan §4.2, §5).

Owned by the input-side work package. Layout is a property of the file, so `stream_bundle` chooses a reader from
the first lines (`sniff_layout`); either way it hands every record to a sink as it is read:

- The line layout: one record per line, read by a strict grammar and streamed, so its size doesn't matter, and its
  text is what `rebuild` writes back byte for byte.
- Any other JSON (pretty-printed, minified): `json.load`, in memory, so a file must fit in memory. It is turned
  into the canonical line layout, every record's keys sorted, and read by the same grammar. Its file bytes aren't
  kept: the promise is JSON-equal. A duplicate key or a non-JSON number stops it.

The caller says how to read a bundle's root with `RootRules`: which property holds the version, and, once the
version is known, which collections the bundle may hold (`BundleShape`). `peek_version` reads only the first lines,
for a caller that needs the version before it can start. Fails (BundleFormatError / SchemaVersionError) on any line
it can't classify, an unknown or repeated section, a broken trailing-comma sequence, trailing garbage or a
truncated file; fails (InputError) when the computed sha256 differs from the expected one. A file is gzip when it
is named .gz or starts with gzip's magic bytes.

The sink sees `begin(version)` ("" for a bundle with no version), which may refuse the version before its shape is
looked at, then per section `open_section(name)`, which returns the function each of the section's records is
passed to as `(key as JSON bytes, record bytes)`, and `close_section(name)`; and `root_value(name, JSON text)` for
each root value that is neither the version nor a collection.

With `text_digests` (a lossless build, ADR 0005), the pass also hashes the uncompressed text, of the line layout:
the whole of it (StreamedBundle.text_sha256) and, per section, its record lines exactly as written (each record
line with its `,` and newline; `section_text_sha256`), which the sink receives through
`section_text(name, sha256)` just before `close_section(name)`. The rest of the text is fixed by the grammar below,
so the two together pin the text byte for byte. For a file read by the general reader, that text is the canonical
one.

The grammar:

    {
      "<version property>": "<version>",  <- optional; no trailing comma when nothing follows
      "<section>": {                      <- a collection with records
        "<key>": <record>,                <- four spaces, JSON key, ": ", compact JSON record
        "<key>": <record>                 <- the section's last record has no comma
      },                                  <- "  }" without a comma closes the last root member
      "<empty section>": {},              <- a collection with no records
      "<value>": <compact JSON>,          <- a root value, on one line
      ...
    }

Duplicate keys within a section are detected when a staged section is loaded into DuckDB (one GROUP BY per section,
build.load_section), not here: an in-memory key set for the largest section (27 M keys) would cost gigabytes of RAM
in Python.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import logging
import re
import shutil
import tempfile
import zlib
from collections.abc import Callable
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import BinaryIO, Protocol

from .errors import BundleFormatError, InputError, SchemaVersionError, ToolError
from .model import InputFile, StagedBundle, StagedSection, StreamedBundle
from .transport import open_stream

log = logging.getLogger(__name__)

LINE_LAYOUT, JSON_LAYOUT = "line", "json"
ROOT_VALUES_TABLE = "root_values"  # the table of a bundle's root values, when it has any
STAGING_SUFFIX = ".tsv.gz"
STAGING_COMPRESSLEVEL = 1
_WRITE_CHUNK = 1 << 20  # bytes buffered before each gzip write
_READ_BUFFER = 1 << 20  # decompressed read buffer
_HEADER_BYTES = 1 << 20  # what a reader looks at to choose: enough for a file's first record line, or to tell

_STRING_LINE = re.compile(rb'  "([^"\\]*)": "([^"\\]*)"(,?)\n')
_ROOT_VALUE_LINE = re.compile(rb'  ("(?:[^"\\]|\\.)*"): (.+?)(,?)\n')
_FIRST_RECORD = re.compile(rb'    "(?:[^"\\]|\\.)*": \{.*\},?')
_SECTION_OPEN = re.compile(rb'  "([^"\\]*)": \{\n')

# Grammar states.
_TOP = 0  # after `"schema_version": "...",` or `  },`: a section must open
_FIRST = 1  # a section just opened: a record must follow
_RECORDS = 2  # after a record: another record (if it had a comma) or the section close
_END = 3  # the closing `}` of the bundle must follow
_DONE = 4  # nothing may follow


class _HashingReader(io.RawIOBase):
    """Passes bytes through, hashing them and counting them."""

    def __init__(self, raw: BinaryIO):
        self._raw = raw
        self.sha = hashlib.sha256()
        self.size = 0

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        n = self._raw.readinto(b)
        if n:
            self.sha.update(memoryview(b)[:n])
            self.size += n
        return n

    def drain(self) -> None:
        buf = bytearray(_READ_BUFFER)
        while self.readinto(buf):
            pass


class SectionWriter:
    """Writes one section's records in the staging format (model.py): to one file, or, with `chunk_bytes`, to
    chunk files of about that many uncompressed bytes each (`<path>.0000.tsv.gz`, `<path>.0001.tsv.gz`, ...),
    which DuckDB reads in parallel. `check`, if given, is called before each buffered write (about every MiB),
    so a caller can stop a long section promptly."""

    def __init__(
        self,
        path: Path,
        *,
        chunk_bytes: int | None = None,
        check: Callable[[], None] | None = None,
        ordinal: bool = False,
    ):
        self._path = path
        self._chunk_bytes = chunk_bytes
        self._check = check
        self._ordinal = ordinal  # prefix each line with the record's position in the section (model.py)
        self.paths: list[Path] = []
        self._fh = None
        self._gz = None
        self._in_file = 0
        self._buf = bytearray()
        self.records = 0
        self.max_line = 0

    def _open(self) -> None:
        if self._chunk_bytes is None:
            path = self._path
        else:
            path = self._path.with_name(f"{self._path.name}.{len(self.paths):04d}{STAGING_SUFFIX}")
        self.paths.append(path)
        self._fh = open(path, "wb")
        self._gz = gzip.GzipFile(
            filename="", mode="wb", compresslevel=STAGING_COMPRESSLEVEL, fileobj=self._fh, mtime=0
        )
        self._in_file = 0

    def _close_file(self) -> None:
        if self._gz is not None:
            gz, fh = self._gz, self._fh
            self._gz = self._fh = None
            try:
                gz.close()
            finally:
                fh.close()

    def _flush(self) -> None:
        if self._check is not None:
            self._check()
        if self._gz is None:
            self._open()
        self._gz.write(self._buf)
        self._in_file += len(self._buf)
        self._buf.clear()
        if self._chunk_bytes is not None and self._in_file >= self._chunk_bytes:
            self._close_file()

    def add(self, key: bytes, record: bytes) -> None:
        buf = self._buf
        start = len(buf)
        if self._ordinal:
            buf += b"%d\t" % self.records
        buf += key
        buf += b"\t"
        buf += record
        buf += b"\n"
        n = len(buf) - start
        if n > self.max_line:
            self.max_line = n
        self.records += 1
        if len(buf) >= _WRITE_CHUNK:
            self._flush()

    def close(self) -> None:
        if self._buf:
            self._flush()
        self._close_file()

    def abort(self) -> None:
        """Close without flushing (after a failure); the caller removes the files."""
        self._buf.clear()
        try:
            self._close_file()
        except OSError:
            pass


@dataclass(frozen=True)
class BundleShape:
    """What the root of a bundle may hold, besides its version."""

    collections: tuple[str, ...]  # in schema order
    other_values: bool = False  # the root may hold values that are not collections


@dataclass(frozen=True)
class RootRules:
    """How to read a bundle's root. `shape_of(version)` ("" for no version) says what it may hold."""

    version_property: str | None
    shape_of: Callable[[str], BundleShape]
    version_required: bool = False  # the version line must be there
    general_reader: bool = True  # False: the grammar reads any file, so only the line layout passes


class Sink(Protocol):
    def begin(self, schema_version: str) -> None: ...

    def open_section(self, section: str) -> Callable[[bytes, bytes], None]: ...

    def close_section(self, section: str) -> None: ...

    def root_value(self, name: str, value: bytes) -> None: ...


def staging_name(inp: InputFile) -> str:
    """The staging folder name for an input: its file name without `.json.gz`."""
    name = inp.name
    for sfx in (".json.gz", ".gz", ".json"):
        if name.endswith(sfx):
            return name[: -len(sfx)]
    return name


class _StagingSink:
    """Writes every section to `<out_dir>/<section>.tsv.gz`."""

    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.staged: dict[str, tuple[Path, int, int]] = {}
        self._writer: SectionWriter | None = None

    def begin(self, schema_version: str) -> None:
        pass

    def open_section(self, section: str):
        self._writer = SectionWriter(self.out_dir / f"{section}{STAGING_SUFFIX}")
        return self._writer.add

    def close_section(self, section: str) -> None:
        w, self._writer = self._writer, None
        w.close()
        if w.paths:  # an empty collection stages nothing
            self.staged[section] = (w.paths[0], w.records, w.max_line)

    def root_value(self, name: str, value: bytes) -> None:
        raise BundleFormatError(f"root value {name!r}: staging holds only collections")

    def abort(self) -> None:
        if self._writer is not None:
            self._writer.abort()


def stage_bundle(inp: InputFile, work_dir: Path, rules: RootRules) -> StagedBundle:
    """Stream `inp` once into `work_dir/<name>/<section>.tsv.gz` and return what was staged (the whole bundle;
    the build itself stages one section at a time, build.py).

    The files are written to a temporary folder and moved into place only after the whole bundle has parsed
    and its sha256 matched, so a failed run never leaves staging that looks complete.
    """
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    final = work_dir / staging_name(inp)
    tmp = Path(tempfile.mkdtemp(prefix=f".{final.name}.", suffix=".partial", dir=work_dir))
    sink = _StagingSink(tmp)
    try:
        try:
            streamed = stream_bundle(inp, sink, rules)
        finally:
            sink.abort()
        if final.exists():
            shutil.rmtree(final)
        tmp.rename(final)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    staged = {
        name: StagedSection(name=name, path=final / p.name, records=n, max_line_bytes=m)
        for name, (p, n, m) in sink.staged.items()
    }
    return StagedBundle(
        input=inp,
        sha256=streamed.sha256,
        size=streamed.size,
        schema_version=streamed.schema_version,
        sections=staged,
    )


def _decompressed(inp: InputFile, buffered: io.BufferedReader) -> BinaryIO:
    if inp.name.endswith(".gz") or buffered.peek(2)[:2] == b"\x1f\x8b":
        return gzip.GzipFile(fileobj=buffered, mode="rb")
    return buffered


def sniff_layout(head: bytes) -> str:
    """Which reader a file takes, from its first bytes. The line layout has `{` alone on the first line, and its
    first line at four spaces is one record, a whole object. A file with no such line (nothing but root lines) is
    in it too, and a malformed line goes to the strict grammar, which names it."""
    lines = head.split(b"\n")
    if head and lines[0] != b"{":
        return JSON_LAYOUT
    for i, line in enumerate(lines[1:], 1):
        if line.startswith(b'    "'):
            if i == len(lines) - 1:  # cut by the read: a line this long is no pretty-printer's
                return LINE_LAYOUT
            m = _FIRST_RECORD.fullmatch(line)
            return LINE_LAYOUT if m else JSON_LAYOUT
    return LINE_LAYOUT


def layout_of(inp: InputFile) -> str:
    """The layout of a file (`sniff_layout`). A cut GET for a URL."""
    raw = open_stream(inp)
    try:
        buffered = io.BufferedReader(raw, buffer_size=_READ_BUFFER)
        try:
            return sniff_layout(_decompressed(inp, buffered).read(_HEADER_BYTES))
        except (EOFError, gzip.BadGzipFile, zlib.error) as e:
            raise BundleFormatError(f"{inp.name}: not a complete gzip stream: {e}") from e
    finally:
        raw.close()


def peek_version(inp: InputFile, version_property: str) -> str | None:
    """The bundle's version from its first lines, or None if it has none; a file whose second line isn't the
    version is read whole. A file that doesn't start as a bundle fails as stream_bundle does, a wrong checksum
    first."""
    raw = open_stream(inp)
    try:
        hr = _HashingReader(raw)
        try:
            buffered = io.BufferedReader(hr, buffer_size=_READ_BUFFER)
            f = _decompressed(inp, buffered)
            head = f.read(_HEADER_BYTES)
            if not head:
                raise BundleFormatError(f"{inp.name}: line 1: empty file (expected '{{' on line 1)")
            lines = head.split(b"\n", 2)
            if lines[0] == b"{" and len(lines) > 1:
                m = _STRING_LINE.fullmatch(lines[1] + b"\n")
                if m and m.group(1).decode("utf-8", "replace") == version_property:
                    return m.group(2).decode("utf-8", "replace")
                if sniff_layout(head) == LINE_LAYOUT:
                    return None
            doc = _load_json(inp.name, head + f.read())
            value = doc.get(version_property)
            if value is not None and not isinstance(value, str):
                raise SchemaVersionError(f"{inp.name}: {version_property} is not a string: {value!r}")
            return value
        except (EOFError, gzip.BadGzipFile, zlib.error, ToolError) as e:
            if inp.expected_sha256 is not None:
                try:
                    hr.drain()
                except (OSError, EOFError, zlib.error):
                    pass
                _check_sha(inp, hr.sha.hexdigest(), cause=f"; the content also fails to parse ({e})")
            if isinstance(e, ToolError):
                raise
            raise BundleFormatError(f"{inp.name}: not a complete gzip stream: {e}") from e
    finally:
        raw.close()


def _short(line: bytes) -> str:
    text = line.decode("utf-8", "replace")
    return repr(text if len(text) <= 120 else text[:120] + "...")


class _Prefixed(io.RawIOBase):
    """Bytes already read, then the rest of the stream: the reader chosen after a look at the head."""

    def __init__(self, head: bytes, rest: BinaryIO):
        self._head = memoryview(head)
        self._rest = rest

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        if self._head:
            n = min(len(b), len(self._head))
            b[:n] = self._head[:n]
            self._head = self._head[n:]
            return n
        return self._rest.readinto(b)


def _no_duplicates(pairs: list[tuple[str, object]]) -> dict:
    out = dict(pairs)
    if len(out) != len(pairs):
        keys = [k for k, _ in pairs]
        raise ValueError(f"duplicate key(s) {sorted({k for k in keys if keys.count(k) > 1})}")
    return out


def _no_constant(name: str):
    raise ValueError(f"{name} is not JSON")


def _load_json(name: str, text: bytes) -> dict:
    try:
        doc = json.loads(text, object_pairs_hook=_no_duplicates, parse_constant=_no_constant)
    except ValueError as e:
        raise BundleFormatError(f"{name}: not a JSON bundle: {e}") from e
    if not isinstance(doc, dict):
        raise BundleFormatError(f"{name}: the root of a bundle is a JSON object, not {type(doc).__name__}")
    return doc


def _compact(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _canonical_text(name: str, doc: dict, rules: RootRules) -> bytes:
    """A bundle's JSON as the line layout: the version, then the collections in schema order, then the root values
    in file order; every record's keys sorted. What the general reader hands to the grammar."""
    version = doc.get(rules.version_property) if rules.version_property else None
    if version is not None and not isinstance(version, str):
        raise SchemaVersionError(f"{name}: {rules.version_property} is not a string: {version!r}")
    if version is None and rules.version_required:
        raise SchemaVersionError(
            f"{name}: no {rules.version_property!r} at the root. See the compatibility matrix in the README."
        )
    shape = rules.shape_of(version or "")
    members: list[bytes] = []
    if version is not None:
        members.append(b"  " + _compact(rules.version_property) + b": " + _compact(version))
    for section in shape.collections:
        if section not in doc:
            continue
        records = doc[section]
        if not isinstance(records, dict):
            raise BundleFormatError(f"{name}: collection {section!r} is not an object")
        if not records:
            members.append(b"  " + _compact(section) + b": {}")
            continue
        lines = []
        for key, record in records.items():
            if not isinstance(record, dict):
                raise BundleFormatError(f"{name}, section {section}, key {key}: the record is not a JSON object")
            lines.append(b"    " + _compact(key) + b": " + _compact(record))
        members.append(b"  " + _compact(section) + b": {\n" + b",\n".join(lines) + b"\n  }")
    for key, value in doc.items():
        if key != rules.version_property and key not in shape.collections:
            members.append(b"  " + _compact(key) + b": " + _compact(value))
    return b"{\n" + b",\n".join(members) + (b"\n}\n" if members else b"}\n")


def stream_bundle(inp: InputFile, sink: Sink, rules: RootRules, *, text_digests: bool = False) -> StreamedBundle:
    """Read `inp` once, passing every record to `sink` (module docstring), and check its sha256 at the end.
    `text_digests`: also hash the text, whole and per section (module docstring).

    A sink that fails raises a ToolError (e.g. a build failure downstream). Then, as for a grammar error, when a
    checksum is known the rest of the file is hashed first, so a mismatch, the likelier cause, is the error
    reported."""
    raw = open_stream(inp)
    layout = LINE_LAYOUT
    text_sha, text_size = None, None
    try:
        hr = _HashingReader(raw)
        try:
            buffered = io.BufferedReader(hr, buffer_size=_READ_BUFFER)
            gz = _decompressed(inp, buffered)
            text = _HashingReader(gz) if text_digests else None
            source = text or gz
            head = source.read(_HEADER_BYTES) if text is None else _read_some(text, _HEADER_BYTES)
            layout = sniff_layout(head) if rules.general_reader else LINE_LAYOUT
            if layout == LINE_LAYOUT:
                f = io.BufferedReader(_Prefixed(head, source), buffer_size=_READ_BUFFER)
                version, counts, root = _parse(inp, f, sink, rules, text_digests)
                if text is not None:
                    text_sha, text_size = text.sha.hexdigest(), text.size
            else:
                body = head + (_read_rest(text) if text is not None else source.read())
                canonical = _canonical_text(inp.name, _load_json(inp.name, body), rules)
                del body
                f = io.BufferedReader(io.BytesIO(canonical), buffer_size=_READ_BUFFER)
                version, counts, root = _parse(inp, f, sink, rules, text_digests)
                if text_digests:
                    text_sha, text_size = hashlib.sha256(canonical).hexdigest(), len(canonical)
            hr.drain()
        except (EOFError, gzip.BadGzipFile, zlib.error) as e:
            # A corrupt or cut-off download: finish hashing so a checksum mismatch, the likelier cause, is the
            # error reported when a checksum is known.
            try:
                hr.drain()
            except OSError:
                pass
            _check_sha(inp, hr.sha.hexdigest(), cause=f"; the gzip stream is also invalid ({e})")
            raise BundleFormatError(f"{inp.name}: not a complete gzip stream: {e}") from e
        except ToolError as e:
            # A wrong or corrupt file can still decompress into lines that fail the grammar. When a checksum is
            # known, finish hashing first, so a mismatch is reported instead of a misleading format error.
            if inp.expected_sha256 is not None and not isinstance(e, InputError):
                try:
                    hr.drain()
                except (OSError, EOFError, zlib.error):
                    pass
                _check_sha(inp, hr.sha.hexdigest(), cause=f"; the content also fails to parse ({e})")
            raise
    finally:
        raw.close()
    sha = hr.sha.hexdigest()
    _check_sha(inp, sha)
    return StreamedBundle(
        input=inp,
        sha256=sha,
        size=hr.size,
        schema_version=version,
        counts=counts,
        text_sha256=text_sha,
        text_size=text_size,
        layout=layout,
        root=tuple(root),
    )


def _read_some(reader: _HashingReader, n: int) -> bytes:
    buf = bytearray(n)
    got = 0
    while got < n:
        m = reader.readinto(memoryview(buf)[got:])
        if not m:
            break
        got += m
    return bytes(buf[:got])


def _read_rest(reader: _HashingReader) -> bytes:
    out = bytearray()
    while chunk := _read_some(reader, _READ_BUFFER):
        out += chunk
    return bytes(out)


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
            "nothing was kept."
        )


def _key_end(line: bytes) -> int:
    """Index of the closing quote of the JSON key that starts at line[4] (slow path, for keys with escapes)."""
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


def _parse(
    inp: InputFile, f: BinaryIO, sink: Sink, rules: RootRules, text_digests: bool = False
) -> tuple[str, dict[str, int], list[str]]:
    name = inp.name
    sec_sha = None  # the current section's record lines, with text_digests
    lineno = 0
    section: str | None = None
    key: bytes | None = None

    def fail(msg: str) -> BundleFormatError:
        where = f"{name}: line {lineno}"
        if section:
            where += f", section {section}"
        if key is not None:
            where += f", key {key.decode('utf-8', 'replace')}"
        return BundleFormatError(f"{where}: {msg}")

    # Line 1 and the version line.
    line = f.readline()
    lineno = 1
    if line != b"{\n":
        if not line:
            raise fail("empty file (expected '{' on line 1)")
        raise fail(f"expected '{{' on line 1, got {_short(line)}")
    line = f.readline()
    lineno = 2
    version = ""
    root: list[str] = []
    m = _STRING_LINE.fullmatch(line)
    if m and m.group(1).decode("utf-8", "replace") == rules.version_property:
        version = m.group(2).decode("utf-8", "replace")
        root.append(rules.version_property)
        state = _TOP if m.group(3) else _END
        pending = None
    elif rules.version_required:
        raise SchemaVersionError(
            f'{name}: line 2 must be \'  "{rules.version_property}": "<version>",\', got {_short(line)}. See the '
            "compatibility matrix in the README."
        )
    else:
        state = _END if line in (b"}\n", b"}") else _TOP
        lineno = 1
        pending = line
    sink.begin(version)
    shape = rules.shape_of(version)
    sections = shape.collections
    section_set = frozenset(sections)

    counts: dict[str, int] = {}
    write = None
    n = 0
    comma = False
    for line in chain((pending,), f) if pending else f:
        lineno += 1
        if state == _RECORDS or state == _FIRST:
            if line.startswith(b'    "'):
                if state == _RECORDS and not comma:
                    key = None
                    raise fail("missing ',' after the previous record")
                if line.endswith(b",\n"):
                    end, comma = -2, True
                elif line.endswith(b"\n"):
                    end, comma = -1, False
                else:
                    raise fail("truncated: the last line has no newline")
                q = line.find(b'"', 5)
                if q > 0 and line.find(b"\\", 5, q) >= 0:
                    q = _key_end(line)
                if q < 0 or line[q + 1 : q + 3] != b": ":
                    raise fail(f"malformed record line {_short(line)}")
                key = line[4 : q + 1]
                record = line[q + 3 : end]
                if not (record.startswith(b"{") and record.endswith(b"}")):
                    raise fail("the record is not a JSON object on one line")
                if b"\t" in line or b"\r" in line:
                    raise fail("raw tab or carriage return in the line (not compact JSON)")
                if sec_sha is not None:
                    sec_sha.update(line)
                write(key, record)
                n += 1
                state = _RECORDS
                continue
            key = None
            if line == b"  },\n" or line == b"  }\n":
                if state == _FIRST:
                    raise fail("empty section (the assembler never writes one)")
                if comma:
                    raise fail("trailing ',' after the section's last record")
                if sec_sha is not None:
                    sink.section_text(section, sec_sha.hexdigest())
                sink.close_section(section)
                counts[section] = n
                write = None
                section = None
                state = _TOP if line == b"  },\n" else _END
                continue
            raise fail(f"expected a record line or the section close, got {_short(line)}")
        if state == _TOP:
            m = _SECTION_OPEN.fullmatch(line)
            if not m:
                if line in (b"}\n", b"}"):
                    raise fail("trailing ',' after the last section")
                m = _ROOT_VALUE_LINE.fullmatch(line)
                if not m:
                    raise fail(f"expected a section to open, got {_short(line)}")
                state = _TOP if m.group(3) else _END
                _root_line(m, sink, fail, section_set, sections, shape, version, counts, root)
                continue
            sec = m.group(1).decode("utf-8", "replace")
            if sec not in section_set:
                raise fail(f"unknown section {sec!r} (a {version or 'this'} bundle has: {', '.join(sections)})")
            if sec in counts:
                raise fail(f"section {sec!r} appears twice")
            section = sec
            root.append(sec)
            write = sink.open_section(sec)
            sec_sha = hashlib.sha256() if text_digests else None
            n = 0
            state = _FIRST
            continue
        if state == _END:
            if line != b"}\n":
                if _SECTION_OPEN.fullmatch(line):
                    raise fail("missing ',' before this section")
                raise fail(f"expected the closing '}}' of the bundle, got {_short(line)}")
            state = _DONE
            continue
        raise fail(f"trailing data after the closing '}}': {_short(line)}")
    key = None
    if state != _DONE:
        where = f"inside section {section}" if section else "before the closing '}'"
        raise fail(f"truncated: the file ends {where}")
    return version, counts, root


def _root_line(m, sink: Sink, fail, section_set, sections, shape: BundleShape, version: str, counts, root) -> None:
    """A root member on one line: an empty collection, or a root value (kept as its text)."""
    try:
        member = json.loads(m.group(1))
        json.loads(m.group(2))
    except ValueError as e:
        raise fail(f"a root member is not JSON on one line: {e}") from e
    if member in root:
        raise fail(f"root member {member!r} appears twice")
    root.append(member)
    if member in section_set:
        if m.group(2) != b"{}":
            raise fail(f"collection {member!r} must open on its own line, or be {{}}")
        sink.open_section(member)
        sink.close_section(member)
        counts[member] = 0
    elif shape.other_values:
        sink.root_value(member, m.group(2))
    else:
        raise fail(f"unknown section {member!r} (a {version or 'this'} bundle has: {', '.join(sections)})")


def canonical_key(key_json: bytes) -> bytes:
    """A bundle key in JSON form -> the key's UTF-8 bytes (what a manifest's `deleted` list holds). A JSON
    string with no backslash is its own text between the quotes, so only escaped keys are decoded."""
    if b"\\" not in key_json:
        return key_json[1:-1]
    return json.loads(key_json).encode("utf-8", "surrogatepass")


def decode_key(key_json: str) -> str:
    """A staging key (JSON string) -> the bundle key."""
    return json.loads(key_json)
