"""Stream one bundle into per-section staging files (design plan §4.2, §5).

Owned by the input-side work package. One pass: hash the compressed bytes, decompress, check
`schema_version` (line 2) against compat.ACCEPTED_SCHEMA_VERSIONS, classify each line with a strict grammar,
and write the staging format described in model.py. Fails (BundleFormatError / SchemaVersionError) on any line
it can't classify, an unknown or repeated section, an empty section, a broken trailing-comma sequence, trailing
garbage or a truncated file; fails (InputError) when the computed sha256 differs from the expected one.

The grammar is the one `assemble()` in ClinVar-GKM's `src/scripts/assemble-gkm-dicts.py` writes (@ origin/1.0
25344ba):

    {
      "schema_version": "1.1.0",        <- no trailing comma when the bundle has no sections
      "<section>": {                    <- opened lazily on the first record, so never empty
        "<key>": <record>,              <- four spaces, JSON key, ": ", compact JSON record
        "<key>": <record>               <- the section's last record has no comma
      },                                <- "  }" without a comma closes the last section
      ...
    }

Duplicate keys within a section are detected when the staging files are loaded (replay.load_state, one
GROUP BY per section in DuckDB), not here: an in-memory key set for the largest section (27 M keys) would
cost gigabytes of RAM in Python.
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
from pathlib import Path
from typing import BinaryIO

from .compat import ACCEPTED_SCHEMA_VERSIONS
from .errors import BundleFormatError, InputError, SchemaVersionError
from .model import InputFile, StagedBundle, StagedSection
from .sources import open_stream

log = logging.getLogger(__name__)

# The 20 sections of the 1.1.0 bundle schema (schema/clinvar-gkm/clinvar-gkm-bundle.schema.json @ origin/1.0),
# in the assembler's SECTIONS order.
SECTIONS: tuple[str, ...] = (
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
)
_SECTION_SET = frozenset(SECTIONS)

STAGING_SUFFIX = ".tsv.gz"
STAGING_COMPRESSLEVEL = 1
_WRITE_CHUNK = 1 << 20  # bytes buffered before each gzip write
_READ_BUFFER = 1 << 20  # decompressed read buffer

_SCHEMA_LINE = re.compile(rb'  "schema_version": "([^"\\]*)"(,?)\n')
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


class _SectionWriter:
    def __init__(self, path: Path):
        self.path = path
        self._fh = open(path, "wb")
        self._gz = gzip.GzipFile(
            filename="", mode="wb", compresslevel=STAGING_COMPRESSLEVEL, fileobj=self._fh, mtime=0
        )
        self._buf = bytearray()
        self.records = 0
        self.max_line = 0

    def add(self, key: bytes, record: bytes) -> None:
        buf = self._buf
        start = len(buf)
        buf += key
        buf += b"\t"
        buf += record
        buf += b"\n"
        n = len(buf) - start
        if n > self.max_line:
            self.max_line = n
        self.records += 1
        if len(buf) >= _WRITE_CHUNK:
            self._gz.write(buf)
            buf.clear()

    def close(self) -> None:
        if self._buf:
            self._gz.write(self._buf)
            self._buf.clear()
        self._gz.close()
        self._fh.close()


def staging_name(inp: InputFile) -> str:
    """The staging folder name for an input: its file name without `.json.gz`."""
    name = inp.name
    for sfx in (".json.gz", ".gz", ".json"):
        if name.endswith(sfx):
            return name[: -len(sfx)]
    return name


def stage_bundle(inp: InputFile, work_dir: Path) -> StagedBundle:
    """Stream `inp` once into `work_dir/<name>/<section>.tsv.gz` and return what was staged.

    The files are written to a temporary folder and moved into place only after the whole bundle has parsed
    and its sha256 matched, so a failed run never leaves staging that looks complete.
    """
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    final = work_dir / staging_name(inp)
    tmp = Path(tempfile.mkdtemp(prefix=f".{final.name}.", suffix=".partial", dir=work_dir))
    try:
        sha, size, version, sections = _stage(inp, tmp)
        if final.exists():
            shutil.rmtree(final)
        tmp.rename(final)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    staged = {
        name: StagedSection(name=name, path=final / p.name, records=n, max_line_bytes=m)
        for name, (p, n, m) in sections.items()
    }
    return StagedBundle(input=inp, sha256=sha, size=size, schema_version=version, sections=staged)


def _stage(inp: InputFile, out_dir: Path) -> tuple[str, int, str, dict[str, tuple[Path, int, int]]]:
    raw = open_stream(inp)
    try:
        hr = _HashingReader(raw)
        try:
            gz = gzip.GzipFile(fileobj=hr, mode="rb")
            f = io.BufferedReader(gz, buffer_size=_READ_BUFFER)
            version, sections = _parse(inp, f, out_dir)
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
        except (BundleFormatError, SchemaVersionError) as e:
            # A wrong or corrupt file can still decompress into lines that fail the grammar. When a checksum is
            # known, finish hashing first, so a mismatch is reported instead of a misleading format error.
            if inp.expected_sha256 is not None:
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
    return sha, hr.size, version, sections


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
            "nothing was staged."
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


def _parse(inp: InputFile, f: BinaryIO, out_dir: Path) -> tuple[str, dict[str, tuple[Path, int, int]]]:
    name = inp.name
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

    def short(line: bytes) -> str:
        text = line.decode("utf-8", "replace")
        return repr(text if len(text) <= 120 else text[:120] + "...")

    # Line 1 and line 2 (schema_version).
    line = f.readline()
    lineno = 1
    if line != b"{\n":
        if not line:
            raise fail("empty file (expected '{' on line 1)")
        raise fail(f"expected '{{' on line 1, got {short(line)}")
    line = f.readline()
    lineno = 2
    accepted = ", ".join(ACCEPTED_SCHEMA_VERSIONS)
    m = _SCHEMA_LINE.fullmatch(line)
    if not m:
        raise SchemaVersionError(
            f'{name}: line 2 must be \'  "schema_version": "<version>",\', got {short(line)}. This tool reads '
            f"bundle schema_version {accepted}; see the compatibility matrix in the README."
        )
    version = m.group(1).decode("utf-8", "replace")
    if version not in ACCEPTED_SCHEMA_VERSIONS:
        raise SchemaVersionError(
            f"{name}: bundle schema_version {version!r} is not supported; this tool reads {accepted}. "
            "See the compatibility matrix in the README for the tool release that reads it."
        )
    state = _TOP if m.group(2) else _END

    staged: dict[str, tuple[Path, int, int]] = {}
    writer: _SectionWriter | None = None
    write = None
    comma = False
    try:
        for line in f:
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
                        raise fail(f"malformed record line {short(line)}")
                    key = line[4 : q + 1]
                    record = line[q + 3 : end]
                    if not (record.startswith(b"{") and record.endswith(b"}")):
                        raise fail("the record is not a JSON object on one line")
                    if b"\t" in line or b"\r" in line:
                        raise fail("raw tab or carriage return in the line (not compact JSON)")
                    write(key, record)
                    state = _RECORDS
                    continue
                key = None
                if line == b"  },\n" or line == b"  }\n":
                    if state == _FIRST:
                        raise fail("empty section (the assembler never writes one)")
                    if comma:
                        raise fail("trailing ',' after the section's last record")
                    writer.close()
                    staged[section] = (writer.path, writer.records, writer.max_line)
                    writer = write = None
                    section = None
                    state = _TOP if line == b"  },\n" else _END
                    continue
                raise fail(f"expected a record line or the section close, got {short(line)}")
            if state == _TOP:
                m = _SECTION_OPEN.fullmatch(line)
                if not m:
                    if line in (b"}\n", b"}"):
                        raise fail("trailing ',' after the last section")
                    raise fail(f"expected a section to open, got {short(line)}")
                sec = m.group(1).decode("utf-8", "replace")
                if sec not in _SECTION_SET:
                    raise fail(f"unknown section {sec!r} (the 1.1.0 bundle has: {', '.join(SECTIONS)})")
                if sec in staged:
                    raise fail(f"section {sec!r} appears twice")
                section = sec
                writer = _SectionWriter(out_dir / f"{sec}{STAGING_SUFFIX}")
                write = writer.add
                state = _FIRST
                continue
            if state == _END:
                if line != b"}\n":
                    if _SECTION_OPEN.fullmatch(line):
                        raise fail("missing ',' before this section")
                    raise fail(f"expected the closing '}}' of the bundle, got {short(line)}")
                state = _DONE
                continue
            raise fail(f"trailing data after the closing '}}': {short(line)}")
    finally:
        if writer is not None:
            writer.close()
    key = None
    if state != _DONE:
        where = f"inside section {section}" if section else "before the closing '}'"
        raise fail(f"truncated: the file ends {where}")
    return version, staged


def decode_key(key_json: str) -> str:
    """A staging key (JSON string) -> the bundle key."""
    return json.loads(key_json)
