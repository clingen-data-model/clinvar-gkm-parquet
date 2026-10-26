"""Data passed between the stages: inputs, streamed and staged bundles, delta manifests, build results.

Stage interfaces (each module owns its half):

- sources.py  resolves a path or URL (or `--release`) into InputFile / BuildInputs, finds the expected sha256
              (explicit, `.sha256` sidecar, line index, frozen-line MANIFEST.json), opens byte streams, and
              reads the line index and delta manifests.
- bundle.py   streams one bundle once: hashes the compressed bytes, checks schema_version, classifies every
              line (design plan §4.2) and hands each record to a sink -> StreamedBundle.
- replay.py   checks a delta chain, reads the (small) deltas into memory and folds them into one net change map
              per section (design plan §4.3), which the build applies while the full streams past.
- build.py    the section pipeline (design plan §6.2): streams the full once, applies the change map, stages
              only the current section as chunk files, and hands each closed section to a worker thread that
              loads it into DuckDB as `bundle_<section>(key VARCHAR, record VARCHAR)`, runs the SQL of every table
              that reads it (sqltemplate.py renders it from the spec), writes `<table>.parquet` with the spec's
              writer settings and key-value metadata, and drops it. Then the manifest.
- verify.py   checks a built directory against the spec, the bundle (recompute, on the same pipeline) or a
              reference set (oracle).

Staging format: gzip text, one record per line, `<key as JSON string>\\t<record>\\n`. `<record>` is the exact
bytes from the bundle line (the trailing `,` removed), so `data` equals the bundle record byte for byte. JSON
text never holds a raw tab or newline, so the split is unambiguous. The build writes a section as chunk files
(`<section>.<nnnn>.tsv.gz`) holding the section's final state: the full's records in bundle order, less the keys
the deltas replace or delete, then the deltas' net upserts. `stage_bundle` writes one file per section.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class InputFile:
    """One input bundle (or manifest) as resolved before reading."""

    source: str  # local path or URL, as resolved
    name: str  # the dated file name (an alias like clinvar-gkm_00-latest is resolved to it)
    release: str | None  # YYYY-MM-DD the file represents (a monthly's source_release; a weekly/delta's release)
    expected_sha256: str | None  # hex, or None when no checksum source exists
    sha256_source: str | None  # "explicit" | "sidecar" | "index" | "frozen-manifest" | None
    size: int | None = None


@dataclass(frozen=True)
class DeltaManifest:
    """A weekly delta's manifest (clinvar-gkm-delta_YYYY-MMDD.manifest.json)."""

    release: str  # YYYY-MM-DD of this delta
    baseline_release: str | None  # the release this delta applies on top of (null only on the first release)
    deleted: dict[str, tuple[str, ...]]  # section -> keys deleted in this delta
    checkpoint_full: dict | None  # as published; the official monthly that precedes the delta
    raw: dict = field(repr=False, default_factory=dict)  # the manifest as read, for provenance


@dataclass(frozen=True)
class StreamedBundle:
    """What one pass over a bundle found."""

    input: InputFile
    sha256: str  # computed over the compressed bytes while streaming
    size: int  # compressed bytes read
    schema_version: str
    counts: dict[str, int]  # section -> records, for the sections the bundle has


@dataclass(frozen=True)
class DeltaBundle:
    """A delta bundle read into memory (deltas are small: a weekly delta is about 10 MB gzip)."""

    streamed: StreamedBundle
    sections: dict[str, list[tuple[bytes, bytes]]]  # section -> [(key as JSON bytes, record bytes)], file order

    @property
    def input(self) -> InputFile:
        return self.streamed.input

    @property
    def sha256(self) -> str:
        return self.streamed.sha256

    @property
    def size(self) -> int:
        return self.streamed.size

    @property
    def schema_version(self) -> str:
        return self.streamed.schema_version


@dataclass(frozen=True)
class StagedSection:
    name: str
    path: Path  # staging file (format above)
    records: int
    max_line_bytes: int = 0  # longest staging line, to size DuckDB's read_csv max_line_size


@dataclass(frozen=True)
class StagedBundle:
    """A whole bundle staged to one file per section (bundle.stage_bundle)."""

    input: InputFile
    sha256: str  # computed over the compressed bytes while streaming
    size: int  # compressed bytes read
    schema_version: str
    sections: dict[str, StagedSection]


@dataclass(frozen=True)
class SectionChunks:
    """One section's final state, staged by the pipeline and handed to the worker."""

    section: str
    paths: tuple[Path, ...]  # chunk files (format above); empty when the final state has no records
    records: int
    max_line_bytes: int = 0


@dataclass(frozen=True)
class BuildInputs:
    """What a build reads: one full, then zero or more deltas (oldest first) with their manifests."""

    full: InputFile
    full_source_release: str | None  # the release the full represents (needed to start a delta chain)
    deltas: tuple[tuple[InputFile, InputFile], ...] = ()  # (delta bundle, its manifest)


@dataclass(frozen=True)
class TableResult:
    table: str
    file: Path
    rows: int
    sha256: str
    size: int


@dataclass(frozen=True)
class BuildResult:
    out_dir: Path
    source_release: str | None
    tables: tuple[TableResult, ...]
    manifest_path: Path
