"""Shared helpers for the input-side tests (sources, bundle, replay).

Records always come from the provisional example fixture (real ClinVar-GKM 1.1.0 example records, see
tests/fixtures/examples-1.1.0/README.md). Hand-made bytes are used only for the grammar around them: section
lines, commas, the header, and the synthetic keys of synthetic deltas, which are named as such.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

from clinvar_gkm_parquet.model import InputFile

FIXTURE = Path(__file__).parent / "fixtures" / "examples-1.1.0" / "clinvar-gkm_examples.json.gz"
PROVENANCE = FIXTURE.parent / "PROVENANCE.json"


def fixture_lines() -> list[bytes]:
    """The fixture's decompressed lines, without newlines."""
    return gzip.decompress(FIXTURE.read_bytes()).split(b"\n")


def fixture_sections() -> dict[str, list[tuple[bytes, bytes]]]:
    """section -> [(key as JSON bytes, record bytes)], in file order, parsed independently of bundle.py: each
    record line is decoded with the stdlib JSON decoder to find where its key ends."""
    out: dict[str, list[tuple[bytes, bytes]]] = {}
    section = None
    dec = json.JSONDecoder()
    for line in fixture_lines():
        if line.startswith(b'  "') and line.endswith(b": {"):
            section = json.loads(line.strip()[:-3])
            out[section] = []
        elif line.startswith(b"    "):
            text = line[4:].decode()
            _, end = dec.raw_decode(text)  # the key
            key = text[:end].encode()
            record = line[4 + len(key) + 2 :]
            if record.endswith(b","):
                record = record[:-1]
            assert json.loads(record) is not None
            out[section].append((key, record))
    return out


def bundle_bytes(sections: dict[str, list[tuple[bytes, bytes]]], schema_version: str = "1.1.0") -> bytes:
    """Decompressed bundle text in the assembler's layout (assemble() in assemble-gkm-dicts.py @ 1.0)."""
    buf = bytearray(b'{\n  "schema_version": "' + schema_version.encode() + b'"')
    for name, recs in sections.items():
        if not recs:
            continue
        buf += b',\n  "' + name.encode() + b'": {\n'
        buf += b",\n".join(b"    " + k + b": " + r for k, r in recs)
        buf += b"\n  }"
    buf += b"\n}\n"
    return bytes(buf)


def write_gz(path: Path, text: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(text, compresslevel=3, mtime=0))
    return path


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def local_input(path: Path, *, sha: str | None = None, release: str | None = None) -> InputFile:
    return InputFile(
        source=str(path),
        name=path.name,
        release=release,
        expected_sha256=sha,
        sha256_source="explicit" if sha else None,
        size=path.stat().st_size,
    )


def delta_manifest_json(
    release: str, baseline: str | None, deleted: dict[str, list[str]] | None = None, **extra
) -> dict:
    """A manifest in the shape build-delta-manifest.py writes (+ r2_layout.py patch-manifest fields)."""
    sections = {s: {"added": 0, "updated": 0, "deleted": list(keys)} for s, keys in (deleted or {}).items()}
    return {
        "release": release,
        "baseline_release": baseline,
        "compare_release": release,
        "pipeline_version": "synthetic",
        "checkpoint_full": None,
        "sections": sections,
        "counts": {"A": 0, "U": 0, "D": sum(len(v) for v in (deleted or {}).values())},
        **extra,
    }
