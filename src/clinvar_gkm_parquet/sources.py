"""Resolve inputs: local paths and URLs, checksums, the line index, delta manifests (design plan §4.1).

Owned by the input-side work package. Network rules (CLAUDE.md, .claude/rules/python.md): every request sends
`User-Agent: clinvar-gkm-parquet/<version>`; one stream per large file; retry 429 and 5xx with backoff; resume
with HTTP Range. Unit tests never touch the network.
"""

from __future__ import annotations

from typing import BinaryIO

from .model import BuildInputs, DeltaManifest, InputFile


def resolve_input(source: str, *, sha256: str | None = None, index: dict | None = None) -> InputFile:
    """A path or URL -> InputFile, with the expected sha256 from the first source that has one."""
    raise NotImplementedError


def open_stream(inp: InputFile) -> BinaryIO:
    """A binary stream of the file's (compressed) bytes."""
    raise NotImplementedError


def load_index(source: str) -> dict:
    """The line index (v1/1.1/index.json), parsed."""
    raise NotImplementedError


def load_manifest(inp: InputFile) -> DeltaManifest:
    raise NotImplementedError


def resolve_release(index: dict, release: str, *, base_url: str) -> BuildInputs:
    """`--release latest|YYYY-MM-DD`: the weekly full if it is still published, else the replay rule (§4.3)."""
    raise NotImplementedError
