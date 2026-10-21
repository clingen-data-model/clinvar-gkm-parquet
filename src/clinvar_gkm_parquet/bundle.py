"""Stream one bundle into per-section staging files (design plan §4.2, §5).

Owned by the input-side work package. One pass: hash the compressed bytes, decompress, check
`schema_version` (line 2) against compat.ACCEPTED_SCHEMA_VERSIONS, classify each line with a strict grammar,
and write the staging format described in model.py. Fails (BundleFormatError / SchemaVersionError) on any line
it can't classify, an unknown or repeated section, or a duplicate key within a section.
"""

from __future__ import annotations

from pathlib import Path

from .model import InputFile, StagedBundle


def stage_bundle(inp: InputFile, work_dir: Path) -> StagedBundle:
    raise NotImplementedError
