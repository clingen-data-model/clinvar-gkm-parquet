"""Build stages: stage, replay, tables, self-check, manifest (design plan §6.2, §7).

Owned by the build/verify work package.
"""

from __future__ import annotations

from pathlib import Path

from .model import BuildInputs, BuildResult


def build(
    inputs: BuildInputs,
    out_dir: Path,
    *,
    tables: list[str] | None = None,
    work_dir: Path | None = None,
    keep_work: bool = False,
    memory_limit: str | None = None,
    threads: int | None = None,
) -> BuildResult:
    raise NotImplementedError


def run_build_command(args) -> int:
    """`clinvar-gkm-parquet build`: resolve the inputs from the CLI arguments, build, print a summary."""
    raise NotImplementedError
