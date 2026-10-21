"""verify: spec check of a built directory, bundle recompute, and the oracle comparison (design plan §8.2).

Owned by the build/verify work package. Exit codes (CLI): 0 = pass, 1 = differences, 2 = usage or input error.
"""

from __future__ import annotations

from pathlib import Path


def verify_dir(out_dir: Path) -> list[str]:
    """Problems found in a built directory on its own (every spec table present, metadata consistent, DESCRIBE
    equals the spec). Empty list = pass."""
    raise NotImplementedError


def run_verify_command(args) -> int:
    """`clinvar-gkm-parquet verify`: returns the exit code."""
    raise NotImplementedError
