#!/usr/bin/env python3
"""Write the snapshot of a bundle schema_version's schema set, through the tool's own loader (ADR 0006).

Fetches the bundle schema from its published URL and the closure of its `$ref`s, and writes `schemas/<version>/`:
one file per schema with its original bytes, plus MANIFEST.json (each `$id`, the URL it was fetched from, sha256).
An existing snapshot of that version is replaced. Fetched files are JSON data; nothing in them is executed.

Usage:
    uv run python scripts/vendor-schemas.py <version>
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from clinvar_gkm_parquet.errors import ToolError
from clinvar_gkm_parquet.schemas import fetch_set, write_set


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("version", help="the bundle schema_version, e.g. 1.1.0")
    args = parser.parse_args()
    try:
        schema_set = fetch_set(args.version)
    except ToolError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    target = Path(__file__).resolve().parents[1] / "schemas" / args.version
    if target.exists():
        shutil.rmtree(target)
    write_set(schema_set, target)
    print(f"{target}: {len(schema_set.files)} files, schema set sha256 {schema_set.sha256}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
