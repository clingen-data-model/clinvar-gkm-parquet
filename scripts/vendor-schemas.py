#!/usr/bin/env python3
"""Write a schema set on disk through the tool's loader: a root schema and the closure of its `$ref`s (ADR 0006).

Reads the root from a file or URL, fetches the closure, and writes <directory>: one file per schema with its
original bytes, plus MANIFEST.json (each `$id`, where it was read from, sha256). An existing set there is replaced.
Fetched files are JSON data; nothing in them is executed. `build --bundle-schema <directory>` reads a set with no
network.

Usage:
    uv run python scripts/vendor-schemas.py <schema file or URL> <directory>
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
    parser.add_argument("root", help="the root schema: a file or an https:// URL")
    parser.add_argument("directory", type=Path, help="where to write the set")
    args = parser.parse_args()
    try:
        schema_set = fetch_set(args.root)
    except ToolError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if args.directory.exists():
        shutil.rmtree(args.directory)
    write_set(schema_set, args.directory)
    print(f"{args.directory}: {len(schema_set.files)} files, schema set sha256 {schema_set.sha256}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
