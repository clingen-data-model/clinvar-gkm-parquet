"""Command line: build, verify, version (design plan §10)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .errors import ToolError


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="clinvar-gkm-parquet",
        description="Rebuild ClinVar-GKM's Parquet tables from its published JSON bundles.",
    )
    sub = p.add_subparsers(dest="command", required=True)

    b = sub.add_parser("build", help="build the Parquet tables from a full bundle (+ optional deltas)")
    src = b.add_mutually_exclusive_group(required=True)
    src.add_argument("--full", help="a full bundle: an official monthly or a weekly full (path or URL)")
    src.add_argument("--release", help="latest | YYYY-MM-DD: resolve the inputs from the line index")
    b.add_argument("--delta", action="append", default=[], help="a delta bundle, oldest first (repeatable)")
    b.add_argument("--manifest", action="append", default=[], help="delta manifests, in --delta order")
    b.add_argument("--index", help="the line index (default: the published v1/1.1/index.json)")
    b.add_argument("--full-source-release", help="YYYY-MM-DD the full represents, if the index can't name it")
    b.add_argument("--sha256", help="expected sha256 of --full (overrides discovery)")
    b.add_argument("--out", required=True, type=Path, help="output directory")
    b.add_argument("--tables", help="comma-separated subset of tables (default: every table in the spec)")
    b.add_argument("--work-dir", type=Path, help="staging and DuckDB spill directory")
    b.add_argument("--keep-work", action="store_true", help="keep the staging files")
    b.add_argument("--memory-limit", help="DuckDB memory limit, e.g. 12GB")
    b.add_argument("--threads", type=int, help="DuckDB threads")

    v = sub.add_parser("verify", help="check a built directory (spec, bundle recompute, oracle)")
    v.add_argument("dir", type=Path)
    v.add_argument("--bundle-recompute", action="store_true", help="id sets and data against the bundle(s)")
    v.add_argument("--full", help="the full bundle, for --bundle-recompute")
    v.add_argument("--delta", action="append", default=[], help="delta bundles, for --bundle-recompute")
    v.add_argument("--manifest", action="append", default=[], help="delta manifests, in --delta order")
    v.add_argument("--index", help="the line index, for --bundle-recompute (as for build)")
    v.add_argument("--full-source-release", help="YYYY-MM-DD the full represents, for --bundle-recompute")
    v.add_argument("--sha256", help="expected sha256 of --full, for --bundle-recompute")
    v.add_argument("--against", type=Path, help="a reference Parquet directory (the oracle comparison)")
    v.add_argument("--report", type=Path, help="write a Markdown report here (and a .json beside it)")
    v.add_argument("--work-dir", type=Path, help="staging and DuckDB spill directory")
    v.add_argument("--memory-limit", help="DuckDB memory limit, e.g. 12GB")
    v.add_argument("--threads", type=int, help="DuckDB threads")

    ver = sub.add_parser("version", help="tool version and compatibility")
    ver.add_argument("--json", action="store_true")
    return p


def _version(args: argparse.Namespace) -> int:
    from .compat import version_info

    info = version_info()
    if args.json:
        print(json.dumps(info, indent=2))
    else:
        print(
            f"clinvar-gkm-parquet {info['tool_version']}: reads bundle schema "
            f"{', '.join(info['accepted_schema_versions'])}; writes table spec {info['table_spec_version']} "
            f"({info['table_spec_status']}); duckdb {info['duckdb_version']}"
        )
    return 0


def _build(args: argparse.Namespace) -> int:
    from .build import run_build_command

    return run_build_command(args)


def _verify(args: argparse.Namespace) -> int:
    from .verify import run_verify_command

    return run_verify_command(args)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    handler = {"build": _build, "verify": _verify, "version": _version}[args.command]
    try:
        return handler(args)
    except ToolError as e:
        print(f"clinvar-gkm-parquet: error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
