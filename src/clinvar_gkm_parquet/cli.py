"""Command line: build, select, rebuild, verify, version (design plan §10)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .errors import ToolError

SPEC_HELP = (
    "table spec: a built-in name (sections, the default: one table per bundle section, a typed column per "
    "record property; baseline: per section id, refs and the record as data; clinvar-gkm, clinvar-gkm-1.0 or "
    "clinvar-gkm@1.0 for the 22 tables of ClinVar-GKM's published set) or the path of a tables.yaml"
)


def _inputs(p: argparse.ArgumentParser) -> None:
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--full", help="a full bundle: an official monthly or a weekly full (path or URL)")
    src.add_argument("--release", help="latest | YYYY-MM-DD: resolve the inputs from the line index")
    p.add_argument("--delta", action="append", default=[], help="a delta bundle, oldest first (repeatable)")
    p.add_argument("--manifest", action="append", default=[], help="delta manifests, in --delta order")
    p.add_argument("--index", help="the line index (default: the published v1/1.1/index.json)")
    p.add_argument("--full-source-release", help="YYYY-MM-DD the full represents, if the index can't name it")
    p.add_argument("--sha256", help="expected sha256 of --full (overrides discovery)")


def _selection(p: argparse.ArgumentParser, *, required: bool) -> None:
    g = p.add_argument_group("selection (ADR 0004)")
    g.add_argument(
        "--driver", required=required, help="a table of the spec: its bundle section holds the starting records"
    )
    g.add_argument(
        "--where",
        help="a DuckDB boolean expression over the driver table as the spec builds it (no ';'), "
        "e.g. \"name = 'x'\" with the sections spec, \"data->>'$.name' = 'x'\" with baseline",
    )
    g.add_argument("--ids", help="comma-separated driver keys (added to the --where keys)")
    g.add_argument("--ids-file", help="a file of driver keys, one per line (added to the --where keys)")
    g.add_argument(
        "--follow", choices=("out", "in", "both"), help="follow pointers out of, into, or both ways (default out)"
    )
    g.add_argument("--max-depth", type=int, help="pointer hops from a driver record (default unlimited)")
    g.add_argument("--sections", help="comma-separated bundle sections that may be traversed and written")


def _engine(p: argparse.ArgumentParser) -> None:
    p.add_argument("--work-dir", type=Path, help="staging and DuckDB spill directory")
    p.add_argument("--memory-limit", help="DuckDB memory limit, e.g. 12GB")
    p.add_argument("--threads", type=int, help="DuckDB threads")


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="clinvar-gkm-parquet",
        description="Rebuild ClinVar-GKM's Parquet tables from its published JSON bundles.",
    )
    sub = p.add_subparsers(dest="command", required=True)

    b = sub.add_parser(
        "build",
        help="build the Parquet tables from a full bundle (+ optional deltas), or from a selection of it",
        description="Build the tables of a spec. With --driver, select the driver records and the records they "
        "reach first (a mini-bundle in the work directory), then build from it.",
    )
    _inputs(b)
    b.add_argument("--spec", help=SPEC_HELP)
    b.add_argument("--out", required=True, type=Path, help="output directory")
    b.add_argument("--tables", help="comma-separated subset of tables (default: every table in the spec)")
    b.add_argument("--keep-work", action="store_true", help="keep the staging files (and the mini-bundle)")
    _engine(b)
    _selection(b, required=False)

    s = sub.add_parser(
        "select",
        help="write only the mini-bundle of a selection (the driver records and the records they reach)",
        description="Write the mini-bundle a selective build would build from: the selected records, verbatim, "
        "in the bundle's line layout. The same inputs and options give the same bytes.",
    )
    _inputs(s)
    s.add_argument("--spec", help=SPEC_HELP + "; it defines the driver table --where reads")
    s.add_argument("--out", required=True, type=Path, help="the mini-bundle to write (.json.gz; must not exist)")
    _engine(s)
    _selection(s, required=True)

    r = sub.add_parser(
        "rebuild",
        help="write a full lossless build (the sections spec) back as its source bundle, byte for byte",
        description="Write the bundle a full build of a lossless spec (the default, sections) was made from: its "
        "uncompressed text byte for byte (ADR 0005), checked against the sha256s the build recorded. Refuses a "
        "selection, a build with deltas or --tables, a directory without its manifest, and any other spec.",
    )
    r.add_argument("dir", type=Path, help="the built directory")
    r.add_argument("-o", "--out", required=True, type=Path, help="the bundle to write (.json, or .json.gz)")
    r.add_argument("--spec", help="a custom lossless spec's tables.yaml (built-in specs are read from the files)")
    _engine(r)

    v = sub.add_parser("verify", help="check a built directory (spec, bundle recompute, round trip, oracle)")
    v.add_argument("dir", type=Path)
    v.add_argument(
        "--spec",
        help="the spec the files were written by (default: read from the files); required for a custom spec, "
        "whose tables.yaml must have the recorded sha256",
    )
    v.add_argument("--bundle-recompute", action="store_true", help="id sets and data against the bundle(s)")
    v.add_argument(
        "--roundtrip",
        action="store_true",
        help="rebuild the bundle text (hashed, not written) and compare it with the recorded sha256s; with "
        "--full, also with that bundle's uncompressed text (a full lossless build only, ADR 0005)",
    )
    v.add_argument(
        "--full",
        help="the full bundle, for --bundle-recompute (or --roundtrip); for a selection, the mini-bundle (its "
        "sha256 must equal the recorded selected_sha256)",
    )
    v.add_argument("--delta", action="append", default=[], help="delta bundles, for --bundle-recompute")
    v.add_argument("--manifest", action="append", default=[], help="delta manifests, in --delta order")
    v.add_argument("--index", help="the line index, for --bundle-recompute (as for build)")
    v.add_argument("--full-source-release", help="YYYY-MM-DD the full represents, for --bundle-recompute")
    v.add_argument("--sha256", help="expected sha256 of --full, for --bundle-recompute")
    v.add_argument(
        "--against",
        type=Path,
        help="a reference Parquet directory (the oracle comparison; a full clinvar-gkm build only)",
    )
    v.add_argument("--report", type=Path, help="write a Markdown report here (and a .json beside it)")
    _engine(v)

    ver = sub.add_parser("version", help="tool version and compatibility")
    ver.add_argument("--json", action="store_true")
    return p


def _version(args: argparse.Namespace) -> int:
    from .compat import version_info

    info = version_info()
    if args.json:
        print(json.dumps(info, indent=2))
    else:
        specs = ", ".join(
            f"{s['name']} {s['version']} ({s['status']}{', default' if s['default'] else ''})"
            for s in info["table_specs"]
        )
        print(
            f"clinvar-gkm-parquet {info['tool_version']}: reads bundle schema "
            f"{', '.join(info['accepted_schema_versions'])}; built-in table specs {specs}; "
            f"duckdb {info['duckdb_version']}"
        )
    return 0


def _build(args: argparse.Namespace) -> int:
    from .build import run_build_command

    return run_build_command(args)


def _select(args: argparse.Namespace) -> int:
    from .driver import run_select_command

    return run_select_command(args)


def _rebuild(args: argparse.Namespace) -> int:
    from .roundtrip import run_rebuild_command

    return run_rebuild_command(args)


def _verify(args: argparse.Namespace) -> int:
    from .verify import run_verify_command

    return run_verify_command(args)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    handler = {"build": _build, "select": _select, "rebuild": _rebuild, "verify": _verify, "version": _version}[
        args.command
    ]
    import duckdb

    try:
        return handler(args)
    except ToolError as e:
        print(f"clinvar-gkm-parquet: error: {e}", file=sys.stderr)
        return 2
    except (
        duckdb.Error
    ) as e:  # e.g. an unreadable or truncated Parquet or staging file: an input error, not a diff
        print(f"clinvar-gkm-parquet: error: {type(e).__name__}: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
