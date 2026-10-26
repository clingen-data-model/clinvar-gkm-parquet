"""Driver-based selection, wired into `build` and the `select` command (ADR 0004, design plan §4.5).

select.py is the engine: it knows bundle sections and `#/` pointers, nothing about tables. This module knows the
spec. It turns a driver table of the chosen spec, a `--where` over it and explicit keys into the engine's seeds,
picks the record source and runs the engine:

- **The driver** is a table of the chosen spec; its one bundle section holds the starting records.
- **`--where`** is a DuckDB boolean expression over the driver table AS THE CHOSEN SPEC BUILDS IT. The driver's
  section is staged from the inputs (the deltas replayed) by the build's own SectionPipeline, the driver table's
  SELECT is rendered with the spec's renderer (build.render_table), and the keys are
  `SELECT DISTINCT <key column> FROM (<rendered>) WHERE (<expr>)`. An expression holding `;` is refused, and it
  is always wrapped in parentheses, so it can't end the query or change its precedence. With neither `--where`
  nor explicit keys, every key of the driver table is selected (for a row-filter table, only its rows).
- **Explicit keys** (`--ids`, `--ids-file`) are added to the `--where` keys (their union). They need no staging
  pass; the engine fails on any the driver section doesn't hold.
- **The source.** A full alone is read by select.BundleFileSource; a full plus deltas by replay.ReplayedSource,
  the same change map the build applies, so the selection sees exactly the state the build would stage.
- **Provenance.** `driver`, `where` and `ids_sha256` (select.ids_sha256 of the explicit keys, or null) join the
  engine's parameters in `SelectionResult.metadata_json()`, the `clinvar_gkm_parquet.selection` entry.

A build with no driver never calls into this module beyond checking that no selection option was given: the full
build, and so the BigQuery-parity build, is untouched.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import duckdb

from . import build, replay, select, sqltemplate
from .build import SectionPipeline, key_column, sql_ident
from .errors import InputError
from .model import BuildInputs, InputFile, StreamedBundle
from .select import SelectionError, SelectionResult
from .spec import Spec, Table, resolve_spec

SELECTION_BUNDLE_NAME = "clinvar-gkm_selection.json.gz"  # the mini-bundle a build writes in its work directory


@dataclass(frozen=True)
class Driver:
    """What a selective run starts from (the CLI's selection options)."""

    table: str
    where: str | None = None
    ids: tuple[str, ...] = ()  # explicit keys (--ids and --ids-file), in the order given
    follow: str = "out"
    max_depth: int | None = None
    sections: tuple[str, ...] | None = None


@dataclass(frozen=True)
class Selected:
    """A finished selection: the mini-bundle and the published inputs it was cut from."""

    result: SelectionResult
    bundle: InputFile  # the mini-bundle, its expected sha256 the one the engine wrote
    full: StreamedBundle  # the published full, as the selection read it
    chain: replay.Chain  # the published deltas, read and folded


# --- checks (before anything is read) -------------------------------------------------------------------------


def check_where(expr: str) -> str:
    if ";" in expr:
        raise SelectionError(f"--where {expr!r}: a ';' is not allowed (give one boolean expression)")
    if not expr.strip():
        raise SelectionError("--where is empty")
    return expr


def driver_table(spec: Spec, drv: Driver) -> Table:
    """The driver's table, checked: a table of the spec that reads one section, with a key column."""
    names = [t.name for t in spec.tables]
    if drv.table not in names:
        raise SelectionError(f"--driver {drv.table}: not a table of spec {spec.label} (it has {', '.join(names)})")
    table = spec.table(drv.table)
    if len(table.sections) != 1:
        raise SelectionError(f"--driver {drv.table} reads sections {list(table.sections)}; a driver reads one")
    if key_column(table) is None:
        raise SelectionError(f"--driver {drv.table} has no key column")
    if drv.where is not None:
        check_where(drv.where)
    if drv.sections is not None and table.sections[0] not in drv.sections:
        raise SelectionError(
            f"--sections {','.join(drv.sections)} must include the driver's section {table.sections[0]}"
        )
    return table


def check_tables(spec: Spec, names: list[str], drv: Driver, *, explicit: bool) -> None:
    """With `--tables` given (`explicit`), every chosen table must read only sections in `--sections`: a table
    reading a section the mini-bundle can't hold would be empty by construction."""
    if drv.sections is None or not explicit:
        return
    allowed = set(drv.sections)
    bad = {n: [s for s in spec.table(n).sections if s not in allowed] for n in names}
    bad = {n: s for n, s in bad.items() if s}
    if bad:
        detail = "; ".join(f"{n} reads {', '.join(s)}" for n, s in bad.items())
        raise SelectionError(
            f"--tables and --sections conflict: {detail}, outside --sections {','.join(drv.sections)}"
        )


# --- the CLI options -------------------------------------------------------------------------------------------


def read_ids(ids: str | None, ids_file: str | Path | None) -> tuple[str, ...]:
    """--ids k1,k2 and --ids-file (one key per line; blank lines skipped), in that order, without repeats."""
    out: list[str] = []
    if ids:
        out += [k.strip() for k in ids.split(",") if k.strip()]
    if ids_file:
        try:
            text = Path(ids_file).read_text(encoding="utf-8")
        except OSError as e:
            raise InputError(f"--ids-file {ids_file}: {e}") from e
        out += [line.strip() for line in text.splitlines() if line.strip()]
    return tuple(dict.fromkeys(out))


SELECTION_OPTIONS = ("where", "ids", "ids_file", "follow", "max_depth", "sections")


def driver_from_args(args) -> Driver | None:
    """The CLI's selection options -> Driver, or None without --driver (then no other selection option may be
    given)."""
    table = getattr(args, "driver", None)
    if table is None:
        given = [o for o in SELECTION_OPTIONS if getattr(args, o, None) is not None]
        if given:
            raise InputError(f"{', '.join('--' + o.replace('_', '-') for o in given)} need(s) --driver")
        return None
    sections = getattr(args, "sections", None)
    return Driver(
        table=table,
        where=getattr(args, "where", None),
        ids=read_ids(getattr(args, "ids", None), getattr(args, "ids_file", None)),
        follow=getattr(args, "follow", None) or "out",
        max_depth=getattr(args, "max_depth", None),
        sections=tuple(s.strip() for s in sections.split(",") if s.strip()) if sections is not None else None,
    )


# --- the driver keys and the selection -------------------------------------------------------------------------


def driver_keys(
    inputs: BuildInputs,
    spec: Spec,
    table: Table,
    where: str | None,
    work: Path,
    chain: replay.Chain,
    *,
    memory_limit: str | None = None,
    threads: int | None = None,
    log: Callable[[str], None] | None = None,
) -> tuple[list[str], StreamedBundle]:
    """The keys of the driver table's rows that `where` holds for (every row with `where` None), evaluated on the
    table as the spec builds it from the inputs' final state. Returns (sorted keys, the full as streamed)."""
    section = table.sections[0]
    body = build.render_table(table).strip().rstrip(";")
    query = f"SELECT DISTINCT {sql_ident(key_column(table))} FROM (\n{body}\n) AS _driver"
    if where is not None:
        query += f"\nWHERE ({check_where(where)})"
    keys: list[str] = []

    def process(con: duckdb.DuckDBPyConnection, sec: str, rows: int) -> None:
        if sec != section:
            return
        con.execute(sqltemplate.macros_sql())
        try:
            keys.extend(r[0] for r in con.execute(query).fetchall())
        except duckdb.Error as e:
            what = f"--where {where!r}" if where is not None else "the key query"
            raise SelectionError(f"{what} on driver table {table.name} (spec {spec.label}) failed: {e}") from e

    if log:
        log(f"driver {table.name}: evaluating " + (f"--where {where!r}" if where is not None else "every key"))
    pipe = SectionPipeline(
        inputs,
        spec,
        work / "driver",
        process=process,
        needed={section},
        memory_limit=memory_limit,
        threads=threads,
        log=log,
        chain=chain,
    )
    state = pipe.run()
    return sorted(keys), state.full


def run_selection(
    inputs: BuildInputs,
    spec: Spec,
    drv: Driver,
    out_path: Path,
    work: Path,
    *,
    memory_limit: str | None = None,
    threads: int | None = None,
    log: Callable[[str], None] | None = None,
) -> Selected:
    """Find the driver keys, run the engine on the (replayed) inputs and write the mini-bundle at `out_path`."""
    table = driver_table(spec, drv)
    section = table.sections[0]
    work = Path(work)
    work.mkdir(parents=True, exist_ok=True)
    chain = replay.load_chain(inputs, log)
    keys = set(drv.ids)
    streamed = None
    if drv.where is not None or not drv.ids:
        found, streamed = driver_keys(
            inputs, spec, table, drv.where, work, chain, memory_limit=memory_limit, threads=threads, log=log
        )
        keys.update(found)
        if log:
            log(
                f"driver {table.name}: {len(found)} key(s) from the table"
                + (f", {len(drv.ids)} given" if drv.ids else "")
            )
    if not keys:
        cond = f"--where {drv.where!r}" if drv.where is not None else "no --where"
        raise SelectionError(
            f"no driver keys: {cond} matches no row of driver table {table.name} (section {section}, spec "
            f"{spec.label}), and no --ids were given; nothing to select"
        )
    if chain.deltas:
        source = replay.ReplayedSource(inputs.full, chain)
        files = source.files
    else:
        source = files = select.BundleFileSource(inputs.full)
    provenance = {
        "driver": table.name,
        "where": drv.where,
        "ids_sha256": select.ids_sha256(drv.ids) if drv.ids else None,
    }
    if log:
        log(f"selecting from {source.name}: {len(keys)} driver key(s), follow {drv.follow}")
    result = select.select(
        source,
        {section: sorted(keys)},
        out_path,
        follow=drv.follow,
        max_depth=drv.max_depth,
        sections=list(drv.sections) if drv.sections is not None else None,
        provenance=provenance,
        work_dir=work,
    )
    if streamed is not None and streamed.sha256 != files.sha256:
        raise InputError(
            f"{inputs.full.name}: sha256 {files.sha256} differs from {streamed.sha256}, read for the driver keys; "
            "the file changed between the passes"
        )
    if log:
        log(
            f"selected {result.records} record(s) in {len(result.section_counts)} section(s), "
            f"{len(result.passes)} pass(es), {len(result.dangling)} dangling pointer(s); mini-bundle sha256 "
            f"{result.sha256}"
        )
    full = StreamedBundle(
        input=inputs.full,
        sha256=files.sha256,
        size=files.size,
        schema_version=result.schema_version,
        counts=dict(result.source_records) if not chain.deltas else {},
    )
    bundle = InputFile(
        source=str(out_path),
        name=Path(out_path).name,
        release=None,
        expected_sha256=result.sha256,
        sha256_source="selection",
        size=result.size,
    )
    return Selected(result=result, bundle=bundle, full=full, chain=chain)


# --- the select command ----------------------------------------------------------------------------------------


def run_select_command(args) -> int:
    """`clinvar-gkm-parquet select`: write only the mini-bundle, and print the selection entry a build from it
    would carry. The same inputs and options give the same bytes."""
    spec, _ = resolve_spec(getattr(args, "spec", None))
    drv = driver_from_args(args)
    out = Path(args.out)
    if out.exists():
        raise InputError(f"{out} already exists; select writes a new file")
    driver_table(spec, drv)
    inputs = build.resolve_build_inputs(args)
    root = Path(args.work_dir) if args.work_dir else out.parent
    root.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=".select-work-", dir=root))
    try:
        sel = run_selection(
            inputs, spec, drv, out, work, memory_limit=args.memory_limit, threads=args.threads, log=build._log
        )
    finally:
        shutil.rmtree(work, ignore_errors=True)
    r = sel.result
    print(
        f"selected {r.records} record(s) in {len(r.section_counts)} section(s) from {r.source_name}: {r.path} "
        f"({r.size} bytes, sha256 {r.sha256})"
    )
    for section, n in r.section_counts.items():
        print(f"  {section:<36} {n:>12}")
    if r.dangling:
        print(f"{len(r.dangling)} dangling pointer(s), e.g. {r.dangling[0].pointer}", file=sys.stderr)
    print(f"selection: {r.metadata_json()}")
    return 0
