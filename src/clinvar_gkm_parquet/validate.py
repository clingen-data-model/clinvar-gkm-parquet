"""Validate every record of a bundle against its JSON Schema set, with one of two engines that must agree.

Each record is validated alone, as a one-section mini-bundle `{<version property>: <the bundle's version>,
<section>: {<key>: <record>}}`, the way ClinVar-GKM's `validate-bundle.py` does, so the section's key pattern is
checked too. An empty section is validated as `{<version property>: ..., <section>: {}}`, and each root value that
is not a collection as `{<version property>: ..., <name>: <value>}`. A verdict is per record. The bundle is
streamed with the tool's reader (bundle.py), and records are parsed and validated in worker processes.

Engines:

- `rs` (the default): jsonschema-rs, with a `Registry` of every other file of the set under its `$id`, and the root
  compiled per set of names a mini-bundle holds (`_RsEngine`). Root-relative `$ref`s (`/clingen/...`) resolve
  against the `$id` of the file they sit in. Its retriever refuses any URI the set doesn't hold, so nothing is
  fetched. Formats aren't validated, as with Python `jsonschema` by default.
- `python` (the reference, the `reference` extra): the GA4GH GKM Toolkit's `validate_bundle_schema`, with a
  `Draft202012Validator` over a `referencing` registry of the same set, each file under its `$id` and its
  root-relative form, as ClinVar-GKM's `load_registry` builds it. A record's errors are that validator's
  `iter_errors`.

Both are built from the same `SchemaSet` (schemas.py), so they see exactly what a build sees. A schema that can't
be compiled or a `$ref` that can't be resolved stops the run (SchemaSetError); it is never reported as an invalid
record.
"""

from __future__ import annotations

import collections
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from multiprocessing import get_context
from pathlib import Path

from .bundle import BundleShape, RootRules, decode_key, stream_bundle
from .compiler import root_parts
from .errors import SchemaSetError, SchemaVersionError, ToolError
from .model import InputFile
from .schemas import SchemaSet, load_schema_set

ENGINES = ("rs", "python")
DEFAULT_ENGINE = "rs"
BATCH = 500  # records per task sent to a worker
MAX_ERRORS = 20  # errors kept per failing record

_ROOT_RELATIVE = re.compile(r"https?://[^/]+(/.*)$")


@dataclass(frozen=True)
class Problem:
    """One error: where in the record (a JSON pointer, '' for the record itself) and the engine's message."""

    path: str
    message: str


def _pointer(parts) -> str:
    return "".join("/" + str(p).replace("~", "~0").replace("/", "~1") for p in parts)


class _RsEngine:
    """Compiles one validator per set of root names a mini-bundle holds, on first use: the root schema with its
    `properties` cut down to those names, every other keyword kept. A document holding only those names gets the
    same verdict from it as from the whole root (the names it lacks are never applied), and a set with many
    sections compiles a validator only for the sections a bundle has: compiling the whole published 1.1.0 root
    takes jsonschema-rs minutes, against well under a minute for its sections one by one (docs/validate.md)."""

    def __init__(self, schema_set: SchemaSet):
        import jsonschema_rs

        self._rs, self._set = jsonschema_rs, schema_set
        self._where = schema_set.bundle_id

        def refuse(uri: str):
            raise SchemaSetError(f"schema set {self._where}: no schema {uri} in the set (not fetched)")

        self._refuse = refuse
        try:
            # The root's own document is the validator's schema, so it isn't registered a second time.
            self._registry = jsonschema_rs.Registry(
                [(sid, f.doc) for sid, f in schema_set.files.items() if sid != schema_set.bundle_id],
                retriever=refuse,
            )
        except (ValueError, jsonschema_rs.ReferencingError) as e:
            raise SchemaSetError(f"schema set {self._where}: jsonschema-rs can't load it: {e}") from e
        self._validators: dict[frozenset, object] = {}
        self.compile_seconds = 0.0  # CPU time spent compiling, reported apart from validation

    def _validator(self, doc: dict):
        names = frozenset(doc)
        v = self._validators.get(names)
        if v is None:
            root = dict(self._set.bundle)
            props = root.get("properties")
            if isinstance(props, dict):
                root["properties"] = {k: props[k] for k in props if k in names}
            t = time.process_time()
            try:
                v = self._rs.validator_for(
                    root, registry=self._registry, retriever=self._refuse, validate_formats=False
                )
            except (ValueError, self._rs.ReferencingError) as e:
                raise SchemaSetError(f"schema set {self._where}: jsonschema-rs can't compile it: {e}") from e
            self.compile_seconds += time.process_time() - t
            self._validators[names] = v
        return v

    def is_valid(self, doc) -> bool:
        return self._validator(doc).is_valid(doc)

    def problems(self, doc) -> list[Problem]:
        return [Problem(_pointer(e.instance_path), e.message) for e in self._validator(doc).iter_errors(doc)]


class _PythonEngine:
    def __init__(self, schema_set: SchemaSet):
        try:
            from ga4gh.gkm.bundles.errors import BundleSerializationError, BundleValidationError
            from ga4gh.gkm.bundles.schema_validation import validate_bundle_schema
            from jsonschema import Draft202012Validator
            from referencing import Registry, Resource
            from referencing.jsonschema import DRAFT202012
        except ImportError as e:
            raise ToolError(
                f"--engine python needs the reference extra (pip install 'clinvar-gkm-parquet[reference]'): {e}"
            ) from e
        resources = []
        for sid, f in schema_set.files.items():
            resource = Resource.from_contents(f.doc, default_specification=DRAFT202012)
            resources.append((sid, resource))
            m = _ROOT_RELATIVE.match(sid)
            if m:
                resources.append((m.group(1), resource))
        self.schema = schema_set.bundle
        self.validator = Draft202012Validator(self.schema, registry=Registry().with_resources(resources))
        self._validate = validate_bundle_schema
        self._invalid, self._broken = BundleValidationError, BundleSerializationError
        self._where = schema_set.bundle_id
        self.compile_seconds = 0.0  # it compiles once, before any record

    def is_valid(self, doc) -> bool:
        try:
            self._validate(doc, self.schema, validator=self.validator)
        except self._invalid:
            return False
        except self._broken as e:
            raise SchemaSetError(f"schema set {self._where}: {e}") from e
        return True

    def problems(self, doc) -> list[Problem]:
        return [Problem(_pointer(e.absolute_path), e.message) for e in self.validator.iter_errors(doc)]


def make_engine(name: str, schema_set: SchemaSet):
    if name == "rs":
        return _RsEngine(schema_set)
    if name == "python":
        return _PythonEngine(schema_set)
    raise ToolError(f"unknown engine {name!r}; one of {', '.join(ENGINES)}")


# --- workers ----------------------------------------------------------------------------------------------------

_W: dict = {}


def _init_worker(schema_source: str, engine: str) -> None:
    # An initializer that raises makes the pool respawn the worker forever, so the error is kept and raised by the
    # first task instead.
    try:
        _W.update(engine=make_engine(engine, load_schema_set(schema_source)))
    except ToolError as e:
        _W.update(error=e)


def _relative(problems: list[Problem], depth: int) -> list[Problem]:
    """Paths relative to the record: drop the mini-bundle's /<section>/<key> prefix."""
    out = []
    for p in problems:
        parts = p.path.split("/")[1:]
        out.append(Problem("/" + "/".join(parts[depth:]) if len(parts) > depth else "", p.message))
    return out


def check_items(engine, version_property: str | None, version: str, section: str, items) -> list[tuple]:
    """[(key, [Problem])] for the failing items of `section`; `items` are (key or None, value) pairs. A None key
    is a whole root value (or an empty section, value {}), validated as `<section>: value`."""
    failing = []
    for key, value in items:
        doc = {version_property: version} if version_property else {}
        doc[section] = value if key is None else {key: value}
        if not engine.is_valid(doc):
            problems = _relative(engine.problems(doc), 1 if key is None else 2)
            if not problems:
                problems = [Problem("", "the engine rejects the record but reports no error")]
            failing.append((key, problems[:MAX_ERRORS]))
    return failing


def _check(task):
    if "error" in _W:
        raise _W["error"]
    version_property, version, section, raw_items = task
    engine = _W["engine"]
    t, c = time.process_time(), engine.compile_seconds
    items = [(None if k is None else decode_key(k), json.loads(v)) for k, v in raw_items]
    failing = check_items(engine, version_property, version, section, items)
    compiled = engine.compile_seconds - c
    return section, len(raw_items), failing, time.process_time() - t - compiled, compiled


# --- the run ----------------------------------------------------------------------------------------------------


@dataclass
class SectionResult:
    records: int = 0
    failing: int = 0
    cpu_seconds: float = 0.0  # parsing and validating, in all workers
    compile_seconds: float = 0.0  # compiling validators, in all workers


@dataclass
class Result:
    version: str = ""
    sections: dict[str, SectionResult] = field(default_factory=dict)
    failures: list[dict] = field(default_factory=list)  # {"section", "key", "errors": [{"path", "message"}]}
    seconds: float = 0.0
    workers: int = 0
    engine: str = ""
    schema_set_sha256: str = ""
    bundle_sha256: str = ""

    @property
    def records(self) -> int:
        return sum(s.records for s in self.sections.values())

    @property
    def failing(self) -> int:
        return sum(s.failing for s in self.sections.values())


class _Sink:
    """Batches the reader's records into worker tasks, with bounded work in flight."""

    def __init__(self, run: _Run):
        self.run = run
        self.batch: list[tuple[bytes | None, bytes]] = []
        self.section = ""
        self.count = 0

    def begin(self, version: str) -> None:
        self.run.start(version)

    def open_section(self, section: str):
        self.section, self.count = section, 0
        self.run.result.sections.setdefault(section, SectionResult())

        def add(key: bytes, record: bytes) -> None:
            self.count += 1
            self.batch.append((key.decode("utf-8", "surrogatepass"), record))
            if len(self.batch) >= BATCH:
                self.flush()

        return add

    def close_section(self, section: str) -> None:
        if self.count == 0:
            self.batch.append((None, b"{}"))  # an empty section: the schema says whether it may be
        self.flush()

    def root_value(self, name: str, value: bytes) -> None:
        self.run.result.sections.setdefault(name, SectionResult())
        self.section = name
        self.batch.append((None, value))
        self.flush()

    def flush(self) -> None:
        if self.batch:
            self.run.submit(self.section, self.batch)
            self.batch = []


class _Run:
    def __init__(self, schema_for, version_property, engine, workers, on_failure):
        self.schema_for, self.version_property = schema_for, version_property
        self.engine, self.workers, self.on_failure = engine, workers, on_failure
        self.result = Result(engine=engine, workers=workers)
        self.pool = None
        self.pending: collections.deque = collections.deque()

    def shape(self, version: str) -> BundleShape:
        schema_set, _ = self.schema_for(version)
        parts = root_parts(schema_set, self.version_property)
        return BundleShape(parts.collections, parts.other_values)

    def start(self, version: str) -> None:
        if not version and self.version_property:
            raise SchemaVersionError(f"the bundle has no {self.version_property}")
        schema_set, source = self.schema_for(version)
        const = schema_set.bundle.get("properties", {}).get(self.version_property or "", {}).get("const")
        if const is not None and const != version:
            raise SchemaVersionError(
                f"the bundle's {self.version_property} is {version!r}; its schema {schema_set.bundle_id} requires "
                f"{const!r}"
            )
        self.result.version = version
        self.result.schema_set_sha256 = schema_set.sha256
        self.pool = get_context("spawn").Pool(
            self.workers, initializer=_init_worker, initargs=(source, self.engine)
        )

    def submit(self, section: str, items) -> None:
        task = (self.version_property, self.result.version, section, items)
        self.pending.append(self.pool.apply_async(_check, (task,)))
        self.drain(self.workers * 4)

    def drain(self, limit: int) -> None:
        while len(self.pending) > limit:
            section, n, failing, cpu, compiled = self.pending.popleft().get()
            s = self.result.sections[section]
            s.records += n
            s.failing += len(failing)
            s.cpu_seconds += cpu
            s.compile_seconds += compiled
            for key, problems in failing:
                f = {"section": section, "key": key, "errors": [vars(p) for p in problems]}
                self.on_failure(f)
                self.result.failures.append(f)

    def close(self) -> None:
        if self.pool is not None:
            self.pool.close()
            self.pool.join()

    def abort(self) -> None:
        if self.pool is not None:
            self.pool.terminate()
            self.pool.join()


def validate_bundle(
    inp: InputFile,
    schema_for,
    version_property: str | None,
    *,
    engine: str = DEFAULT_ENGINE,
    workers: int | None = None,
    on_failure=lambda failure: None,
) -> Result:
    """Validate every record of `inp`. `schema_for(version)` returns (SchemaSet, the source a worker loads it
    from).
    Raises ToolError for anything that isn't a verdict on a record: an unreadable or malformed bundle, an unknown
    section, a missing or unsupported version, a schema set that doesn't load or compile."""
    if engine not in ENGINES:
        raise ToolError(f"unknown engine {engine!r}; one of {', '.join(ENGINES)}")
    workers = workers or os.cpu_count() or 1
    run = _Run(schema_for, version_property, engine, workers, on_failure)
    rules = RootRules(version_property, run.shape, version_required=version_property is not None)
    t0 = time.time()
    try:
        streamed = stream_bundle(inp, _Sink(run), rules)
        run.drain(0)
        run.close()
    except BaseException:
        run.abort()
        raise
    run.result.seconds = time.time() - t0
    run.result.bundle_sha256 = streamed.sha256
    if run.result.records == 0:
        raise ToolError(f"{inp.name}: no records")
    return run.result


# --- command ----------------------------------------------------------------------------------------------------


def _schema_resolver(bundle_schema: str | None):
    """schema_for(version): the given set, or the default bundle's set for the bundle's own version."""
    cache: dict[str, tuple[SchemaSet, str]] = {}

    def schema_for(version: str) -> tuple[SchemaSet, str]:
        if version not in cache:
            if bundle_schema is not None:
                source = bundle_schema
            else:
                from .default_pairing import schema_source

                source = schema_source(version)
            # A file or URL is fetched and cached here, so each worker reads it from the cache.
            cache[version] = load_schema_set(source), source
        return cache[version]

    return schema_for


def _format(f: dict) -> str:
    errs = "; ".join(f"{e['path'] or '/'}: {e['message'][:200]}" for e in f["errors"][:3])
    more = f" (+{len(f['errors']) - 3} more)" if len(f["errors"]) > 3 else ""
    key = f"/{f['key']}" if f["key"] is not None else ""
    return f"{f['section']}{key}: {errs}{more}"


def run_validate_command(args) -> int:
    from . import sources
    from .default_pairing import version_property

    index = sources.load_index(args.index) if args.index else None
    inp = sources.resolve_input(args.bundle, sha256=args.sha256, index=index)
    failures_out = open(args.failures, "w", encoding="utf-8") if args.failures else None
    printed = 0

    def on_failure(f: dict) -> None:
        nonlocal printed
        if failures_out is not None:
            failures_out.write(json.dumps(f, ensure_ascii=False) + "\n")
        if printed < args.max_print:
            print(f"  FAIL {_format(f)}", file=sys.stderr)
            printed += 1

    try:
        result = validate_bundle(
            inp,
            _schema_resolver(args.bundle_schema),
            args.version_property or version_property(),
            engine=args.engine,
            workers=args.workers,
            on_failure=on_failure,
        )
    finally:
        if failures_out is not None:
            failures_out.close()
    print(f"bundle: {inp.name} (sha256 {result.bundle_sha256}, {inp.sha256_source or 'not checked'})")
    print(f"version: {result.version}; schema set sha256 {result.schema_set_sha256}")
    print(f"engine: {result.engine}; {result.workers} workers")
    print(f"  {'section':34s} {'records':>11s} {'failing':>8s} {'cpu ms/rec':>10s} {'compile s':>10s}")
    for name, s in result.sections.items():
        rate = 1000 * s.cpu_seconds / s.records if s.records else 0.0
        print(f"  {name:34s} {s.records:>11,d} {s.failing:>8,d} {rate:>10.3f} {s.compile_seconds:>10.1f}")
    print(f"records: {result.records:,d}  failing: {result.failing:,d}  ({result.seconds:,.1f} s)")
    if args.json:
        Path(args.json).write_text(
            json.dumps(
                {
                    "bundle": inp.name,
                    "bundle_sha256": result.bundle_sha256,
                    "version": result.version,
                    "schema_set_sha256": result.schema_set_sha256,
                    "engine": result.engine,
                    "workers": result.workers,
                    "seconds": round(result.seconds, 3),
                    "sections": {n: vars(s) for n, s in result.sections.items()},
                    "records": result.records,
                    "failing": result.failing,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    print("PASS" if result.failing == 0 else "FAIL")
    return 0 if result.failing == 0 else 1
