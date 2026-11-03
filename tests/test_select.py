"""select.py (driver-based selection, ADR 0004) on the real 1.1.0 excerpt, against an independent recompute.

The expected selection comes from a graph built here with stdlib json (nodes are (section, key); edges come from
`#/<section>/<key>` string values found by walking the parsed records) and a plain breadth-first search per follow
mode, depth limit and section allowlist. The output is parsed line by line here too, independently of the engine
and of bundle.py.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path

import pytest
from _fixture import FIXTURE

from clinvar_gkm_parquet import select as sel
from clinvar_gkm_parquet.bundle import SECTIONS, stage_bundle
from clinvar_gkm_parquet.errors import BundleFormatError, InputError, SchemaVersionError
from clinvar_gkm_parquet.model import InputFile

Node = tuple[str, str]


# ---------------------------------------------------------------------------------------------------------------
# The independent model


def parse_lines(path: Path) -> tuple[list[str], dict[str, list[tuple[bytes, bytes]]]]:
    """(section order, {section: [(key JSON bytes, record bytes)]}) from the bundle lines, with stdlib json."""
    order: list[str] = []
    out: dict[str, list[tuple[bytes, bytes]]] = {}
    section = None
    dec = json.JSONDecoder()
    for line in gzip.decompress(path.read_bytes()).split(b"\n"):
        if line.startswith(b'  "') and line.endswith(b": {"):
            section = json.loads(line.strip()[:-3])
            order.append(section)
            out[section] = []
        elif line.startswith(b"    "):
            text = line[4:].decode()
            _, end = dec.raw_decode(text)
            key = text[:end].encode()
            out[section].append((key, line[4 + len(key) + 2 :].removesuffix(b",")))
    return order, out


def walk_pointers(node, found: list[str]) -> list[str]:
    if isinstance(node, str):
        if node.startswith("#/"):
            found.append(node)
    elif isinstance(node, dict):
        for v in node.values():
            walk_pointers(v, found)
    elif isinstance(node, list):
        for v in node:
            walk_pointers(v, found)
    return found


class Graph:
    def __init__(self, path: Path):
        bundle = json.loads(gzip.decompress(path.read_bytes()))
        self.nodes: set[Node] = set()
        self.out: dict[Node, set[Node]] = defaultdict(set)  # including targets the bundle doesn't hold
        self.inn: dict[Node, set[Node]] = defaultdict(set)
        for s, recs in bundle.items():
            if s == "schema_version":
                continue
            for k in recs:
                self.nodes.add((s, k))
        for s, recs in bundle.items():
            if s == "schema_version":
                continue
            for k, rec in recs.items():
                for p in walk_pointers(rec, []):
                    t = tuple(p[2:].split("/", 1))
                    self.out[(s, k)].add(t)
                    if t in self.nodes:
                        self.inn[t].add((s, k))

    def bfs(self, seeds, follow: str, max_depth: int | None, allowed: set[str] | None):
        """(depth by node, dangling pointers) of a plain breadth-first search."""
        ok = (lambda n: True) if allowed is None else (lambda n: n[0] in allowed)
        depth = {n: 0 for n in seeds}
        frontier = sorted(seeds)
        dangling = set()
        d = 0
        while frontier and (max_depth is None or d < max_depth):
            nxt = []
            for n in frontier:
                nbrs = set()
                if follow in ("out", "both"):
                    for t in self.out[n]:
                        if not ok(t):
                            continue
                        if t in self.nodes:
                            nbrs.add(t)
                        else:
                            dangling.add(f"#/{t[0]}/{t[1]}")
                if follow in ("in", "both"):
                    nbrs |= {r for r in self.inn[n] if ok(r)}
                for t in sorted(nbrs):
                    if t not in depth:
                        depth[t] = d + 1
                        nxt.append(t)
            frontier = nxt
            d += 1
        return depth, dangling


GRAPH = Graph(FIXTURE)
SOURCE_ORDER, SOURCE_LINES = parse_lines(FIXTURE)
FIXTURE_SHA = hashlib.sha256(FIXTURE.read_bytes()).hexdigest()


def fixture_input(path: Path = FIXTURE, sha: str | None = FIXTURE_SHA) -> InputFile:
    return InputFile(str(path), path.name, "2026-10-18", sha, "explicit" if sha else None, path.stat().st_size)


def seed_of(section: str) -> str:
    """The first key of `section` that has both incoming and outgoing pointers (so every mode moves)."""
    keys = [json.loads(k) for k, _ in SOURCE_LINES[section]]
    return next(k for k in keys if GRAPH.inn[(section, k)] and GRAPH.out[(section, k)])


def run(tmp_path: Path, seeds: dict, name: str = "selected.json.gz", src=None, **kw) -> sel.SelectionResult:
    return sel.select(src if src is not None else fixture_input(), seeds, tmp_path / name, **kw)


def output_nodes(path: Path) -> dict[str, list[tuple[bytes, bytes]]]:
    return parse_lines(path)[1]


def check_against_bfs(result: sel.SelectionResult, seeds: set[Node], follow, max_depth, allowed=None) -> None:
    want_depth, want_dangling = GRAPH.bfs(seeds, follow, max_depth, allowed)
    got = output_nodes(result.path)
    got_nodes = {(s, json.loads(k)) for s, recs in got.items() for k, _ in recs}
    assert got_nodes == set(want_depth)
    assert result.depth_counts == dict(sorted(Counter(want_depth.values()).items()))
    assert result.section_counts == {s: len(recs) for s, recs in got.items()}
    assert result.records == len(want_depth)
    assert {d.pointer for d in result.dangling} == want_dangling


# ---------------------------------------------------------------------------------------------------------------
# Selection against the breadth-first search


@pytest.mark.parametrize("max_depth", [0, 1, 2, None])
@pytest.mark.parametrize("follow", ["out", "in", "both"])
@pytest.mark.parametrize("section", ["vcv", "variation", "scv"])
def test_selection_matches_bfs(tmp_path, section, follow, max_depth):
    key = seed_of(section)
    result = run(tmp_path, {section: {key}}, follow=follow, max_depth=max_depth)
    check_against_bfs(result, {(section, key)}, follow, max_depth)
    assert result.parameters["follow"] == follow and result.parameters["max_depth"] == max_depth


def test_several_seeds(tmp_path):
    keys = [json.loads(k) for k, _ in SOURCE_LINES["variation"][:5]]
    result = run(tmp_path, {"variation": keys}, follow="both", max_depth=2)
    check_against_bfs(result, {("variation", k) for k in keys}, "both", 2)
    assert result.parameters["seeds"] == {"variation": {"count": 5, "sha256": sel.ids_sha256(keys)}}


def test_out_follows_pointers_down_to_sequence_reference(tmp_path):
    result = run(tmp_path, {"vcv": [seed_of("vcv")]})
    assert {"vcv", "variation", "allele", "location", "sequenceReference"} <= set(result.section_counts)
    assert result.dangling == ()


# ---------------------------------------------------------------------------------------------------------------
# The section allowlist


def test_allowlist_cuts_the_traversal(tmp_path):
    key = seed_of("vcv")
    allowed = [s for s in SECTIONS if s != "variation"]
    result = run(tmp_path, {"vcv": [key]}, sections=allowed)
    check_against_bfs(result, {("vcv", key)}, "out", None, set(allowed))
    full = run(tmp_path, {"vcv": [key]}, name="full.json.gz")
    assert "variation" in full.section_counts and "allele" in full.section_counts
    assert "variation" not in result.section_counts and "allele" not in result.section_counts
    # The pointer into the unlisted section stays in the record bytes, and is not dangling.
    out = output_nodes(result.path)
    assert any(b'"#/variation/' in rec for s, recs in out.items() for _, rec in recs)
    assert result.dangling == ()
    assert result.parameters["sections"] == allowed


@pytest.mark.parametrize("follow", ["in", "both"])
def test_allowlist_limits_incoming_too(tmp_path, follow):
    key = seed_of("variation")
    allowed = {"variation", "allele", "location", "sequenceReference", "propositionPathogenicity"}
    result = run(tmp_path, {"variation": [key]}, follow=follow, sections=sorted(allowed))
    check_against_bfs(result, {("variation", key)}, follow, None, allowed)
    assert set(result.section_counts) <= allowed


def test_driver_section_must_be_in_the_allowlist(tmp_path):
    with pytest.raises(sel.SelectionError, match="driver section 'vcv' must be one of the selected sections"):
        run(tmp_path, {"vcv": [seed_of("vcv")]}, sections=["variation", "allele"])
    assert not any(tmp_path.iterdir())


@pytest.mark.parametrize(
    ("kw", "match"),
    [
        ({"sections": ["vcv", "nosuch"]}, "unknown section"),
        ({"sections": "vcv"}, "list of section names"),
        ({"follow": "sideways"}, "follow must be out, in or both"),
        ({"max_depth": -1}, "max_depth must be a non-negative integer"),
    ],
)
def test_bad_parameters(tmp_path, kw, match):
    with pytest.raises(sel.SelectionError, match=match):
        run(tmp_path, {"vcv": [seed_of("vcv")]}, **kw)


def test_no_driver_keys_fails(tmp_path):
    with pytest.raises(sel.SelectionError, match="no driver keys"):
        run(tmp_path, {"vcv": set()})
    with pytest.raises(sel.SelectionError, match="unknown driver section"):
        run(tmp_path, {"nosuch": {"x"}})


# ---------------------------------------------------------------------------------------------------------------
# Seeds, validity, determinism


def test_unknown_seeds_fail_listing_them(tmp_path):
    real = seed_of("vcv")
    missing = [f"VCV{i:09d}.1-G-x" for i in range(12)]
    with pytest.raises(sel.SelectionError) as e:
        run(tmp_path, {"vcv": [real, *missing]})
    msg = str(e.value)
    assert "12 driver key(s) not in clinvar-gkm_excerpt.json.gz" in msg and "and 2 more" in msg
    assert all(f"vcv:{k}" in msg for k in sorted(missing)[: sel.MAX_LISTED])
    assert real not in msg
    assert not any(tmp_path.iterdir())


def test_output_is_verbatim_in_source_order(tmp_path):
    result = run(tmp_path, {"scv": [seed_of("scv")]}, follow="both", max_depth=3)
    order, out = parse_lines(result.path)
    assert order == [s for s in SOURCE_ORDER if s in out]
    for section, recs in out.items():
        position = {k: i for i, (k, _) in enumerate(SOURCE_LINES[section])}
        source = dict(SOURCE_LINES[section])
        assert [position[k] for k, _ in recs] == sorted(position[k] for k, _ in recs)
        assert all(source[k] == r for k, r in recs)
    assert gzip.decompress(result.path.read_bytes()).startswith(b'{\n  "schema_version": "1.1.0",\n')


def test_output_passes_stage_bundle(tmp_path):
    result = run(tmp_path, {"vcv": [seed_of("vcv")]}, follow="both", max_depth=2)
    assert hashlib.sha256(result.path.read_bytes()).hexdigest() == result.sha256
    assert result.path.stat().st_size == result.size
    inp = InputFile(str(result.path), result.path.name, None, result.sha256, "explicit", result.size)
    staged = stage_bundle(inp, tmp_path / "work")
    assert {s: x.records for s, x in staged.sections.items()} == result.section_counts
    assert staged.schema_version == result.schema_version == "1.1.0"
    assert not [p for p in tmp_path.iterdir() if p.name.startswith(".")]  # no work or partial files left


def test_byte_identical_across_runs(tmp_path):
    key = seed_of("variation")
    a = run(tmp_path, {"variation": [key]}, name="a.json.gz", follow="both", max_depth=3)
    b = run(tmp_path, {"variation": [key]}, name="b.json.gz", follow="both", max_depth=3)
    assert a.path.read_bytes() == b.path.read_bytes() and a.sha256 == b.sha256
    assert a.path.read_bytes()[4:8] == b"\0\0\0\0"  # gzip mtime 0
    assert a.metadata_json() == b.metadata_json()


def test_source_checks(tmp_path):
    result = run(tmp_path, {"vcv": [seed_of("vcv")]})
    assert result.source_sha256 == FIXTURE_SHA and result.source_name == FIXTURE.name
    assert result.source_records == {s: len(r) for s, r in SOURCE_LINES.items()}
    with pytest.raises(InputError, match="sha256 mismatch"):
        run(tmp_path, {"vcv": [seed_of("vcv")]}, name="bad.json.gz", src=fixture_input(sha="0" * 64))
    assert not (tmp_path / "bad.json.gz").exists()


def test_unsupported_schema_version_fails(tmp_path):
    text = gzip.decompress(FIXTURE.read_bytes()).replace(
        b'"schema_version": "1.1.0"', b'"schema_version": "9.9.9"'
    )
    path = tmp_path / "v9.json.gz"
    path.write_bytes(gzip.compress(text, mtime=0))
    with pytest.raises(SchemaVersionError, match="'9.9.9' is not supported"):
        run(tmp_path, {"vcv": [seed_of("vcv")]}, src=fixture_input(path, None))


def test_reopened_source_must_match_pass_1(tmp_path):
    other = tmp_path / "other.json.gz"
    other.write_bytes(gzip.compress(gzip.decompress(FIXTURE.read_bytes()), compresslevel=1, mtime=0))
    src = sel.BundleFileSource(fixture_input(), reopen=lambda n: open(other, "rb"), checkpoint_bytes=1000)
    with pytest.raises(sel.SourceChangedError, match="re-opened source differs from the pass-1 stream"):
        run(tmp_path, {"vcv": [seed_of("vcv")]}, src=src)
    same = sel.BundleFileSource(fixture_input(), reopen=lambda n: open(FIXTURE, "rb"), checkpoint_bytes=1000)
    assert run(tmp_path, {"vcv": [seed_of("vcv")]}, src=same).records > 1


def test_later_passes_stop_early(tmp_path):
    result = run(tmp_path, {"vcv": [seed_of("vcv")]})
    first, *later = result.passes
    assert first.stopped_before is None and first.records_read == sum(result.source_records.values())
    assert first.bytes_read == FIXTURE.stat().st_size
    assert later and all(p.stopped_before for p in later)
    assert [p.depth_limit for p in result.passes] == list(range(1, len(result.passes) + 1))
    assert sum(p.selected for p in result.passes) == result.records


# ---------------------------------------------------------------------------------------------------------------
# Dangling pointers


def _without(path: Path, section: str, key: str) -> Path:
    """A copy of the excerpt with one record removed (keeping the line grammar)."""
    lines = gzip.decompress(FIXTURE.read_bytes()).split(b"\n")
    target = b"    " + json.dumps(key).encode() + b": "
    i = next(i for i, line in enumerate(lines) if line.startswith(target))
    if not lines[i].endswith(b","):  # the section's last record: the one before it loses its comma
        lines[i - 1] = lines[i - 1].removesuffix(b",")
    del lines[i]
    path.write_bytes(gzip.compress(b"\n".join(lines), mtime=0))
    return path


def test_dangling_pointer_is_reported_not_fatal(tmp_path):
    key = seed_of("vcv")
    depth, _ = GRAPH.bfs({("vcv", key)}, "out", None, None)
    location = next(n for n in sorted(depth) if n[0] == "location")
    referrers = sorted(n for n in depth if location in GRAPH.out[n])
    src = _without(tmp_path / "cut.json.gz", *location)
    result = run(tmp_path, {"vcv": [key]}, src=fixture_input(src, None))
    pointer = f"#/location/{location[1]}"
    assert [d.pointer for d in result.dangling] == [pointer]
    assert (result.dangling[0].section, result.dangling[0].key) in referrers
    got = output_nodes(result.path)
    assert location[1] not in {json.loads(k) for k, _ in got.get("location", [])}
    # Everything else the vcv reaches is still there; the location's own targets are not reachable now.
    cut = Graph(src)
    want, want_dangling = cut.bfs({("vcv", key)}, "out", None, None)
    assert {(s, json.loads(k)) for s, recs in got.items() for k, _ in recs} == set(want)
    assert want_dangling == {pointer}


def test_dangling_pointer_into_an_unlisted_section_is_not_reported(tmp_path):
    key = seed_of("vcv")
    depth, _ = GRAPH.bfs({("vcv", key)}, "out", None, None)
    location = next(n for n in sorted(depth) if n[0] == "location")
    src = _without(tmp_path / "cut.json.gz", *location)
    allowed = [s for s in SECTIONS if s not in ("location", "sequenceReference")]
    result = run(tmp_path, {"vcv": [key]}, src=fixture_input(src, None), sections=allowed)
    assert result.dangling == ()


# ---------------------------------------------------------------------------------------------------------------
# Record sources other than a bundle file (a replayed delta state)


def _records(path: Path):
    order, lines = parse_lines(path)
    return [(s, k, r) for s in order for k, r in lines[s]]


def test_iter_record_source_gives_the_same_bundle(tmp_path):
    key = seed_of("variation")
    from_file = run(tmp_path, {"variation": [key]}, name="file.json.gz", follow="both", max_depth=3)
    rows = _records(FIXTURE)
    calls = []

    def records(n):
        calls.append(n)
        return iter(rows)

    src = sel.IterRecordSource("replayed-state", "1.1.0", records)
    from_iter = run(tmp_path, {"variation": [key]}, name="iter.json.gz", src=src, follow="both", max_depth=3)
    assert from_iter.path.read_bytes() == from_file.path.read_bytes()
    assert calls == list(range(1, len(from_iter.passes) + 1))
    assert from_iter.source_sha256 is None and all(p.bytes_read is None for p in from_iter.passes)


def test_iter_record_source_checks(tmp_path):
    rows = _records(FIXTURE)
    key = seed_of("vcv")
    gene = rows.index(next(r for r in rows if r[0] == "gene"))
    split = rows[:gene] + rows[gene + 1 :] + [rows[gene]]
    shuffled = sel.IterRecordSource("x", "1.1.0", lambda n: rows[-1:] + rows[:-1])
    with pytest.raises(BundleFormatError, match="appears twice"):
        run(tmp_path, {"vcv": [key]}, src=shuffled)
    with pytest.raises(BundleFormatError, match="appears twice"):
        run(tmp_path, {"vcv": [key]}, src=sel.IterRecordSource("x", "1.1.0", lambda n: split))
    with pytest.raises(SchemaVersionError):
        run(tmp_path, {"vcv": [key]}, src=sel.IterRecordSource("x", "9.9.9", lambda n: rows))
    changing = sel.IterRecordSource("x", "1.1.0", lambda n: rows if n == 1 else rows[1:])
    with pytest.raises(sel.SourceChangedError):
        run(tmp_path, {"vcv": [key]}, src=changing)


# ---------------------------------------------------------------------------------------------------------------
# Provenance


def test_parameters_and_metadata(tmp_path):
    key = seed_of("vcv")
    result = run(
        tmp_path,
        {"vcv": [key]},
        follow="both",
        max_depth=1,
        provenance={"driver": "vcv", "where": "id = 'x'", "ids_sha256": sel.ids_sha256([key])},
    )
    p = result.parameters
    assert p == {
        "driver": "vcv",
        "where": "id = 'x'",
        "ids_sha256": sel.ids_sha256([key]),
        "follow": "both",
        "max_depth": 1,
        "sections": None,
        "seeds": {"vcv": {"count": 1, "sha256": sel.ids_sha256([key])}},
        "selected_records": result.records,
        "selected_sha256": result.sha256,
    }
    assert json.loads(result.metadata_json()) == p
    assert sel.SELECTION_METADATA_KEY == "clinvar_gkm_parquet.selection"
    assert sel.ids_sha256(["b", "a", "a"]) == hashlib.sha256(b"a\nb\n").hexdigest()
    with pytest.raises(sel.SelectionError, match="set by the engine"):
        run(tmp_path, {"vcv": [key]}, name="x.json.gz", provenance={"follow": "in"})


# ---------------------------------------------------------------------------------------------------------------
# The pointer scan


def test_pointer_scan_matches_the_parsed_records():
    for section, recs in SOURCE_LINES.items():
        for key, rec in recs:
            want = [tuple(p[2:].split("/", 1)) for p in walk_pointers(json.loads(rec), [])]
            got = [(SECTIONS[si], json.loads(b'"' + k + b'"')) for si, k in sel.parse_pointers(rec)]
            assert got == want, (section, key)


def test_pointer_text_inside_a_string_is_not_a_pointer():
    """A real record changed minimally: a free-text value quoting a pointer, before a real pointer."""
    key, rec = next((k, r) for k, r in SOURCE_LINES["scv"] if b'"#/submitter/' in r)
    obj = json.loads(rec)
    obj = {"note": 'see "#/vcv/VCV0.1-G-x" and #/gene/ncbigene:0', **obj}
    text = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode()
    got = [SECTIONS[si] for si, _ in sel.parse_pointers(text)]
    assert "vcv" not in got and "gene" not in got and "submitter" in got
    assert got == [p[2:].split("/", 1)[0] for p in walk_pointers(json.loads(rec), [])]
    with pytest.raises(BundleFormatError, match='unknown section: "#/nosuch/x"'):
        sel.parse_pointers(json.dumps({**obj, "x": "#/nosuch/x"}, separators=(",", ":")).encode())
