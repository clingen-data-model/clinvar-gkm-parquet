"""bundle.stage_bundle: the §4.2 grammar, checksums, schema_version and the staging format."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest
from _fixture import STAGING_NAME
from _input_helpers import (
    FIXTURE,
    PROVENANCE,
    bundle_bytes,
    fixture_sections,
    local_input,
    sha256_of,
    write_gz,
)

from clinvar_gkm_parquet.bundle import SECTIONS, stage_bundle
from clinvar_gkm_parquet.errors import BundleFormatError, InputError, SchemaVersionError
from clinvar_gkm_parquet.sources import resolve_input

# ---------------------------------------------------------------------------------------------------------------
# The fixture (positive)


def test_sections_match_the_bundle_schema():
    schema_sections = [
        "sequenceReference",
        "location",
        "allele",
        "copyNumberCount",
        "copyNumberChange",
        "gene",
        "variation",
        "condition",
        "conditionSet",
        "therapy",
        "therapyGroup",
        "submitter",
        "propositionPathogenicity",
        "propositionOncogenicity",
        "propositionSomaticClinicalImpact",
        "propositionClinvarCustom",
        "evidenceLine",
        "scv",
        "vcv",
        "rcv",
    ]  # clinvar-gkm-bundle.schema.json @ ClinVar-GKM origin/1.0 (25344ba), in assemble-gkm-dicts.py order
    assert list(SECTIONS) == schema_sections


def test_stage_fixture(tmp_path):
    inp = resolve_input(str(FIXTURE))
    assert inp.sha256_source == "sidecar"
    staged = stage_bundle(inp, tmp_path)
    assert staged.sha256 == inp.expected_sha256 == sha256_of(FIXTURE)
    assert staged.size == FIXTURE.stat().st_size
    assert staged.schema_version == "1.1.0"
    counts = json.loads(PROVENANCE.read_text())
    expected = {s: n for s, n in _provenance_counts(counts).items() if n}
    assert {s: sec.records for s, sec in staged.sections.items()} == expected


def _provenance_counts(prov: dict) -> dict[str, int]:
    for key in ("section_counts", "counts", "records"):
        if isinstance(prov.get(key), dict):
            return prov[key]
    raise AssertionError(f"no per-section counts in PROVENANCE.json: {sorted(prov)}")


def test_staging_is_key_tab_exact_record_bytes(tmp_path):
    staged = stage_bundle(resolve_input(str(FIXTURE)), tmp_path)
    for section, recs in fixture_sections().items():
        sec = staged.sections[section]
        assert sec.path == tmp_path / STAGING_NAME / f"{section}.tsv.gz"
        lines = gzip.decompress(sec.path.read_bytes()).split(b"\n")
        assert lines[-1] == b""
        assert lines[:-1] == [k + b"\t" + r for k, r in recs]
        assert sec.max_line_bytes == max(len(k) + len(r) + 2 for k, r in recs)


def test_no_partial_folders_left(tmp_path):
    stage_bundle(resolve_input(str(FIXTURE)), tmp_path)
    assert [p.name for p in tmp_path.iterdir()] == [STAGING_NAME]


def test_restage_replaces(tmp_path):
    inp = resolve_input(str(FIXTURE))
    stage_bundle(inp, tmp_path)
    stale = tmp_path / STAGING_NAME / "stale.tsv.gz"
    stale.write_bytes(b"")
    stage_bundle(inp, tmp_path)
    assert not stale.exists()


def test_bundle_with_no_sections(tmp_path):
    # A zero-change delta: the assembler writes the header without a trailing comma, then "}".
    path = write_gz(
        tmp_path / "in" / "clinvar-gkm-delta_2026-1025.json.gz", b'{\n  "schema_version": "1.1.0"\n}\n'
    )
    staged = stage_bundle(local_input(path), tmp_path / "work")
    assert staged.sections == {}


def test_key_with_escapes_is_kept_as_json(tmp_path):
    key, record = fixture_sections()["gene"][0]
    odd = b'"synthetic \\"q\\" \\\\ caf\\u00e9 \xc3\xbc"'
    path = write_gz(tmp_path / "in" / "b.json.gz", bundle_bytes({"gene": [(odd, record), (key, record)]}))
    staged = stage_bundle(local_input(path), tmp_path / "work")
    lines = gzip.decompress(staged.sections["gene"].path.read_bytes()).split(b"\n")
    assert lines[0] == odd + b"\t" + record


def test_warns_without_checksum(tmp_path, caplog):
    path = write_gz(tmp_path / "in" / "b.json.gz", bundle_bytes({"gene": fixture_sections()["gene"]}))
    staged = stage_bundle(local_input(path), tmp_path / "work")
    assert staged.sha256 == sha256_of(path)
    assert "no published sha256" in caplog.text and staged.sha256 in caplog.text


# ---------------------------------------------------------------------------------------------------------------
# Checksums


def test_checksum_mismatch_stages_nothing(tmp_path):
    inp = local_input(FIXTURE, sha="0" * 64)
    with pytest.raises(InputError, match="sha256 mismatch"):
        stage_bundle(inp, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_checksum_mismatch_keeps_earlier_staging_untouched_by_partial(tmp_path):
    stage_bundle(resolve_input(str(FIXTURE)), tmp_path)
    with pytest.raises(InputError):
        stage_bundle(local_input(FIXTURE, sha="0" * 64), tmp_path)
    assert [p.name for p in tmp_path.iterdir()] == [STAGING_NAME]


def test_truncated_gzip_reports_checksum_mismatch(tmp_path):
    data = FIXTURE.read_bytes()
    cut = tmp_path / "in" / "cut.json.gz"
    cut.parent.mkdir()
    cut.write_bytes(data[: len(data) // 2])
    with pytest.raises(InputError, match="sha256 mismatch.*gzip stream is also invalid"):
        stage_bundle(local_input(cut, sha=sha256_of(FIXTURE)), tmp_path / "work")
    with pytest.raises(BundleFormatError, match="not a complete gzip stream"):
        stage_bundle(local_input(cut), tmp_path / "work")
    assert list((tmp_path / "work").iterdir()) == []


def test_format_error_with_known_checksum_reports_mismatch(tmp_path):
    # A valid gzip of the wrong content (a grammar-test byte string, not a record): with a checksum known, the
    # mismatch is reported, not the format error it also causes.
    wrong = tmp_path / "in" / "wrong.json.gz"
    wrong.parent.mkdir()
    wrong.write_bytes(gzip.compress(b'{\n  "schema_version": "1.1.0",\n  not a section line\n}\n', mtime=0))
    with pytest.raises(InputError, match="sha256 mismatch.*also fails to parse"):
        stage_bundle(local_input(wrong, sha=sha256_of(FIXTURE)), tmp_path / "work")
    with pytest.raises(BundleFormatError):
        stage_bundle(local_input(wrong), tmp_path / "work")


def test_not_gzip(tmp_path):
    path = tmp_path / "plain.json.gz"
    path.write_bytes(gzip.decompress(FIXTURE.read_bytes()))
    with pytest.raises(BundleFormatError, match="gzip"):
        stage_bundle(local_input(path), tmp_path / "work")


# ---------------------------------------------------------------------------------------------------------------
# schema_version


@pytest.mark.parametrize("version", ["1.0.1", "1.2.0", ""])
def test_unsupported_schema_version(tmp_path, version):
    path = write_gz(tmp_path / "b.json.gz", bundle_bytes({"gene": fixture_sections()["gene"]}, version))
    with pytest.raises(SchemaVersionError) as e:
        stage_bundle(local_input(path), tmp_path / "work")
    assert "1.1.0" in str(e.value) and "compatibility matrix" in str(e.value)


def test_missing_schema_version(tmp_path):
    text = bundle_bytes({"gene": fixture_sections()["gene"]}).replace(b'  "schema_version": "1.1.0",\n', b"")
    path = write_gz(tmp_path / "b.json.gz", text)
    with pytest.raises(SchemaVersionError, match="line 2"):
        stage_bundle(local_input(path), tmp_path / "work")


# ---------------------------------------------------------------------------------------------------------------
# Grammar (negative controls). The records are real fixture records; only the layout around them is altered.


def _two_sections() -> bytes:
    s = fixture_sections()
    return bundle_bytes({"gene": s["gene"], "submitter": s["submitter"][:3]})


def _stage_text(tmp_path: Path, text: bytes):
    path = write_gz(tmp_path / "in" / "bad.json.gz", text)
    return stage_bundle(local_input(path), tmp_path / "work")


def _replace_line(text: bytes, lineno: int, new: bytes | None) -> bytes:
    lines = text.split(b"\n")
    if new is None:
        del lines[lineno - 1]
    else:
        lines[lineno - 1] = new
    return b"\n".join(lines)


def _line_of(text: bytes, prefix: bytes) -> int:
    return next(i for i, ln in enumerate(text.split(b"\n"), 1) if ln.startswith(prefix))


def test_first_line_must_be_brace(tmp_path):
    with pytest.raises(BundleFormatError, match="line 1"):
        _stage_text(tmp_path, b"[" + _two_sections()[1:])


def test_empty_file(tmp_path):
    with pytest.raises(BundleFormatError, match="empty file"):
        _stage_text(tmp_path, b"")


def test_unknown_section(tmp_path):
    text = _two_sections().replace(b'  "submitter": {', b'  "proposition": {')
    with pytest.raises(BundleFormatError) as e:
        _stage_text(tmp_path, text)
    msg = str(e.value)
    line = _line_of(text, b'  "proposition": {')
    assert f"bad.json.gz: line {line}:" in msg and "unknown section 'proposition'" in msg


def test_repeated_section(tmp_path):
    text = _two_sections().replace(b'  "submitter": {', b'  "gene": {')
    with pytest.raises(BundleFormatError, match="section 'gene' appears twice"):
        _stage_text(tmp_path, text)


def test_missing_comma_between_records(tmp_path):
    text = _two_sections()
    n = _line_of(text, b"    ")  # first record of gene
    line = text.split(b"\n")[n - 1]
    with pytest.raises(BundleFormatError, match=r"line 5, section gene: missing ','"):
        _stage_text(tmp_path, _replace_line(text, n, line[:-1]))


def test_trailing_comma_before_section_close(tmp_path):
    text = _two_sections()
    n = _line_of(text, b"  },")
    line = text.split(b"\n")[n - 2]
    with pytest.raises(BundleFormatError, match="trailing ',' after the section's last record"):
        _stage_text(tmp_path, _replace_line(text, n - 1, line + b","))


def test_trailing_comma_after_last_section(tmp_path):
    text = _two_sections().replace(b"\n  }\n}\n", b"\n  },\n}\n")
    with pytest.raises(BundleFormatError, match="trailing ',' after the last section"):
        _stage_text(tmp_path, text)


def test_missing_comma_before_section(tmp_path):
    text = _two_sections().replace(b"  },\n", b"  }\n")
    with pytest.raises(BundleFormatError, match="missing ',' before this section"):
        _stage_text(tmp_path, text)


def test_trailing_garbage(tmp_path):
    with pytest.raises(BundleFormatError, match="trailing data"):
        _stage_text(tmp_path, _two_sections() + b"\n")


def test_truncated_inside_section(tmp_path):
    text = _two_sections()
    cut = b"\n".join(text.split(b"\n")[:5]) + b"\n"
    with pytest.raises(BundleFormatError, match="truncated: the file ends inside section gene"):
        _stage_text(tmp_path, cut)


def test_truncated_mid_line(tmp_path):
    text = _two_sections()
    n = _line_of(text, b"    ")
    cut = b"\n".join(text.split(b"\n")[:n])[:-10]
    with pytest.raises(BundleFormatError, match="last line has no newline"):
        _stage_text(tmp_path, cut)


def test_truncated_before_closing_brace(tmp_path):
    with pytest.raises(BundleFormatError, match="truncated: the file ends before the closing"):
        _stage_text(tmp_path, _two_sections()[:-2])


def test_unclassifiable_line(tmp_path):
    text = _two_sections()
    n = _line_of(text, b"    ")
    with pytest.raises(BundleFormatError, match="expected a record line"):
        _stage_text(tmp_path, _replace_line(text, n + 1, b"  junk"))


def test_record_split_over_lines(tmp_path):
    key, record = fixture_sections()["gene"][0]
    pretty = json.dumps(json.loads(record), indent=2).encode().replace(b"\n", b"\n    ")
    text = bundle_bytes({"gene": [(key, pretty)]})
    with pytest.raises(BundleFormatError) as e:
        _stage_text(tmp_path, text)
    assert "line 4, section gene, key " + key.decode() in str(e.value)


def test_malformed_key(tmp_path):
    text = _two_sections()
    n = _line_of(text, b"    ")
    line = text.split(b"\n")[n - 1]
    with pytest.raises(BundleFormatError, match="malformed record line"):
        _stage_text(tmp_path, _replace_line(text, n, line.replace(b'": ', b'":', 1)))


def test_empty_section(tmp_path):
    text = _two_sections().replace(b'  "gene": {\n', b'  "gene": {\n  },\n  "location": {\n')
    # gene is now empty; its old records follow under location, so the first error is the empty section.
    with pytest.raises(BundleFormatError, match="line 4, section gene: empty section"):
        _stage_text(tmp_path, text)


def test_raw_tab_in_line(tmp_path):
    key, record = fixture_sections()["gene"][0]
    text = bundle_bytes({"gene": [(key, record[:-1] + b"\t}")]})
    with pytest.raises(BundleFormatError, match="raw tab"):
        _stage_text(tmp_path, text)


def test_error_names_the_key(tmp_path):
    s = fixture_sections()
    key, record = s["gene"][1]
    text = bundle_bytes({"gene": [s["gene"][0], (key, b"[" + record[1:])]})
    with pytest.raises(BundleFormatError) as e:
        _stage_text(tmp_path, text)
    assert f"line 5, section gene, key {key.decode()}: the record is not a JSON object" in str(e.value)
