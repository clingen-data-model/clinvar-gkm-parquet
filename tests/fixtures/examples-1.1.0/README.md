# examples-1.1.0 (provisional fixture)

A small mini-bundle in the ClinVar-GKM 1.1.0 bundle line format, built from the example records in
ClinVar-GKM's `examples/` folder. It stands in until a real 1.1.0 bundle is published. Replace it with a
real bundle excerpt (design plan §8.1) after the 2026-10-18 1.1.0 bundle is staged.

These are not published bundle bytes. ClinVar-GKM's `src/scripts/extract-example.py` made each example from
one bundle record and inlined its `#/<section>/<key>` pointers. The script here reverses that: each inlined
object goes back into its own section as a record, and its pointer goes back where it was. As a check,
inlining the fixture again with the extractor's rules gives back all 42 examples exactly.

## Files

- `clinvar-gkm_examples.json.gz`: the mini-bundle (about 20 KB). It has 346 records in 17 of the 20
  sections; `copyNumberCount`, `copyNumberChange` and `therapyGroup` are empty.
- `clinvar-gkm_examples.json.gz.sha256`: its checksum, in `sha256sum -c` format.
- `PROVENANCE.json`: the source commit, each example's path and git blob sha, the release dataset the
  examples came from, per-section record counts, keys the script had to compute, and dangling pointers
  (pointers to records that are not in the fixture, such as the `variation` and `scv` pointers the
  examples kept).

## How it differs from a real 1.1.0 bundle

- Record key order is the examples' order (alphabetical, from BigQuery JSON), not the export order.
  Keys are sorted within each section.
- Records are written with the stdlib `json` module, not `orjson`. The output is the same here because
  the records hold no floats.
- The examples come from the `clinvar_2026_10_04_v2_6_0` build, which is older than some 1.1.0 changes.
  Two differences found: `geneContextQualifier` has no `geneContextSource` extension, and the members of
  the `clinvar.conditionset:vcv-…` condition sets are bare CURIEs, where the 1.0 branch code now writes
  `#/condition/…` pointers.
- The one `therapy` key is computed with the rule in `gkm-scv-statement-proc.sql` (sha256 of the
  therapy's BigQuery JSON). No published key was available to check it against.

## Regenerate

Requires `git` and a local ClinVar-GKM clone. The script reads it with `git show` at a fixed commit and
does not change its working tree.

```sh
python3 scripts/make-example-fixtures.py --clinvar-gks ~/Development/gks/clinvar-gks
python3 scripts/make-example-fixtures.py --check   # fails if the files here are stale
cd tests/fixtures/examples-1.1.0 && sha256sum -c clinvar-gkm_examples.json.gz.sha256
```

The output is deterministic: the same commit gives byte-identical files.
