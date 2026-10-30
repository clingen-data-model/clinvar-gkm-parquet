---
description: Python package conventions (layout, dependencies, streaming, fail-loud rules, tests)
paths:
  - "src/**"
  - "tests/**"
  - "pyproject.toml"
---

# Python package conventions

The package layout follows the [design plan](../../docs/plans/2026-10-20-design.md) §11, plus `model.py` (the data
passed between stages, and the staging format), `sources.py` (paths, URLs, checksums, the line index, manifests),
`spec.py` (loads and checks table specs, built-in or custom), `sqltemplate.py` (renders each table's SQL from the
spec: from its `sql/<table>.sql`, or from the spec alone with the generic renderer), `baseline.py` (renders the
baseline spec's id/refs/data tables), `select.py` (the selection engine: pointer walk and mini-bundle),
`driver.py` (wires selection into `build` and `select`) and `roundtrip.py` (rows back to bundle records: the
build's round-trip check, `rebuild` and `verify --roundtrip`; ADR 0005). Update this file when the layout changes.

## Layout and tooling

- `src/` layout, package `clinvar_gkm_parquet`, console script `clinvar-gkm-parquet`.
- Python `>=3.10`. Manage environments with `uv`; lint and format with `ruff`; test with `pytest`.
- Runtime dependencies stay minimal: `duckdb` (pinned to the exact version that defines the output, see below)
  and `PyYAML` (reads `spec/<version>/tables.yaml`). The reader passes records through as bytes and never parses
  them in Python, so there is no JSON speed-up dependency. Anything else needs a reason in the PR.

## Output determinism

- The same tool version, table spec and input bundle(s) must give byte-identical Parquet files. So:
  - pin the exact DuckDB version a release was tested with;
  - write with `ORDER BY` the table's sort key, fixed compression and fixed row-group size;
  - put no wall-clock timestamps in the Parquet key-value metadata (put them in the dataset manifest the tool
    writes beside the files instead; design plan §7.3).
- Declare every output column type explicitly. Never rely on DuckDB's JSON type inference: it differs from
  the table spec on nested struct field order, integer widths and empty-versus-NULL lists.

## Reading bundles

- Bundles are large (a monthly full is about 4.8 GB gzip). **Stream them**; never `json.load` a whole bundle.
  The assembler writes one `"<key>": <record>` per line; the format is verified in the design plan §4.2.
- Compute the sha256 of the compressed bytes while streaming, and check it against the published checksum
  (`<file>.sha256` sidecar, the line `index.json`, or a frozen line's `MANIFEST.json`) when one is available.
  Refuse on a mismatch.
- Read `schema_version` from the bundle itself and refuse a version the tool doesn't list.

## Fail loudly

- An unknown bundle section, an unknown extension name, a record that doesn't match the table spec, a broken
  delta chain: stop with a clear error naming the release, section and key. Never skip or drop silently.

## Network

- Every HTTP request sends `User-Agent: clinvar-gkm-parquet/<version>`.
- Respect the CDN limit of 250 requests per 10 s: one stream per large file, retry 429 and 5xx with backoff,
  resume interrupted downloads with HTTP `Range`.
- Unit tests never touch the network. Network tests are opt-in (`pytest -m network`).

## Tests

- Fixtures are small excerpts of real published bundles, made by a checked-in script that records the source
  URL, release, schema_version and source sha256. Don't hand-write bundle records.
- Golden expectations come from the real published Parquet for the same release and ids, not from the code
  under test.
