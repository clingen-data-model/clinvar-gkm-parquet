# real-1.1.0 (real bundle excerpt)

The suite's bundle fixture: a small, connected excerpt of the 2026-10-18 ClinVar-GKM 1.1.0 weekly full, made by
`scripts/extract-fixtures.py` (design plan §8.1). Its source, counts and seeds are in `PROVENANCE.json`. It
replaced the provisional `tests/fixtures/examples-1.1.0/` fixture (de-inlined from ClinVar-GKM's `examples/`,
not published bundle bytes), which was removed on 2026-10-31.

## What the extractor writes here

- `clinvar-gkm_excerpt.json.gz`: the mini-bundle, in the exact line layout of design plan §4.2. Every key and
  record is copied verbatim from the source line (no re-serialization), in the source's section order and the
  source's key order within each section. gzip level 9, mtime 0 and no file name, so two runs on the same input
  give the same bytes. Size limit 1 MB (`--max-bytes`).
- `clinvar-gkm_excerpt.json.gz.sha256`: its checksum, in `sha256sum -c` format.
- `PROVENANCE.json`: the source URL, release and `schema_version`; the source sha256 as computed and as
  published; the tool, table-spec and script versions; the number of passes; each seed, the shapes it covers
  and the size of its closure; every shape's status; per-section counts; dangling pointers (none expected in a
  full); and the shapes not found or over budget.

What goes in: for each shape the table tests need (allele; copy-number count and change; a Range location;
haplotype and genotype; a germline and a somatic VCV; an RCV; a therapy group; the
`submittedNotNormalized` gene-context placeholder; an hgvs repair; an evidence line of each id shape; and one
shape per extension name in `spec/1.0/tables.yaml`'s `extension_slots`), the first matching records whose
closure is at most `--max-seed-bytes`, plus every record they point to, transitively. `--seed section:key`
adds records by hand. After writing, the script re-reads the excerpt with the tool's own
`bundle.stage_bundle` and compares each staged record with the source bytes.

## What it needs

- The repo's environment (`env -u VIRTUAL_ENV uv sync`). No other dependencies.
- One 1.1.0 full bundle: a local file, an `https://` URL, or a stream on stdin. Nothing the size of the bundle
  is written to disk: the extractor writes only the three files above (about 1 MB) and a temporary copy of the
  excerpt for the validation step. Memory stays at about 100 to 200 MiB (an estimate; 50 to 65 MiB measured on
  test inputs).
- The published sha256 (the `.sha256` sidecar, or the `sha256` in the line index `v1/1.1/index.json`).
- Time, an estimate for a 4.9 GB full on the maintainer's laptop: 7 to 9 passes. Pass 1 reads the whole file
  (about 2 to 3 minutes); the next two read nearly all of it (about 1 to 1.5 minutes each); the last few stop
  after the early sections. About 6 to 10 minutes from a local file. Over the network every pass downloads its
  part of the file again (about 4 to 5 full sizes in all), so allow 10 to 20 minutes.

## Why several passes, and why passes 2+ re-open the source

A record mostly points to records in earlier sections (vcv -> proposition -> variation -> allele -> location
-> sequenceReference), and those have already streamed past by the time the pointer is read. So each pass
captures what the previous pass found it needs, until nothing is pending. A later pass stops at the first
section with nothing left to find.

Passes 2+ re-open the source; nothing is staged. A local path or URL is re-opened as is. Stdin can't be read
twice, so with `-` give `--reopen PATH_OR_URL` or `--reopen-cmd COMMAND` (run once per later pass). Staging
instead would not be bounded by the excerpt: which earlier records are needed is known only after reading the
later records that point to them, so pass 1 would have to keep whole sections. Each re-opened stream is checked
against pass 1 (the sha256 of each 64 MiB prefix), so a changed file fails the run.

## How to run

From the repo root. Replace the placeholders with the 2026-10-18 full's published location; resolve it through
the line index (`https://data.clinvar-gkm.org/v1/1.1/index.json`, provisional path) rather than guessing a path.

A local file:

```sh
env -u VIRTUAL_ENV uv run python scripts/extract-fixtures.py /path/to/clinvar-gkm_2026-1018.json.gz \
  --source-url <published URL of the file> --release 2026-10-18 \
  --sha256 "$(cut -d' ' -f1 /path/to/clinvar-gkm_2026-1018.json.gz.sha256)"
```

A URL (each pass streams it again; the tool's HTTP client sends its User-Agent, makes one request per pass and
resumes with `Range`):

```sh
URL=<https URL of the 2026-10-18 full>
env -u VIRTUAL_ENV uv run python scripts/extract-fixtures.py "$URL" --source-url "$URL" --release 2026-10-18 \
  --sha256 "$(curl -sfA 'clinvar-gkm-parquet fixture extractor' "$URL.sha256" | cut -d' ' -f1)"
```

A GCS stream, with no local copy:

```sh
GS=gs://<bucket>/<path>/clinvar-gkm_2026-1018.json.gz
gsutil cat "$GS" | env -u VIRTUAL_ENV uv run python scripts/extract-fixtures.py - \
  --reopen-cmd "gsutil cat $GS" --source-url <published URL of the file> --release 2026-10-18 \
  --sha256 <published sha256>
```

Useful options: `--seed section:key` (repeatable), `--per-shape` (1 to 3, default 2), `--candidates`
(matches kept per shape, default 4), `--max-seed-bytes` (default 131072), `--max-bytes` (default 1000000),
`--out` (default this folder). The run report (each pass's time and bytes read, the total time and the peak
memory) goes to stderr; it is not stored, so the output stays the same from run to run.

## After a run

1. Read the warnings and `PROVENANCE.json`: `dangling_pointers` should be empty for a full, and
   `shapes_not_found` / `shapes_over_budget` say what the excerpt lacks. Add `--seed`s or raise
   `--max-seed-bytes` (within `--max-bytes`) to fill a gap.
2. `cd tests/fixtures/real-1.1.0 && sha256sum -c clinvar-gkm_excerpt.json.gz.sha256`
3. Try the suite on it: `CLINVAR_GKM_FIXTURE=<folder> env -u VIRTUAL_ENV uv run pytest` runs it on an excerpt
   written elsewhere (`--out`) before it replaces this one (`tests/_fixture.py`). Some tests state facts of the
   committed excerpt (its shapes not found, its Range-location count), so a new excerpt may need them updated.

Counts, the staging folder name and the synthetic delta releases follow the fixture's `PROVENANCE.json`.
