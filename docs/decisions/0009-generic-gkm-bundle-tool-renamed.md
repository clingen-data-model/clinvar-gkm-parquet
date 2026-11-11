# 0009 — A generic GKM bundle tool, renamed gkm-bundle-parquet

- **Status:** Proposed
- **Date:** 2026-11-11
- **Plan (if any):** none; the work order is below. Amends [ADR 0001](0001-parquet-derived-from-bundles.md),
  [ADR 0002](0002-independent-tool-versioning.md), [ADR 0004](0004-driver-based-selection.md),
  [ADR 0006](0006-sections-spec-compiled-from-schemas.md) and [ADR 0008](0008-end-state-reversible-default-custom-projections-filters.md).

## Context

The tool began as ClinVar-GKM's bundle → Parquet builder. Since then its core has been made producer-free:

- **The generic core:** the schema-set loader, the compiler and column sets, the two bundle readers, the round trip and
  `validate` name no producer. `tests/test_any_bundle.py` enforces this for the core modules
  ([ADR 0006](0006-sections-spec-compiled-from-schemas.md)).
- **A second producer works.** Two real CIViC bundles from the GKM Toolkit's starter kit build, verify and rebuild. They
  differ from ClinVar-GKM in several ways: no version property, a pretty-printed layout, empty collections, and root
  values that aren't collections (`tests/fixtures/civic/`).
- **ADR 0008 retires the ClinVar-GKM-only table specs:** the `clinvar-gkm` spec and its SQL, the BigQuery oracle,
  `baseline`, and hand-written `tables.yaml` input.

An inventory of the code on 2026-11-11 (main at `4e60f8d`) found the remaining ties to ClinVar-GKM:

1. **ClinVar-GKM's release model.** These live in `sources.py`, `replay.py`, `model.py` and the `--release`, `--index`,
   `--delta`, `--manifest` and `--full-source-release` options:
   - the `https://data.clinvar-gkm.org` line index (`v1/1.1/index.json`);
   - its monthly, weekly and delta files, file-name patterns and `00-latest` aliases;
   - delta manifests and the replay rule, with `ChainError`.
2. **A default producer.** With no `--bundle-schema`, the tool assumes a ClinVar-GKM bundle: `spec/sections/default-bundle.yaml`
   (`version_property: schema_version` and ClinVar-GKM's schema URLs), the shipped 1.1.0 schema snapshot and its column
   set.
3. **Selection.** `select` and `build --driver` expect a `"schema_version"` line and the default bundle's sections, and
   take no `--bundle-schema` (`select.py`, `driver.py`).
4. **`validate`.** It can't check a bundle that has no version: an unset `--version-property` falls back to
   `schema_version`.
5. **Identity.** The name `clinvar-gkm-parquet` appears in many places:
   - the package and the console script;
   - the cache directory and the User-Agent;
   - the selection's mini-bundle file name;
   - the manifest's `builder`;
   - the `clinvar_gkm_parquet.` prefix of every Parquet file's key-value metadata.

The maintainer wants the tool to serve any VA-Spec/GKM-based bundle schema and dataset. Its first and main producer
stays ClinVar-GKM.

## Decision

The maintainer's choices of 2026-11-11.

### 1. The tool serves any GKM bundle, and names no producer

The tool reads any JSON bundle that follows the GKM bundle conventions (section 4), with that bundle's JSON Schema. No
producer is built in, and none is the default:

- **Every command that reads a bundle takes its schema:** `--bundle-schema`, a file, a URL or a schema-set directory.
- **The column set** is the producer's file, or generated from the schema once ADR 0007's generator lands.
- **Without a schema the command stops** with a clear error. It doesn't guess a producer from the bundle's contents.
- **The tool ships no producer's schemas or column sets.** `spec/sections/default-bundle.yaml`, `schemas/1.1.0/` and
  `spec/sections/column-sets/1.1.0.columns.yaml` leave the package. The 1.1.0 schema set and column set become test
  fixtures, beside the CIViC ones, because the suite tests on real producers' data.

### 2. ClinVar-GKM's release model moves to ClinVar-GKM

- **The tool reads one bundle:** a path or URL, with its sha256 checked against `--sha256` or a `<file>.sha256` sidecar
  when there is one. A sidecar is a convention any producer can use.
- **Leaving the tool:** the line index, the release and alias resolution, monthly, weekly and delta handling, delta
  manifests, the replay rule, and their options and errors.
- **ClinVar-GKM's pipeline takes them over.** It resolves which file to read, and builds a full bundle for a past week
  when it needs one. It then calls the tool with that bundle and its schema.

### 3. Renamed gkm-bundle-parquet

| What | Before | After |
| --- | --- | --- |
| GitHub repository | `clingen-data-model/clinvar-gkm-parquet` | `clingen-data-model/gkm-bundle-parquet` (GitHub redirects the old URL) |
| PyPI project, console script | `clinvar-gkm-parquet` | `gkm-bundle-parquet` |
| Python package | `clinvar_gkm_parquet` | `gkm_bundle_parquet` |
| Cache directory, User-Agent | `clinvar-gkm-parquet` | `gkm-bundle-parquet` |
| Key-value metadata prefix | `clinvar_gkm_parquet.` | `gkm_bundle_parquet.` |

- **Sets from 1.x stay readable.** The new prefix changes the bytes of every file a build writes, and that is part of
  the MAJOR. `verify` and `rebuild` still read the old prefix, so sets built by 1.x stay checkable and rebuildable.
- **A last release under the old name** (a 1.x) says the tool has moved. The old name gets no new features.

### 4. The conventions the tool relies on, stated

These hold for ClinVar-GKM and CIViC, and are what the tool needs from any producer:

- **The root.** It is one JSON object. Its collections are maps from a record key to a record object, each identified by
  the schema (`patternProperties`, or `additionalProperties` holding a schema). An optional version property is named by
  the column set or `--version-property`; there is no default name. Other root values are kept as they are.
- **Pointers.** A record points to another record in the same bundle as `#/<section>/<key>`.
- **GKM classes.** Extensions are gkm-core `Extension` lists of `{name, value}`. A VRS integer-or-Range is recognised by
  its shape.
- **Layouts.** The line layout (one record per line) is streamed, so its size doesn't matter. Any other JSON layout is
  read whole, so it must fit in memory.

A producer whose collections aren't keyed maps, or whose pointers take another form, doesn't fit. Supporting one would be
a new decision.

### 5. What it lands with

- **Selection and validation become generic.** Selection takes `--bundle-schema` and reads the version property from
  the pairing (ADR 0008 decision 4.1). `validate` checks a bundle with no version when its schema has none.
- **The rename, the removals and the end of a default producer are one MAJOR, 2.0.0.** ADR 0008's retirements land in
  the same release. A 1.x release before it deprecates what goes, with a warning naming the replacement:
  - `--release`, `--index`, `--delta`, `--manifest` and `--full-source-release`;
  - building without `--bundle-schema`;
  - the old name.

## Earlier ADRs

| ADR | What changes |
| --- | --- |
| 0001 | "The bundle is the record, and Parquet derives from it" holds for any producer. A missing fact goes into that producer's bundle first. ClinVar-GKM's publishing arrangements become its own. |
| 0002 | The compatibility matrix lists no producer's `schema_version`s. A release states its table-spec and policy versions, and that it reads any bundle whose schema loads and compiles. |
| 0004 | Selection works on any pairing, as ADR 0008 decided, and no longer reads a fixed `schema_version` line. |
| 0006 | The default bundle (`default-bundle.yaml` and the shipped snapshot and column set) goes. A schema is always given. |
| 0008 | Its work order gains this ADR's items. "ClinVar-GKM publishes the default set" stands, with ClinVar-GKM passing its bundle and schema. |

## Consequences

- **Any GKM producer can use the tool as it is.** The README and docs describe the tool, with ClinVar-GKM and CIViC as
  worked examples, not as its purpose.
- **ClinVar-GKM has work to do:**
  - resolve its releases and replay deltas itself;
  - pass `--bundle-schema` (its schema's w3id URL, or a local set);
  - move its pipeline to the new name;
  - update its docs (`docs/parquet/tables.md` and the planned Ecosystem → Add-ons page).

  Its users rebuilding a past week's Parquet then need a full for that week, which ClinVar-GKM's own tooling makes.
- **Breaking for every 1.x user.** The command name changes, and so do the options a ClinVar-GKM build used. A build with
  no schema stops.
- **Outward-facing steps, each done only with the maintainer's go-ahead:**
  - renaming the GitHub repository;
  - registering the PyPI project;
  - the last release under the old name.
- **The GKM Toolkit move is unchanged** (ADR 0006): the generic core still moves when asked. A standalone name until then
  means one more rename if the tool itself moves rather than only its core.
- **CLAUDE.md, README.md and `.claude/` are revised to the generic framing.** The design plan stays as the history of
  the ClinVar-GKM-specific design, with a note at its top pointing here and to ADR 0008.
- **The work, in order:**
  1. ADRs 0008 and 0009 (docs);
  2. `validate` with no version, and generic selection;
  3. the deprecation release (1.x);
  4. the rename;
  5. removing the release model, the default producer and ADR 0008's retirements;
  6. 2.0.0.

  ADR 0007's generator and the filters continue alongside.
