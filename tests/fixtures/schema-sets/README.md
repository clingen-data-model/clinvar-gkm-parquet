# schema-sets (unpublished schema sets, for tests)

## clinvar-gkm-narrowed-a956b5a

ClinVar-GKM's narrowed profiles: the bundle schema and its `$ref` closure at commit
`a956b5a4dbba3ee9f6954101d0b726ebedae6d4b` of branch `feat/narrow-gkm-profiles`
([clingen-data-model/clinvar-gkm#215](https://github.com/clingen-data-model/clinvar-gkm/pull/215), draft). The branch's later
commits (`ea34971`, `17c62df`) change no schema file. The GA4GH files come from the `va-spec` submodule at
`29573487fe340827f28304dd43e0353e05db1524` and the `cat-vrs`, `vrs` and `gkm-core` submodules it nests, at the commits
their gitlinks record. Each file's source is the `url` in `MANIFEST.json`.

- **Root:** `schema/clinvar-gkm/clinvar-gkm-bundle.schema.json`. 143 files, from 159 candidates.
- **Schema set sha256** (schemas.py, over each `$id` and its bytes): `98612fcac74526e4af76cd81a4cfadb09111c7df941c303ddc8cb79663618c6a`.
- **What changed from the published 1.1.0 set** (`schemas/1.1.0/`): every section points at a `Clinvar*` profile that sets
  unused base properties to `false`; pointers name their target section (`pattern: "^#/<section>/"`); extensions are closed
  classes; enums are set where the pipeline fixes the values; each section's key `patternProperties` match the exact id the
  pipeline writes. Of the published set's 95 files, 77 are byte-identical here, 17 differ (16 ClinVar-GKM files and
  va-spec's `Statement`), and one isn't reached; 49 files are new.
- **Not bumped:** the `$id`s still say `.../clinvar-gkm/1.1.0/...`, and the bundle's `schema_version` const is still `1.1.0`.
  That is why the set lives here and not under `schemas/`: every `schemas/<version>/` is a supported pairing, and this one
  would claim the published 1.1.0 set's identity. It isn't packaged and isn't in the compatibility matrix.

Made by `scripts/vendor-git-schemas.py`, which reads git objects only (no checkout, no network), the way ClinVar-GKM's
`load_registry` walks `schema/`: every file under it, following symlinks into the submodules, that sits in a `json/` folder
and declares `$id`. To refresh it from a later commit:

```sh
uv run python scripts/vendor-git-schemas.py ~/Development/gks/clinvar-gks <commit> \
    schema/clinvar-gkm/clinvar-gkm-bundle.schema.json tests/fixtures/schema-sets/clinvar-gkm-narrowed-<short sha>
```

then update the directory name, the commit and sha256 above, and `NARROWED`, `NARROWED_COMMIT` and `NARROWED_SHA256` in
`tests/test_validate.py`. When ClinVar-GKM publishes the bumped version, it is fetched and snapshotted the normal way
(`scripts/vendor-schemas.py`), and this fixture goes.
