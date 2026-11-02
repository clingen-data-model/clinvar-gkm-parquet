# civic (a real second producer's bundles)

Two real CIViC GKM bundles and their schema, copied byte for byte from `ga4gh/gkm-starter-kit`
(`https://github.com/ga4gh/gkm-starter-kit`), so the tool's generic path is proved on a producer that isn't ClinVar-GKM:
no `schema_version`, pretty-printed, empty collections, and root values that aren't collections.

| File here | Source path | Source commit | sha256 |
| --- | --- | --- | --- |
| `civic-assertion-9-bundle.json` | `notebooks/civic/bundles/civic-assertion-9-bundle.json` | `4b05ae77ad4e8fda7ed8b7749c4fa7af598147f5` | `83b566af7bf6a6721f264f64c9499ac24e944f4d98a275f02994ba3081f23321` |
| `civic-assertion-251-bundle.json` | `notebooks/civic/bundles/civic-assertion-251-bundle.json` | `4b05ae77ad4e8fda7ed8b7749c4fa7af598147f5` | `9e1a75178c2e1a299b22519b880f507d4533d7276d2da93e6f2f8f65548cc283` |
| `schema-set/civic-gkm-bundle-v0.1.0-a0.schema.json` | `notebooks/civic/bundles/civic-gkm-bundle-v0.1.0-a0.schema.json` | `8f3e6fe` | `ae0efee00f2b6bf41f00e0eb8b8d96cc80b9fefd400b45e36035fc15e0b1ec5b` |

`schema-set/` is the schema's whole `$ref` closure (72 files, schema set sha256
`c7df53717b2fec3ffe2705cb3e3800548362d12312ce562128629c97a0a4a4f0`), written on 2026-11-02 by
`scripts/vendor-schemas.py` from `w3id.org`, with each file's sha256 in its `MANIFEST.json`. The root's `$id` is
`urn:civic:gkm-bundle:schema:0.1.0-a0`, which can't be fetched.

`civic.columns.yaml` is not CIViC's: it was written for this fixture from `docs/column-set-format.md` and the records.

**Licence.** The source repository's `LICENSE` is CC0 1.0 Universal, and it states no separate licence for these files.
Neither the bundles nor the schema carry one. CIViC itself publishes its data as CC0 by its own statement, which this
repository doesn't record, so check upstream before relying on that.
