# real-1.1.0-1031 (real bundle excerpt, for `validate`'s negative cases)

A small, connected excerpt of the 2026-10-31 ClinVar-GKM 1.1.0 weekly full
(`https://data.clinvar-gkm.org/v1/1.1/weekly/clinvar-gkm_2026-1031.json.gz`, 91,854,927 records, sha256
`7105eba1d88f22359e94d8d813a1e7b3afd00afe1b2e528180ea312267392c20`, checked against the published one), made by
`scripts/extract-fixtures.py` like `real-1.1.0/`. Its source, counts and seeds are in `PROVENANCE.json`: 345 records in all
20 sections, 36,818 bytes.

It covers what the suite's main fixture (`real-1.1.0/`, a 2026-10-18 excerpt) lacks, so that `tests/test_validate.py`'s
generated negative cases reach every property ClinVar-GKM's narrowed profiles set to `false`: the
`propositionClinvarCustom` section (one record of each of its 9 types), a pathogenicity proposition with a
`penetranceQualifier`, and a somatic clinical impact proposition of the prognostic kind. Only `tests/test_validate.py` reads
it; the rest of the suite uses `real-1.1.0/`.

Made with (the bundle read locally; the seeds came from a sample of the same file):

```sh
uv run python scripts/extract-fixtures.py clinvar-gkm_2026-1031.json.gz \
    --source-url https://data.clinvar-gkm.org/v1/1.1/weekly/clinvar-gkm_2026-1031.json.gz --release 2026-10-31 \
    --sha256 7105eba1d88f22359e94d8d813a1e7b3afd00afe1b2e528180ea312267392c20 \
    --out tests/fixtures/real-1.1.0-1031 --per-shape 1 --candidates 1 \
    --seed propositionPathogenicity:SCV002547360-PATH --seed propositionSomaticClinicalImpact:SCV006076900-PROG \
    --seed propositionClinvarCustom:VCV001065846-G-AFF --seed propositionClinvarCustom:VCV000523645-G-ASSOC-NOCP \
    --seed propositionClinvarCustom:VCV000996154-G-CS --seed propositionClinvarCustom:VCV000150646-G-CDFS-NOCP \
    --seed propositionClinvarCustom:VCV000226025-G-DR-NOCP --seed propositionClinvarCustom:VCV000004792-G-NP \
    --seed propositionClinvarCustom:VCV000010406-G-OTH-NOCP --seed propositionClinvarCustom:VCV000013416-G-PROTECT-NOCP \
    --seed propositionClinvarCustom:VCV000004286-G-RF
```

Nine of the extractor's built-in shapes have no record in this release (hgvs repairs and some extension names; listed in
`PROVENANCE.json`). They are in `real-1.1.0/`.
