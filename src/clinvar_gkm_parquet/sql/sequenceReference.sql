-- sequenceReference: port of ClinVar-GKM src/scripts/parquet-schemas/sequenceReference.sql @ 1.0 25344ba6. Placeholders: sqltemplate.py.
SELECT
  key AS "id",
  {{scalar type}} AS "type",
  {{scalar refget_accession}} AS "refget_accession",
  {{scalar name}} AS "name",
  {{scalar molecule_type}} AS "molecule_type",
  {{scalar residue_alphabet}} AS "residue_alphabet",
  record AS "data"
FROM {{source}}
