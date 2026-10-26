-- gene: port of ClinVar-GKM src/scripts/parquet-schemas/gene.sql @ 1.0 25344ba6. Placeholders: sqltemplate.py.
SELECT
  key AS "id",
  {{scalar concept_type}} AS "concept_type",
  {{scalar name}} AS "name",
  record AS "data"
FROM {{source}}
