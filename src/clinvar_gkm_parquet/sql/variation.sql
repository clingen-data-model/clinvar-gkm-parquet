-- variation: port of ClinVar-GKM src/scripts/parquet-schemas/variation.sql @ 1.0 25344ba6. Placeholders: sqltemplate.py.
SELECT
  key AS "id",
  {{scalar type}} AS "type",
  {{scalar name}} AS "name",
  {{typed extensions ptr=[].value_gene_array[].gene}} AS "extensions",
  {{typed mappings}} AS "mappings",
  {{bare_id members}} AS "members",
  {{typed constraints ptr=[].allele,[].location}} AS "constraints",
  record AS "data"
FROM {{source}}
WHERE {{known_keys}}
