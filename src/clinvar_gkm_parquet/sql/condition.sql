-- condition: port of ClinVar-GKM src/scripts/parquet-schemas/condition.sql @ 1.0 25344ba6. Placeholders: sqltemplate.py.
SELECT
  key AS "id",
  {{scalar type}} AS "type",
  {{scalar conceptType}} AS "conceptType",
  {{scalar name}} AS "name",
  {{typed aliases}} AS "aliases",
  {{typed primaryCoding}} AS "primaryCoding",
  {{typed mappings}} AS "mappings",
  record AS "data"
FROM bundle_condition
WHERE {{known_keys}}
