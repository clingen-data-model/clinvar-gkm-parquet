-- conditionSet: port of ClinVar-GKM src/scripts/parquet-schemas/conditionSet.sql @ 1.0 25344ba6. Placeholders: sqltemplate.py.
-- BigQuery's WHERE ARRAY_LENGTH(concepts) > 1 is not ported: the bundle already holds only such sets, and
-- the tool never drops a bundle row (spec row_filter: null).
SELECT
  key AS "id",
  {{scalar type}} AS "type",
  {{scalar conceptSetType}} AS "conceptSetType",
  {{scalar membershipOperator}} AS "membershipOperator",
  {{bare_id concepts}} AS "concepts",
  record AS "data"
FROM {{source}}
WHERE {{known_keys}}
