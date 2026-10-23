-- submitter: port of ClinVar-GKM src/scripts/parquet-schemas/submitter.sql @ 1.0 25344ba6. Placeholders: sqltemplate.py.
SELECT
  key AS "id",
  {{scalar type}} AS "type",
  {{scalar name}} AS "name",
  record AS "data"
FROM bundle_submitter
