-- therapy: port of ClinVar-GKM src/scripts/parquet-schemas/therapy.sql @ 1.0 25344ba6. Placeholders: sqltemplate.py.
SELECT
  key AS "key",
  record AS "value",
  {{scalar name}} AS "name",
  {{scalar concept_type}} AS "concept_type"
FROM bundle_therapy
