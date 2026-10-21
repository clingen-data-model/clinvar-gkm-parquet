-- copyNumberChange: port of ClinVar-GKM src/scripts/parquet-schemas/copyNumberChange.sql @ 1.0 25344ba6. Placeholders: sqltemplate.py.
SELECT
  key AS "id",
  {{scalar type}} AS "type",
  {{scalar digest}} AS "digest",
  {{scalar copy_change}} AS "copy_change",
  {{bare_id location_id}} AS "location_id",
  record AS "data"
FROM bundle_copyNumberChange
