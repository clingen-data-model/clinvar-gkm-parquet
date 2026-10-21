-- scv: port of ClinVar-GKM src/scripts/parquet-schemas/scv.sql @ 1.0 25344ba6. Placeholders: sqltemplate.py.
-- The bundle has no all-null placeholder extension elements, so BigQuery's is_blank rebuild of extensions
-- is not needed: a slot the name doesn't map to is simply NULL.
SELECT
  key AS "id",
  {{scalar type}} AS "type",
  {{typed classification}} AS "classification",
  {{typed strength}} AS "strength",
  {{scalar direction}} AS "direction",
  {{typed quality}} AS "quality",
  {{scalar description}} AS "description",
  {{typed specifiedBy}} AS "specifiedBy",
  {{typed reportedIn}} AS "reportedIn",
  {{typed extensions}} AS "extensions",
  {{bare_id proposition_id}} AS "proposition_id",
  {{pointer_section proposition_section}} AS "proposition_section",
  {{typed contributions ptr=[].contributor}} AS "contributions",
  {{bare_id has_evidence_lines}} AS "has_evidence_lines",
  record AS "data"
FROM bundle_scv
WHERE {{known_keys}}
