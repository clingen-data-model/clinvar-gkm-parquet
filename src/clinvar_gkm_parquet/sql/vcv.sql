-- vcv: port of ClinVar-GKM src/scripts/parquet-schemas/vcv.sql @ 1.0 25344ba6. Placeholders: sqltemplate.py.
SELECT
  key AS "id",
  {{scalar type}} AS "type",
  {{scalar direction}} AS "direction",
  {{typed strength}} AS "strength",
  {{typed quality}} AS "quality",
  {{typed classification}} AS "classification",
  {{typed extensions}} AS "extensions",
  {{bare_id proposition_id}} AS "proposition_id",
  {{pointer_section proposition_section}} AS "proposition_section",
  {{bare_id has_evidence_lines}} AS "has_evidence_lines",
  record AS "data"
FROM {{source}}
WHERE {{known_keys}}
