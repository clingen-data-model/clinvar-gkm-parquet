-- evidenceLine: port of ClinVar-GKM src/scripts/parquet-schemas/evidenceLine.sql @ 1.0 25344ba6. Placeholders: sqltemplate.py.
-- The bundle's one evidenceLine section holds three tables' rows, split by id shape
-- (cgp_evidence_line_table fails on a key of no known shape).
SELECT
  key AS "id",
  {{scalar type}} AS "type",
  {{scalar directionOfEvidenceProvided}} AS "directionOfEvidenceProvided",
  {{typed evidenceOutcome}} AS "evidenceOutcome",
  {{typed extensions}} AS "extensions",
  {{bare_id target_proposition_id}} AS "target_proposition_id",
  record AS "data"
FROM {{source}}
-- CASE, not AND: the field check must see only this table's rows (AND may evaluate both sides).
WHERE CASE WHEN cgp_evidence_line_table(key, 'table evidenceLine: row filter') = 'evidenceLine' THEN {{known_keys}} ELSE FALSE END
