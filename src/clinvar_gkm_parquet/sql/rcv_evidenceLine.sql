-- rcv_evidenceLine: port of ClinVar-GKM src/scripts/parquet-schemas/rcv_evidenceLine.sql @ 1.0 25344ba6. Placeholders: sqltemplate.py.
-- The bundle's one evidenceLine section holds three tables' rows, split by id shape
-- (cgp_evidence_line_table fails on a key of no known shape).
SELECT
  key AS "id",
  {{scalar type}} AS "type",
  {{scalar directionOfEvidenceProvided}} AS "directionOfEvidenceProvided",
  {{typed strengthOfEvidenceProvided}} AS "strengthOfEvidenceProvided",
  {{bare_id evidence_items}} AS "evidence_items",
  record AS "data"
FROM bundle_evidenceLine
-- CASE, not AND: the field check must see only this table's rows (AND may evaluate both sides).
WHERE CASE WHEN cgp_evidence_line_table(key, 'table rcv_evidenceLine: row filter') = 'rcv_evidenceLine' THEN {{known_keys}} ELSE FALSE END
