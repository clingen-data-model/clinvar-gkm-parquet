-- therapyGroup: port of ClinVar-GKM src/scripts/parquet-schemas/therapyGroup.sql @ 1.0 25344ba6. Placeholders: sqltemplate.py.
SELECT
  key AS "key",
  record AS "value",
  {{scalar membership_operator}} AS "membership_operator",
  {{bare_id members}} AS "members"
FROM bundle_therapyGroup
