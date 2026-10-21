-- location: port of ClinVar-GKM src/scripts/parquet-schemas/location.sql @ 1.0 25344ba6. Placeholders: sqltemplate.py.
-- loc_*: an integer, or a VRS Range [min, max] whose elements may be null (open range). BigQuery's
-- COALESCE(JSON_VALUE('$.start'), JSON_VALUE('$.start[i]')); here any other shape fails.
SELECT
  key AS "id",
  {{scalar type}} AS "type",
  {{scalar digest}} AS "digest",
  cgp_coordinate(json_extract(record, '$.start'), {{ctx loc_start}}, key) AS "loc_start",
  cgp_coordinate(json_extract(record, '$.end'), {{ctx loc_end}}, key) AS "loc_end",
  {{bare_id sequence_reference_id}} AS "sequence_reference_id",
  record AS "data",
  cgp_coordinate_bound(json_extract(record, '$.start'), 0, {{ctx loc_start_min}}, key) AS "loc_start_min",
  cgp_coordinate_bound(json_extract(record, '$.start'), 1, {{ctx loc_start_max}}, key) AS "loc_start_max",
  cgp_coordinate_bound(json_extract(record, '$.end'), 0, {{ctx loc_end_min}}, key) AS "loc_end_min",
  cgp_coordinate_bound(json_extract(record, '$.end'), 1, {{ctx loc_end_max}}, key) AS "loc_end_max"
FROM bundle_location
