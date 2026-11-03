-- Shared DuckDB macros (see sqltemplate.py). build.py runs this once per connection before any table.
-- Every failure goes through cgp_fail, so the message names the table, column, bundle key and position.
-- `j` arguments are JSON values (json_extract results); NULL and JSON null both mean "absent".

-- Stop the build: '<table ...: column ...>: key <key>: <msg>'.
CREATE OR REPLACE MACRO cgp_fail(ctx, k, msg) AS
  error(concat(ctx, ': key ', coalesce(k, '(none)'), ': ', coalesce(msg, '(no message)')));

-- A JSON scalar as VARCHAR; an object or array fails (JSON_VALUE would give NULL and hide it).
CREATE OR REPLACE MACRO cgp_str(j, ctx, k, pos) AS
  CASE
    WHEN j IS NULL OR json_type(j) = 'NULL' THEN NULL
    WHEN json_type(j) IN ('OBJECT', 'ARRAY')
      THEN cgp_fail(ctx, k, concat('expected a scalar at ', pos, ', got ', j::VARCHAR))
    ELSE json_extract_string(j, '$')
  END;

-- A JSON scalar left as JSON, for a CAST to a non-VARCHAR type by the caller; an object or array fails.
CREATE OR REPLACE MACRO cgp_scalar(j, ctx, k, pos) AS
  CASE
    WHEN j IS NULL OR json_type(j) = 'NULL' THEN NULL
    WHEN json_type(j) IN ('OBJECT', 'ARRAY')
      THEN cgp_fail(ctx, k, concat('expected a scalar at ', pos, ', got ', j::VARCHAR))
    ELSE j
  END;

-- A '#/<section>/<id>' pointer string, checked; anything else (a bare id, an inline object) fails.
CREATE OR REPLACE MACRO cgp_pointer(j, ctx, k, pos) AS
  CASE
    WHEN j IS NULL OR json_type(j) = 'NULL' THEN NULL
    WHEN json_type(j) = 'VARCHAR' AND regexp_matches(json_extract_string(j, '$'), '^#/[^/]+/.')
      THEN json_extract_string(j, '$')
    ELSE cgp_fail(ctx, k, concat('not a #/<section>/<id> pointer at ', pos, ': ', j::VARCHAR))
  END;

-- Bare id: the pointer with ^#/[^/]+/ removed.
CREATE OR REPLACE MACRO cgp_bare_id(j, ctx, k, pos) AS
  regexp_replace(cgp_pointer(j, ctx, k, pos), '^#/[^/]+/', '');

-- Pointer section: the <section> part of the pointer.
CREATE OR REPLACE MACRO cgp_pointer_section(j, ctx, k) AS
  regexp_extract(cgp_pointer(j, ctx, k, '(column)'), '^#/([^/]+)/', 1);

-- TRUE, or a failure naming the record's top-level fields that no column covers.
CREATE OR REPLACE MACRO cgp_known_keys(rec, allowed, ctx, k) AS
  CASE
    WHEN len(list_filter(json_keys(rec), lambda f: NOT list_contains(allowed, f))) = 0 THEN TRUE
    ELSE cgp_fail(concat(ctx, ': record'), k, concat('top-level field(s) no column covers: ',
      list_filter(json_keys(rec), lambda f: NOT list_contains(allowed, f))::VARCHAR))
  END;

-- The evidence-line table a bundle evidenceLine key belongs to, by id shape (design plan §3.3):
-- clinvar.submission:SCV<n>.<v> -> evidenceLine; VCV<n>.<v>-... -> vcv_evidenceLine; RCV<n>.<v>-... ->
-- rcv_evidenceLine. Any other shape fails. (Each regex runs only on the keys with its literal prefix.)
CREATE OR REPLACE MACRO cgp_evidence_line_table(k, ctx) AS
  CASE
    WHEN starts_with(k, 'clinvar.submission:SCV') AND regexp_matches(k, '^clinvar\.submission:SCV[0-9]+\.[0-9]+$')
      THEN 'evidenceLine'
    WHEN starts_with(k, 'VCV') AND regexp_matches(k, '^VCV[0-9]+\.[0-9]+-') THEN 'vcv_evidenceLine'
    WHEN starts_with(k, 'RCV') AND regexp_matches(k, '^RCV[0-9]+\.[0-9]+-') THEN 'rcv_evidenceLine'
    ELSE cgp_fail(ctx, k, 'evidence-line id of no known shape (clinvar.submission:SCV<n>.<v>, VCV<n>.<v>-..., RCV<n>.<v>-...)')
  END;

-- A VRS coordinate: a definite integer, or NULL when it is a Range ([min, max] list), which the *_min/*_max
-- columns read instead. Any other shape fails.
CREATE OR REPLACE MACRO cgp_coordinate(j, ctx, k) AS
  CASE
    WHEN j IS NULL OR json_type(j) IN ('NULL', 'ARRAY') THEN NULL
    ELSE CAST(cgp_scalar(j, ctx, k, '(column)') AS BIGINT)
  END;

-- One bound of a VRS coordinate: the integer itself, else element i (0 = min, 1 = max) of the Range.
CREATE OR REPLACE MACRO cgp_coordinate_bound(j, i, ctx, k) AS
  CASE
    WHEN j IS NULL OR json_type(j) = 'NULL' THEN NULL
    WHEN json_type(j) = 'ARRAY' THEN
      CASE
        WHEN json_array_length(j) <> 2
          THEN cgp_fail(ctx, k, concat('a Range must have 2 elements: ', j::VARCHAR))
        ELSE CAST(cgp_scalar(json_extract(j, concat('$[', i, ']')), ctx, k, concat('[', i, ']')) AS BIGINT)
      END
    ELSE CAST(cgp_scalar(j, ctx, k, '(column)') AS BIGINT)
  END;
