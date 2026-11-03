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

-- TRUE, or a failure when the record's own `id` (when present and not null) is not its bundle key. Every
-- known_fields table checks it: the key column stands for the record's id.
CREATE OR REPLACE MACRO cgp_record_id(j, ctx, k) AS
  CASE
    WHEN j IS NULL OR json_type(j) = 'NULL' THEN TRUE
    WHEN json_type(j) = 'VARCHAR' AND json_extract_string(j, '$') = k THEN TRUE
    ELSE cgp_fail(concat(ctx, ': record'), k, concat('the record''s id ', j::VARCHAR, ' is not its bundle key'))
  END;

-- The JSON kind of a value, as json-type extension slots name it: string, boolean, integer, number, object,
-- array; null for JSON null or absent.
CREATE OR REPLACE MACRO cgp_json_kind(j) AS
  CASE coalesce(json_type(j), 'NULL')
    WHEN 'VARCHAR' THEN 'string'
    WHEN 'BOOLEAN' THEN 'boolean'
    WHEN 'BIGINT' THEN 'integer'
    WHEN 'UBIGINT' THEN 'integer'
    WHEN 'DOUBLE' THEN 'number'
    WHEN 'OBJECT' THEN 'object'
    WHEN 'ARRAY' THEN 'array'
    ELSE 'null'
  END;

-- The slot kind of an extension value (json-type slots, sqltemplate.py): its JSON kind, and for an array
-- <element kind>_array; 'array' for an empty array, 'mixed_array' for elements of more than one kind.
CREATE OR REPLACE MACRO cgp_ext_kind(j) AS
  CASE
    WHEN cgp_json_kind(j) <> 'array' THEN cgp_json_kind(j)
    ELSE (list_transform([list_distinct(list_transform(CAST(j AS JSON[]), lambda e: cgp_json_kind(e)))],
      lambda ks: CASE len(ks) WHEN 0 THEN 'array' WHEN 1 THEN ks[1] || '_array' ELSE 'mixed_array' END))[1]
  END;

-- An integer-or-Range value (VRS: an integer, or a Range [min, max] whose ends are integers or null), checked;
-- anything else fails. The JSON value, or NULL when absent or null.
CREATE OR REPLACE MACRO cgp_range(j, ctx, k) AS
  CASE
    WHEN j IS NULL OR json_type(j) = 'NULL' THEN NULL
    WHEN json_type(j) IN ('BIGINT', 'UBIGINT') THEN j
    WHEN json_type(j) = 'ARRAY' AND json_array_length(j) = 2
      AND coalesce(json_type(j, '$[0]'), 'NULL') IN ('NULL', 'BIGINT', 'UBIGINT')
      AND coalesce(json_type(j, '$[1]'), 'NULL') IN ('NULL', 'BIGINT', 'UBIGINT') THEN j
    ELSE cgp_fail(ctx, k, concat('expected an integer or a Range [min, max] of integers or nulls, got ', j::VARCHAR))
  END;

-- range-exact: the integer; NULL when the value is a Range (or absent).
CREATE OR REPLACE MACRO cgp_range_exact(j, ctx, k) AS
  (list_transform([cgp_range(j, ctx, k)],
    lambda r: CASE WHEN json_type(r) = 'ARRAY' THEN NULL ELSE CAST(r AS BIGINT) END))[1];

-- range-min (i = 0) / range-max (i = 1): the integer, else element i of the Range.
CREATE OR REPLACE MACRO cgp_range_bound(j, i, ctx, k) AS
  (list_transform([cgp_range(j, ctx, k)],
    lambda r: CASE WHEN json_type(r) = 'ARRAY' THEN CAST(json_extract(r, concat('$[', i, ']')) AS BIGINT)
                   ELSE CAST(r AS BIGINT) END))[1];
