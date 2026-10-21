-- propositionSomaticClinicalImpact: port of ClinVar-GKM src/scripts/parquet-schemas/proposition.sql @ 1.0 25344ba6. Placeholders: sqltemplate.py.
-- One shared column schema for the four proposition sections; the bundle has one section per table, so
-- BigQuery's type -> section dispatch is not needed. Extension values are collapsed to `value` in the
-- bundle (BigQuery read value_string / value_enum from the uncollapsed dict).
SELECT
  key AS "id",
  {{scalar type}} AS "type",
  {{scalar predicate}} AS "predicate",
  {{bare_id subject_variant_id}} AS "subject_variant_id",
  {{bare_id object_id}} AS "object_id",
  {{pointer_section object_type}} AS "object_type",
  {{bare_id condition_qualifier_id}} AS "condition_qualifier_id",
  'ncbigene:' || cgp_str(json_extract(record, '$.geneContextQualifier.primaryCoding.code'), {{ctx gene_context_id}}, key, '(column)') AS "gene_context_id",
  {{scalar gene_context_name}} AS "gene_context_name",
  CAST(flatten(list_transform(
    list_filter(gcq_ext, lambda e: json_extract_string(e, '$.name') = 'submittedGeneSymbols'),
    lambda e: list_transform(CAST(json_extract(e, '$.value') AS JSON[]),
      lambda s: cgp_str(s, {{ctx submitted_gene_symbols}}, key, 'submittedGeneSymbols.value[]')))) AS {{type submitted_gene_symbols}}) AS "submitted_gene_symbols",
  CASE
    -- geneContextQualifier extensions: the two names the columns read; any other name stops the build.
    WHEN len(list_filter(gcq_ext, lambda e: coalesce(json_extract_string(e, '$.name'), '') NOT IN
           ('submittedGeneSymbols', 'geneContextSource'))) > 0
      THEN cgp_fail({{ctx gene_context_source}}, key, concat('unknown geneContextQualifier extension name(s): ',
        list_transform(list_filter(gcq_ext, lambda e: coalesce(json_extract_string(e, '$.name'), '') NOT IN
          ('submittedGeneSymbols', 'geneContextSource')), lambda e: json_extract_string(e, '$.name'))::VARCHAR))
    WHEN len(list_filter(gcq_ext, lambda e: json_extract_string(e, '$.name') = 'geneContextSource')) > 1
      THEN cgp_fail({{ctx gene_context_source}}, key, 'more than one geneContextSource extension')
    ELSE cgp_str(json_extract(list_filter(gcq_ext, lambda e: json_extract_string(e, '$.name') = 'geneContextSource')[1], '$.value'),
      {{ctx gene_context_source}}, key, 'geneContextSource.value')
  END AS "gene_context_source",
  {{scalar mode_of_inheritance}} AS "mode_of_inheritance",
  {{scalar penetrance}} AS "penetrance",
  record AS "data"
FROM (
  SELECT key, record,
    -- [] when there is no qualifier or it has no extensions
    coalesce(CAST(json_extract(record, '$.geneContextQualifier.extensions') AS JSON[]), []) AS gcq_ext
  FROM bundle_propositionSomaticClinicalImpact
)
