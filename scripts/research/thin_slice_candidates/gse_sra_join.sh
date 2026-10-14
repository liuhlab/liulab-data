#!/usr/bin/env bash
# Join sampled GEO Series (from sample_gse.py) to the SRA metadata Parquet by GSM sample name.
# Writes OUT/gse_sra_join.csv: per GSE, listed GSMs, GSMs with an SRA run, BioProjects and
# legacy studies the runs carry, runs, experiments, strategies, instruments and organisms.
# Usage: gse_sra_join.sh PARQUET_DIR GSM_GSE GSE_META OUT
set -euo pipefail
src=$1 gsm=$2 meta=$3 out=$4
mkdir -p "$out"
duckdb -c "
CREATE TABLE gg AS SELECT * FROM read_csv('$gsm', delim='\t', header=false,
  columns={'gsm': 'VARCHAR', 'gse': 'VARCHAR'});
CREATE TABLE meta AS SELECT * FROM read_csv('$meta', delim='\t', header=false, quote='',
  columns={'gse': 'VARCHAR', 'n_samples': 'INT', 'taxon': 'VARCHAR', 'pdat': 'VARCHAR',
           'bioproject': 'VARCHAR', 'pmids': 'VARCHAR', 'superseries': 'VARCHAR',
           'gdstype': 'VARCHAR', 'title': 'VARCHAR'});
CREATE TABLE runs AS SELECT sample_name AS gsm, acc, experiment, bioproject, sra_study,
  assay_type, instrument, organism
  FROM read_parquet('$src/*.parquet')
  WHERE sample_name IN (SELECT gsm FROM gg);
COPY (
  SELECT m.gse, any_value(m.n_samples) AS n_samples, count(DISTINCT gg.gsm) AS gsms,
         count(DISTINCT r.gsm) AS gsms_with_run,
         count(DISTINCT r.experiment) AS experiments, count(DISTINCT r.acc) AS runs,
         count(DISTINCT nullif(r.bioproject, '')) AS n_bioprojects,
         string_agg(DISTINCT nullif(r.bioproject, ''), ';') AS bioprojects,
         count(DISTINCT r.acc) FILTER (WHERE coalesce(r.bioproject, '') = '') AS runs_no_bioproject,
         string_agg(DISTINCT r.sra_study, ';') AS studies,
         string_agg(DISTINCT r.assay_type, ';') AS strategies,
         string_agg(DISTINCT r.instrument, ';') AS instruments,
         string_agg(DISTINCT r.organism, ';') AS organisms,
         any_value(m.taxon) AS taxon, any_value(m.pdat) AS pdat,
         any_value(m.bioproject) AS geo_bioproject, any_value(m.pmids) AS pmids,
         any_value(m.superseries) AS superseries, any_value(m.title) AS title
  FROM meta m JOIN gg USING (gse) LEFT JOIN runs r USING (gsm)
  GROUP BY m.gse
) TO '$out/gse_sra_join.csv';
"
