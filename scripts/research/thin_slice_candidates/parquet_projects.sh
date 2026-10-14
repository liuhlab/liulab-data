#!/usr/bin/env bash
# Aggregate the SRA metadata Parquet to one row per BioProject (and per legacy study for
# runs with no BioProject). Writes OUT/projects.parquet with run, experiment and sample
# counts, library strategies, instruments, organisms, first release date, centers and how
# many experiments carry a GSM sample name.
# Usage: parquet_projects.sh PARQUET_DIR OUT
set -euo pipefail
src=$1 out=$2
mkdir -p "$out"
duckdb -c "
COPY (
  SELECT coalesce(nullif(bioproject, ''), 'NOPROJECT:' || sra_study) AS project,
         count(*) AS runs,
         count(DISTINCT experiment) AS experiments,
         count(DISTINCT biosample) AS biosamples,
         count(DISTINCT CASE WHEN sample_name LIKE 'GSM%' THEN experiment END) AS gsm_experiments,
         string_agg(DISTINCT assay_type, ';') AS strategies,
         string_agg(DISTINCT instrument, ';') AS instruments,
         string_agg(DISTINCT organism, ';') AS organisms,
         count(DISTINCT organism) AS n_organisms,
         string_agg(DISTINCT center_name, ';') AS centers,
         string_agg(DISTINCT sra_study, ';') AS studies,
         min(releasedate) AS first_release
  FROM read_parquet('$src/*.parquet')
  GROUP BY 1
) TO '$out/projects.parquet' (FORMAT parquet);
"
