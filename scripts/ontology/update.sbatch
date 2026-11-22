#!/usr/bin/env bash
# One ontology store update as one Slurm job. Submit from the repo checkout:
#
#   LABDATA_ONTOLOGIES=/path/to/ontology-store sbatch scripts/ontology/update.sbatch
#
# The update downloads only the vocabularies with a new release, then switches, prunes and
# copies the store to GCS. Arguments after the script go to `labdata ontology update`, such as
# `--source chebi=/path/to/chebi.obo` to read a file copied in from elsewhere. Every Slurm option
# here can be overridden on the sbatch command line.
#SBATCH --job-name=labdata-ontology
#SBATCH --partition=cpu_batch
#SBATCH --time=02:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --output=labdata-ontology.%j.log

set -euo pipefail
: "${LABDATA_ONTOLOGIES:?set LABDATA_ONTOLOGIES to the ontology store root}"
repo="${LABDATA_REPO:-${SLURM_SUBMIT_DIR:-$PWD}}"

echo "$(date -u +%FT%TZ) job ${SLURM_JOB_ID:-local} on $(hostname)" \
  "at $(git -C "$repo" rev-parse --short HEAD 2>/dev/null || echo unknown)"
# sbatch passes on the submitting shell's PATH, and a non-interactive shell often lacks pixi.
pixi="$(command -v pixi || echo "$HOME/.pixi/bin/pixi")"
exec "$pixi" run --manifest-path "$repo/pyproject.toml" \
  labdata ontology update --root "$LABDATA_ONTOLOGIES" "$@"
