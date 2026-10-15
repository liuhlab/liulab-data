#!/usr/bin/env bash
# One round of the full store download as one Slurm job. Submit from the repo checkout:
#
#   LABDATA_STORE=/path/to/store sbatch scripts/store/run.sbatch [FRACTION]
#
# FRACTION is the round's seeded sample of the target list (0.01, 0.1); omit it for all.
# The run resumes where it stopped, so a preempted job is requeued and carries on. Every
# Slurm option here can be overridden on the sbatch command line.
#
# Memory: the build holds about one chunk per worker plus DuckDB's cap, and the snapshot
# extract can run beside the E-utilities lane, each with its own DuckDB. Two caps and the
# workers fit in the request with room to spare.
#SBATCH --job-name=labdata-run
#SBATCH --partition=cpu_preemptible
#SBATCH --requeue
#SBATCH --open-mode=append
#SBATCH --time=3-00:00:00
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --output=labdata-run.%j.log

set -euo pipefail
: "${LABDATA_STORE:?set LABDATA_STORE to the store root}"
export LABDATA_DUCKDB_MEMORY="${LABDATA_DUCKDB_MEMORY:-64GB}"
repo="${LABDATA_REPO:-${SLURM_SUBMIT_DIR:-$PWD}}"

echo "$(date -u +%FT%TZ) job ${SLURM_JOB_ID:-local} restart ${SLURM_RESTART_COUNT:-0} on $(hostname)"
args=(store run --root "$LABDATA_STORE")
if [[ $# -gt 0 ]]; then
  args+=(--fraction "$1")
fi
exec pixi run --manifest-path "$repo/pyproject.toml" labdata "${args[@]}"
