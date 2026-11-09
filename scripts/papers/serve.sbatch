#!/usr/bin/env bash
# The paper job as one Slurm job. Submit once, from the repo checkout:
#
#   LABDATA_PAPERS=/path/to/paper-store sbatch scripts/papers/serve.sbatch
#
# At start the job submits its own successor, which waits for this job to end for any reason
# (time limit, failure, cancel), then takes over. To stop the chain, cancel the waiting
# successor first, then this job. The successor gets the Slurm options written here, not
# options given on the first sbatch command line, except its log: each job writes
# labdata-papers.<job id>.log into the folder of the job before it. The first job's log
# goes where --output says, or into the submit directory without it.
#SBATCH --job-name=labdata-papers
#SBATCH --partition=cpu_batch
#SBATCH --open-mode=append
#SBATCH --time=14-00:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --output=labdata-papers.%j.log

set -euo pipefail
: "${LABDATA_PAPERS:?set LABDATA_PAPERS to the paper store root}"
repo="${LABDATA_REPO:-${SLURM_SUBMIT_DIR:-$PWD}}"
export LABDATA_REPO="$repo"

echo "$(date -u +%FT%TZ) job ${SLURM_JOB_ID:-local}" \
  "restart ${SLURM_RESTART_COUNT:-0} on $(hostname)" \
  "at $(git -C "$repo" rev-parse --short HEAD 2>/dev/null || echo unknown)"
# sbatch passes on the submitting shell's PATH, and a non-interactive shell often lacks pixi.
pixi="$(command -v pixi || echo "$HOME/.pixi/bin/pixi")"
exec "$pixi" run --manifest-path "$repo/pyproject.toml" \
  labdata papers serve --root "$LABDATA_PAPERS" --successor "$repo/scripts/papers/serve.sbatch"
