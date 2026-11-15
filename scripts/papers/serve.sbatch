#!/usr/bin/env bash
# The paper job as one Slurm job. Submit once, from the repo checkout:
#
#   LABDATA_PAPERS=/path/to/paper-store sbatch scripts/papers/serve.sbatch
#
# At start the job keeps two successors queued behind it, each waiting for the jobs before it
# to end for any reason (time limit, failure, cancel), so one that fails at start does not end
# the chain. To stop the chain, cancel the waiting successors first, then this job. The
# successors get the Slurm options written here, not options given on the first sbatch
# command line, except their log: each job writes labdata-papers.<job id>.log into the
# folder of the job before it. The first job's log goes where --output says, or into the
# submit directory without it.
#
# rclone reads $LABDATA_RCLONE_CONFIG, else its per-user default config, whatever
# RCLONE_CONFIG the submitting shell had; successors inherit the choice.
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
export RCLONE_CONFIG="${LABDATA_RCLONE_CONFIG:-$HOME/.config/rclone/rclone.conf}"

echo "$(date -u +%FT%TZ) job ${SLURM_JOB_ID:-local}" \
  "restart ${SLURM_RESTART_COUNT:-0} on $(hostname)" \
  "at $(git -C "$repo" rev-parse --short HEAD 2>/dev/null || echo unknown)"
# sbatch passes on the submitting shell's PATH, and a non-interactive shell often lacks pixi.
pixi="$(command -v pixi || echo "$HOME/.pixi/bin/pixi")"
exec "$pixi" run --manifest-path "$repo/pyproject.toml" \
  labdata papers serve --root "$LABDATA_PAPERS" --successor "$repo/scripts/papers/serve.sbatch"
