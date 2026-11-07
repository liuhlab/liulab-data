#!/usr/bin/env bash
# One store refresh as one Slurm job. Submit from the repo checkout:
#
#   LABDATA_STORE=/path/to/store sbatch scripts/store/refresh.sbatch [REFRESH OPTIONS]
#
# Options after the script name go to `labdata store refresh`, such as `--since 2026-10-14`.
# The refresh resumes where it stopped: submit it again after a failure, or run it on a
# preemptible partition with --requeue. Every Slurm option here can be overridden on the
# sbatch command line.
#
# Memory: the refresh reads the job's allocation and splits it among the steps that can run
# at once; each step caps DuckDB and Ray inside its share. Ask for more memory, not a cap.
#SBATCH --job-name=labdata-refresh
#SBATCH --partition=cpu_batch
#SBATCH --open-mode=append
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --output=labdata-refresh.%j.log

set -euo pipefail
: "${LABDATA_STORE:?set LABDATA_STORE to the store root}"
repo="${LABDATA_REPO:-${SLURM_SUBMIT_DIR:-$PWD}}"

echo "$(date -u +%FT%TZ) job ${SLURM_JOB_ID:-local}" \
  "restart ${SLURM_RESTART_COUNT:-0} on $(hostname)" \
  "at $(git -C "$repo" rev-parse --short HEAD 2>/dev/null || echo unknown)"
# sbatch passes on the submitting shell's PATH, and a non-interactive shell often lacks pixi.
pixi="$(command -v pixi || echo "$HOME/.pixi/bin/pixi")"
exec "$pixi" run --manifest-path "$repo/pyproject.toml" \
  labdata store refresh --root "$LABDATA_STORE" "$@"
