#!/usr/bin/env bash
# Time the store's agreed queries on its current build as one Slurm job. Submit from the
# repo checkout, with an output folder outside the store:
#
#   LABDATA_STORE=/path/to/store sbatch --output=OUT/bench.%j.log \
#     scripts/store/benchmark.sbatch OUT [SEED]
#
# The store is only read. The results go to OUT/bench.<job id>.json and the job log; DuckDB
# and the cache use a folder under OUT that the job deletes. Every Slurm option here can be
# overridden on the sbatch command line.
#SBATCH --job-name=labdata-bench
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G

set -euo pipefail
: "${LABDATA_STORE:?set LABDATA_STORE to the store root}"
out="${1:?give an output folder outside the store}"
seed="${2:-0}"
repo="${LABDATA_REPO:-${SLURM_SUBMIT_DIR:-$PWD}}"

mkdir -p "$out"
scratch="$(mktemp -d "$out/tmp.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
export TMPDIR="$scratch" XDG_CACHE_HOME="$scratch/cache"

echo "$(date -u +%FT%TZ) job ${SLURM_JOB_ID:-local} on $(hostname)"
pixi="$(command -v pixi || echo "$HOME/.pixi/bin/pixi")"
cd "$scratch"
"$pixi" run --manifest-path "$repo/pyproject.toml" python "$repo/scripts/store/benchmark.py" \
  "$LABDATA_STORE" --seed "$seed" --out "$out/bench.${SLURM_JOB_ID:-local}.json"
