#!/usr/bin/env bash
# Keep one round's or one refresh's minimal logs with the store, under <store>/support/logs/:
#
#   scripts/store/keep_run_logs.sh <store> <round>
#   scripts/store/keep_run_logs.sh <store> refresh/<started>
#
# <round> is the round's folder name under <store>/work/run/, such as 2026-10-14-all. Its
# logs/*.log and run.json go to support/logs/<round>/. A Slurm job log
# work/run/labdata-run.*.log goes to support/logs/jobs/ when it holds the run's
# "round <round>:" line, so job logs of other rounds stay out.
# refresh/<started> is a refresh's folder under <store>/work/, such as
# refresh/2026-11-07T162405Z. Its logs/*.log and config.json go to
# support/logs/refresh/<started>/. It only copies: a rerun refreshes the copies and deletes
# nothing.
set -euo pipefail
if [[ $# -ne 2 ]]; then
  echo "usage: $0 <store> <round | refresh/<started>>" >&2
  exit 2
fi
store=$1
round=$2
run="$store/work/run"
refresh=false
src="$run/$round"
if [[ $round == refresh/* ]]; then
  refresh=true
  src="$store/work/$round"
fi
if [[ ! -d "$src" ]]; then
  echo "no folder $src; name a round under $run or a refresh under $store/work/refresh" >&2
  exit 1
fi
shopt -s nullglob
keep=("$src"/logs/*.log "$src/run.json")
if $refresh; then
  keep=("$src"/logs/*.log "$src/config.json")
fi
dest="$store/support/logs/$round"
mkdir -p "$dest"
for f in "${keep[@]}"; do
  if [[ -f "$f" ]]; then
    cp -p "$f" "$dest/"
    echo "kept $f"
  fi
done
if $refresh; then
  exit 0
fi
jobs="$store/support/logs/jobs"
mkdir -p "$jobs"
for f in "$run"/labdata-run.*.log; do
  if grep -qF "round $round: " "$f"; then
    cp -p "$f" "$jobs/"
    echo "kept $f"
  fi
done
