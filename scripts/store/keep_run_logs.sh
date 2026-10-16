#!/usr/bin/env bash
# Keep one round's minimal logs with the store, under <store>/support/logs/:
#
#   scripts/store/keep_run_logs.sh <store> <round>
#
# <round> is the round's folder name under <store>/work/run/, such as 2026-10-14-all. Its
# logs/*.log and run.json go to support/logs/<round>/. A Slurm job log
# work/run/labdata-run.*.log goes to support/logs/jobs/ when it holds the run's
# "round <round>:" line, so job logs of other rounds stay out. It only copies: a rerun
# refreshes the copies and deletes nothing.
set -euo pipefail
if [[ $# -ne 2 ]]; then
  echo "usage: $0 <store> <round>" >&2
  exit 2
fi
store=$1
round=$2
run="$store/work/run"
src="$run/$round"
if [[ ! -d "$src" ]]; then
  echo "no round folder $src; name a folder under $run" >&2
  exit 1
fi
dest="$store/support/logs/$round"
jobs="$store/support/logs/jobs"
mkdir -p "$dest" "$jobs"
shopt -s nullglob
for f in "$src"/logs/*.log "$src/run.json"; do
  if [[ -f "$f" ]]; then
    cp -p "$f" "$dest/"
    echo "kept $f"
  fi
done
for f in "$run"/labdata-run.*.log; do
  if grep -qF "round $round: " "$f"; then
    cp -p "$f" "$jobs/"
    echo "kept $f"
  fi
done
