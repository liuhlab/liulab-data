#!/usr/bin/env bash
#
# The gate runner. Usage: bash scripts/check.sh <pixi-task>...
#
# Each argument names a task in [tool.pixi.tasks]. The steps all start at once and
# each one's output is captured to its own file, because interleaved concurrent
# output is unreadable. Nothing is printed until every step has finished; then each
# step gets a labelled block, in the order it was named, followed by a summary that
# carries each step's verdict and how long it took. The exit status is non-zero if
# any step failed.
#
# A passing step prints only the last few lines it wrote; a failing step prints
# everything. A red run is read to find the failure, and the full output of every
# green step is thousands of lines of nothing standing between the reader and it.
#
# Exit status: 1 when a step ran and failed, 2 when the gate itself could not run —
# a usage error, or a capture directory it could not make. `scripts/conformance.py`,
# the one step this repo writes rather than installs, spells its own two the same way,
# and the summary prints each step's own status, so "could not run" reaches the reader
# as itself rather than as a rule nobody broke.
#
# The step list lives in the `check-static` task in pyproject.toml and nowhere else,
# so adding a step is one more word in one place and the local gate cannot drift
# from CI, which invokes the same task names.
#
# `set -e` is DELIBERATELY ABSENT. It is exactly what would stop the runner at the
# first failing step, and telling you everything that is wrong in a single run is
# the entire point of this file.
#
# `set -m` gives every step its own process group, so one kill on the negated pid
# reaches past `pixi` to the test workers underneath. It also means Ctrl-C never
# reaches a step: a terminal signals its foreground group, and the steps are not in
# it. So interrupt and terminate are turned into an ordinary exit, which is what
# lets the EXIT trap run, and that trap kills the groups BEFORE removing the capture
# directory. Killing first is the whole point of the ordering — a step still writing
# into a directory that has been deleted is the race it closes. An abandoned gate
# used to keep every worker it started, so the next run competed with the last one.
#
# Portable to bash 3.2, which is what macOS ships: no `wait -n`, no associative
# arrays, nothing beyond coreutils.

set -muo pipefail

if [ "$#" -eq 0 ]; then
    printf 'usage: %s <pixi-task>...\n' "$0" >&2
    exit 2
fi

steps=("$@")
count=$#

# How much of a passing step is worth reading.
tail_lines=3

capture_dir=$(mktemp -d "${TMPDIR:-/tmp}/liulab-check.XXXXXX") || exit 2

# Kill, then delete, in that order and never one without the other. Each surviving
# job leads its own process group, so the negated pid takes its children with it.
cleanup() {
    # Job control has done its work by now; leaving it on only makes the shell print
    # a "Terminated" notice, quoting the whole step, for each job killed below.
    set +m
    for pgid in $(jobs -p); do
        kill -- "-$pgid" 2>/dev/null
    done
    wait 2>/dev/null
    rm -rf "$capture_dir"
}
trap cleanup EXIT
# A signal would otherwise end this shell without running the EXIT trap, and every
# step would outlive the gate that started it.
trap 'printf "\ninterrupted, stopping every step\n" >&2; exit 130' INT
trap 'printf "\nterminated, stopping every step\n" >&2; exit 143' TERM

rule="--------------------------------------------------------------------"

# Start every step. The subshell is what makes it a job, and so a process group; it
# also times the step, which this shell cannot do, because it collects statuses in
# launch order rather than in the order the steps finish. Steps get no stdin: one
# that asks a question would otherwise be stopped by its own terminal and hang the
# gate until CI times out.
pids=()
i=0
while [ "$i" -lt "$count" ]; do
    (
        started=$SECONDS
        pixi run "${steps[$i]}" </dev/null >"$capture_dir/$i.log" 2>&1
        status=$?
        printf '%s' "$((SECONDS - started))" >"$capture_dir/$i.time"
        exit "$status"
    ) &
    pids[$i]=$!
    i=$((i + 1))
done

# Collect every status. Without `wait -n` this waits in launch order, which costs
# nothing: the steps are already running, and no failure short-circuits the loop.
statuses=()
elapsed=()
i=0
while [ "$i" -lt "$count" ]; do
    wait "${pids[$i]}"
    statuses[$i]=$?
    elapsed[$i]=$(cat "$capture_dir/$i.time" 2>/dev/null)
    [ -n "${elapsed[$i]}" ] || elapsed[$i]="?"
    i=$((i + 1))
done

# One labelled block per step: the tail of a passing one, all of a failing one.
failed=0
i=0
while [ "$i" -lt "$count" ]; do
    if [ "${statuses[$i]}" -eq 0 ]; then
        verdict="PASS"
    else
        verdict="FAIL"
        failed=$((failed + 1))
    fi
    printf '\n%s\n%s  %s\n%s\n' "$rule" "$verdict" "${steps[$i]}" "$rule"
    if [ "$verdict" = "FAIL" ]; then
        cat "$capture_dir/$i.log"
    else
        lines=$(wc -l <"$capture_dir/$i.log")
        if [ "$lines" -gt "$tail_lines" ]; then
            printf '[%s earlier lines hidden; a failing step prints in full]\n' \
                "$((lines - tail_lines))"
        fi
        tail -n "$tail_lines" "$capture_dir/$i.log"
    fi
    i=$((i + 1))
done

# The summary, so a long run does not have to be re-read to find what broke.
printf '\n%s\nsummary\n%s\n' "$rule" "$rule"
i=0
while [ "$i" -lt "$count" ]; do
    if [ "${statuses[$i]}" -eq 0 ]; then
        printf '  pass  %s (%ss)\n' "${steps[$i]}" "${elapsed[$i]}"
    else
        printf '  FAIL  %s (exit %s, %ss)\n' \
            "${steps[$i]}" "${statuses[$i]}" "${elapsed[$i]}"
    fi
    i=$((i + 1))
done

if [ "$failed" -gt 0 ]; then
    printf '\n%s of %s steps failed in %ss. Every failure is above; read to the bottom.\n' \
        "$failed" "$count" "$SECONDS" >&2
    exit 1
fi

printf '\nAll %s steps passed in %ss.\n' "$count" "$SECONDS"
