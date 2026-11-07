---
search:
  exclude: true
---

# Run the paper job as one process with a lock, not a Snakemake DAG

The paper job is one long-running process, `labdata papers serve`. It runs the daily pass and
the PMC sync one at a time, and each holds the paper store's lock file for its whole run. This
departs from the repo's rule that pipelines are Snakemake DAGs.

## Considered options

- **A Snakemake DAG per pass.** Snakemake resumes by output files and schedules steps across
  cores. Here the work is rationed by the day: each OpenAlex key buys 100 files a day, and a
  host link is tried once. What is done lives in rows of `attempts` and `files`, per paper, not
  in one output file per step. The steps share four small tables that each step loads whole and
  writes back, so parallel steps would contend for the same files.
- **One process with a lock.** Each pass resumes per paper from the tables, and a pass killed
  mid-way spends no quota twice. The lock keeps the daily pass and the PMC sync apart; a later
  one waits for the earlier one.

## Consequences

- Each step is also its own command, so it can be run and tested alone.
- If the PMC sync outgrows the job's memory, it moves to its own Slurm job, which still takes
  the lock.
