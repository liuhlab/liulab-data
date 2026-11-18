# Monthly maintenance run

The GEO/SRA store's refresh and its light checks, then the paper store's checks. The procedure
for each store lives in its how-to; follow it from there, and record each check below in the
log entry.

**Budget:** the refresh, 2 h wall time. Past 2 h, stop and report: the user decides whether a
full rebuild is worth it.

## GEO/SRA store

Follow "Refresh the store" in `docs/how-to/store-run.md`.

1. **Refresh.** Submit `scripts/store/refresh.sbatch` under the run plan. Probe it in its first
   minutes.
2. **Status record.** `refresh-status.json` in the store root: `outcome` is `succeeded` and
   `last_success` is today. Record the new build ID from the store marker. If `copy_pending` is
   `true`, get the login (SKILL.md, preflight), run `labdata store copy`, and read it again.
3. **Coverage.** `labdata store coverage` ends with `Coverage passes.`
4. **Thin-slice checks.** `pixi run python scripts/store/check_stage1.py` on the held node:
   the thin-slice checks pass with the network blocked. Its `--every-record` flag belongs to the
   yearly page, or to a run the user said yes to.
5. **Step times.** A table with one row per refresh step: its wall time, from the first and
   last timestamps in its `logs/<step>.log` under the status record's `folder`, then the total
   against the 2 h budget. Put the table in the report and the log entry.
6. **Refresh logs.** `scripts/store/keep_run_logs.sh "$LABDATA_STORE" refresh/<started>`, with
   `<started>` from the status record's `folder`. Deleting `work/` after that needs the user's
   yes, and never takes `work/extract/`: every build reads the extracts there.
7. **GCS copy.** The routine check of "Check the copy", in `docs/how-to/store-gcs.md`, passes:
   `copy_pending` is `false` in `refresh-status.json`, and the copy record check reports 0
   differences. No step here lists every file of the store: that section's full check runs
   only to repair, with the user's yes.

## Paper store

Follow `docs/how-to/papers-run.md`. The paper job does the work; these checks read what it did.

1. **The new build's PMC sync.** Read `last_pmc_sync_build` in `paper-status.json`, in the
   paper store root. When it names a build, the new build makes the PMC sync due: the paper
   job starts it within a minute, once any pass already running ends, but never between 00:00
   and 00:30 UTC. When it is empty, as in the first month after the job's deploy, only the
   date rule applies: the sync starts on the 3rd, and this check waits for it. Probe the job
   log in the sync's first minutes, then wait until `paper-status.json` shows
   `last_pmc_sync_build` equal to the build ID recorded in the GEO/SRA status-record step, and
   `last_pmc_sync_success` the day the sync ran. A failed sync shows `last_pass.outcome`
   `failed`, and the job tries it again an hour later. Record the sync's counts from its line
   in the job log, re-keyed papers and DOIs turned away among them, in the log entry.
2. **Status record.** `paper-status.json`: `last_pass.outcome` is `success`,
   `last_daily_success` is today (yesterday before 00:30 UTC), and `copy_pending` is `false`.
   Record the `counts` by status in the log entry.
3. **GCS copy.** The routine check of "Check the copy", in `docs/how-to/papers-run.md`, passes:
   `copy_pending` is `false`, `last_copy_success` is at or after `last_pass.end`, and the copy
   record check reports 0 differences. If `copy_pending` is `true`, get the login (SKILL.md,
   preflight), run `labdata papers copy`, and read it again. No step here lists every file of
   the paper store: that section's full check runs only to repair, with the user's yes.

Done when: every check on this page has an outcome.
