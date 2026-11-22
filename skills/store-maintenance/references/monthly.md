# Monthly maintenance run

The GEO/SRA store's refresh and its light checks, the paper store's checks, then the ontology
store's update. The procedure for each store lives in its how-to; follow it from there, and
record each check below in the log entry. The run ends only when all three stores are
**finalized**: GCS holds each store's new state, and each status record says so.

**Budget:** the refresh, 2 h wall time. Past 2 h, stop and report: the user decides whether a
full rebuild is worth it. The ontology update runs beside it, within its job's time limit.

## GEO/SRA store

Follow "Refresh the store" in `docs/how-to/store-run.md`.

1. **Refresh.** Submit `scripts/store/refresh.sbatch` under the run plan. Probe it in its first
   minutes. If it refuses to start, see [Refusals](#refusals).
2. **Status record.** `refresh-status.json` in the store root: `outcome` is `finalized` and
   `last_success` is today. Record the new build ID from the store marker. An outcome of
   `built` with `copy_pending` `true` means the copy was skipped and the job exited non-zero:
   get the login (SKILL.md, preflight), submit the same refresh again, which does only the copy,
   and read the record again.
3. **Coverage.** `labdata store coverage` ends with `Coverage passes.`
4. **Thin-slice checks.** `pixi run python scripts/store/check_stage1.py` on the held node:
   the thin-slice checks pass with the network blocked. Its `--every-record` flag belongs to the
   yearly page, or to a run the user said yes to.
5. **Step times.** A table with one row per refresh step: its wall time, from the first and
   last timestamps in its `logs/<step>.log` under the status record's `folder`, then the total
   against the 2 h budget. Put the table in the report and the log entry.
6. **Refresh logs.** `scripts/store/keep_run_logs.sh "$LABDATA_STORE" refresh/<started>`, with
   `<started>` from the status record's `folder`. Deleting `work/` after that needs the user's
   yes.
7. **GCS copy.** The routine check of "Check the copy", in `docs/how-to/store-gcs.md`, passes:
   `outcome` is `finalized` and `copy_pending` is `false` in `refresh-status.json`, and the copy
   record check reports 0 differences. No step here lists every file of the store: that
   section's full check runs only to repair, with the user's yes.

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
2. **Status record.** `paper-status.json`: `last_pass.outcome` is `success` and
   `last_daily_success` is today (yesterday before 00:30 UTC). Record the `counts` by status in
   the log entry.
3. **GCS copy.** The routine check of "Check the copy", in `docs/how-to/papers-run.md`, passes:
   `copy_pending` is `false`, `finalized` is `true`, and the copy record check reports 0
   differences. If `copy_pending` is `true`, get the login (SKILL.md, preflight), run
   `labdata papers copy` between passes, and read it again; the job also retries once an
   hour. A job log that names `labdata papers restore` means a refusal: see
   [Refusals](#refusals). No step here lists every file of the paper store: that section's full
   check runs only to repair, with the user's yes.

## Ontology store

Follow "Update the store" in `docs/how-to/ontology-run.md`.

1. **Update.** Submit `scripts/ontology/update.sbatch` under the run plan, with
   `LABDATA_ONTOLOGIES` set. It needs no GEO/SRA build, so submit it beside the refresh. If it
   refuses to start, see [Refusals](#refusals). For a vocabulary whose host refuses the
   cluster, download its file off the cluster, copy it over and pass `--source`: see "When a
   source host refuses the cluster" in `docs/how-to/ontology-run.md`.
2. **Status record.** `ontology-status.json` in the ontology store root: `outcome` is
   `finalized`, `end` is today, and it has one row per vocabulary declared in
   `src/labdata/ontology/vocabularies.py`. Put a table in the log entry, one row per
   vocabulary: its `result`, `snapshot` and `terms`, and the `reason` of a failed one. Each
   failed row is a fail with its own issue; that vocabulary keeps its snapshot. Once its cause
   is fixed, submit the same job again: it downloads only what is still missing. With
   `copy_pending` `true`, get the login (SKILL.md, preflight), submit the job again, which
   copies first, and read the record again.
3. **GCS copy.** The routine check of "Check the copy", in `docs/how-to/ontology-run.md`,
   passes: the copy record check reports 0 differences.

## All finalized

The last check, read after every other check on this page. Pass when all three hold at once:

- `refresh-status.json`: `outcome` is `finalized`, `copy_pending` is `false`, and `build` is
  the build ID recorded in the GEO/SRA status-record step.
- `paper-status.json`: `finalized` is `true` and `copy_pending` is `false`.
- `ontology-status.json`: `outcome` is `finalized` and `copy_pending` is `false`.

Log it as its own row, last in the entry's table: `All stores finalized`, with the build ID
and the time each record was read. This check is never skipped and never ends as fail: until it
passes, the run stays open, and each report names what holds it.

## Refusals

Each update and each copy can refuse before it changes anything. The message names the cause.

- **Another update holds the run lock.** Each store has its own. If the message says the
  holder still runs, wait for it.
  If it says this host cannot check, run `squeue` on the holder's cluster. Once the holder has
  ended, resubmit with `--owner-gone`. A refresh killed inside a job that still runs, such as
  the held `labdata-dev` job, looks alive from any other node: start the next refresh on that
  node, where the check asks the process itself.
- **The work disk is behind GCS.** The copy record on the disk is not the one in GCS, or the
  disk has none while GCS does. Stop and report: the fix is the restore the message names
  (`labdata store restore`, `labdata papers restore` or `labdata ontology restore`, per
  "Restore the store from GCS" in the how-to). A restore downloads out of GCS, so it needs a
  job in the run plan, and the user's yes past 1 GB. A GEO/SRA or ontology restore takes its
  store's run lock, so it refuses while an update runs. For the paper store, stop the paper
  job before the restore and start it after.
- **GCS holds no copy record.** The message asks for `--full`. Stop and report; a full copy
  needs the user's yes.

Done when: every check on this page has an outcome, and "All finalized" has passed.
