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
   the 13 quick checks pass with the network blocked. Its `--every-record` flag belongs to the
   yearly page, or to a run the user said yes to.
5. **Refresh logs.** `scripts/store/keep_run_logs.sh "$LABDATA_STORE" refresh/<started>`, with
   `<started>` from the status record's `folder`. Deleting `work/` after that needs the user's
   yes.
6. **GCS copy.** `rclone check` of the store against its `copy_to` reports 0 differences.
   Leave out what the copy leaves out:

    ```bash
    pixi run rclone check "$LABDATA_STORE" <copy_to> --one-way \
      --filter "- .*" --filter "- .*/**" --filter "- /work/**" \
      --filter "- /support/**" --filter "- /refresh-status.json" \
      --filter "- /builds/<old build>/**"
    ```

    A build or snapshot that prune keeps for its grace period is not in the bucket; add a
    filter for each one instead of counting it as a difference.

## Paper store

Follow `docs/how-to/papers-run.md`. The paper job does the work; these checks read what it did.

1. **Status record.** `paper-status.json` in the paper store root: `last_pass.outcome` is
   `success`, `last_daily_success` is today (yesterday before 00:30 UTC), and `copy_pending`
   is `false`. Record the `counts` by status in the log entry.
2. **This month's PMC sync.** `last_pmc_sync_success` falls in this month, on or after the 3rd.
   Before the 3rd, it falls in last month. A sampled sync never sets it.
3. **GCS copy.** `rclone check` of the paper store against its `copy_to` reports 0 differences.
   Leave out the hidden files and the status record, as the copy does:

    ```bash
    pixi run rclone check "$LABDATA_PAPERS" <copy_to> --one-way \
      --filter "- .*" --filter "- .*/**" --filter "- /paper-status.json"
    ```

    Run it between passes: a pass that runs beside it changes files under it.

Done when: all nine checks have an outcome.
