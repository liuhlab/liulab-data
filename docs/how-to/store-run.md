# Run the full download

One command fetches every target GEO Series and the records it links to, builds the tables,
switches the store to the new build, deletes old builds and copies the store to the bucket.
It runs on the cluster as one Slurm job, and you can stop and restart it at any time.

## Before you start

- Download the bulk snapshots with `scripts/bulk/download_ncbi_bulk.py`. The run uses the
  newest snapshot of each source.
- Save your NCBI email and API key with `labdata ncbi configure`.
- To copy the store at the end, set `copy_to` in `labdata-store.toml` and log in to rclone
  that day. See [Copy the store to GCS](store-gcs.md). Without `copy_to`, the run skips the
  copy and says so.

## Start a round

Run the download in three rounds: 1%, then 10%, then all of the target list. Each round's
Series are a fixed random sample, and a smaller round's Series are all inside the next one.
A larger round never fetches again what a smaller round saved.

From the repository folder on the cluster:

```bash
export LABDATA_STORE=/path/to/store
sbatch scripts/store/run.sbatch 0.01   # round 1
sbatch scripts/store/run.sbatch 0.1    # round 2
sbatch scripts/store/run.sbatch        # round 3: every target Series
```

Start the next round only when the last one looks right. Off a cluster, or for a quick try,
run the same steps straight away:

```bash
labdata store run --fraction 0.01
```

## Watch it

The job log, `labdata-run.<job id>.log`, shows each step as it starts and ends. The fetch
steps print a line at least once a minute:

```text
2026-10-15 21:04:10 labdata.store.geo_lane 1,204/1,985 Series SOFT, round 1 (60.7%), 26.8/s, errors: timeout 3, finish ~2026-10-15 21:05 UTC
```

Each step also writes its own log under `work/run/<snapshot date>-<fraction>/logs/` in the
store. That folder holds the round's markers too: one file per finished step.

At the end, the coverage step checks that every Series in the round is held or explained:

```text
Target Series: 1985 (geo-series snapshot 2026-10-14)
Held: 1983
Gaps: 2
  soft-missing: 2
  fetch-failed: 0
  unexplained: 0
SRA experiments fetch-failed: 0 (not Series gaps)
Coverage passes.
```

If coverage fails, the run stops there: it does not delete old builds or copy the store.

## Resume

Submit the same command again. The run starts at the first step that did not finish, and the
fetch steps skip every Series and experiment already saved. A job that the cluster stops to
make room is put back in the queue and resumes by itself.

The fetch steps ask once more for every item that failed before they finish. A Series that
still fails is a `fetch-failed` gap, and an SRA experiment that still fails is counted on the
`SRA experiments fetch-failed` line. To ask for those again, submit the same round again:
the fetch steps ask only for what failed, and the steps after them run again. Nothing saved
is fetched twice.

## After the last round

The build writes `README.md` at the store root. It says what each folder holds, how it was
made, and how to open the store as a database. After you move logs into `support/` or delete
`work/`, write it again with fresh folder sizes:

```bash
labdata store readme
```

The run keeps the old build for a day after the switch. To delete it and the snapshots only
it read straight away, run `labdata store prune --grace 0`. Prune never deletes the current
build or anything it reads.

## Go deeper

The steps are also commands of their own: `labdata store pack`, `build`, `coverage`, `prune`
and `copy`. Run `labdata store --help` to list them, and see the
[API reference](../reference.md) for the modules behind them.
