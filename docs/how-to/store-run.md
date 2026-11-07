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

To keep each round's logs with the store, copy them into `support/`. `<round>` is the round's
folder under `work/run/`, such as `2026-10-14-all`:

```bash
scripts/store/keep_run_logs.sh "$LABDATA_STORE" <round>
```

The build writes `README.md` at the store root. It says what each folder holds, how it was
made, and how to open the store as a database. After you copy logs into `support/` or delete
`work/`, write it again with fresh folder sizes:

```bash
labdata store readme
```

The run keeps the old build for a grace period after the switch, so a reader that opened it
can finish; the [API reference](../reference.md#labdata.store.prune) gives its length.
`labdata store prune --grace 0` deletes it and the snapshots only it read at once. Use that
only as a one-off to tidy a store nobody is reading, not as a refresh step. Prune never
deletes the current build or anything it reads.

## Refresh the store

Once the full download is done, one command a month keeps the store current. It downloads new
snapshots, fetches the GEO Series that changed or appeared since the last successful refresh,
removes the GEO Series that a fetch confirms are withdrawn, builds, switches, prunes and copies. Run it from
the repository folder:

```bash
labdata store refresh
```

On a Slurm cluster, run it as one job:

```bash
export LABDATA_STORE=/path/to/store
sbatch scripts/store/refresh.sbatch
```

The first refresh searches for changes from the date of the `geo-series` snapshot the current
build read; each later one, from the last successful refresh, less a few days of overlap
(`labdata store refresh --help` gives the number). To search from another date, add
`--since YYYY-MM-DD`.

If it stops, run the same command again. It goes on in the same folder,
`work/refresh/<start time>/`, from the first unfinished step, and asks for nothing it saved.
That folder holds the refresh's markers and logs.

A refresh removes a GEO Series only when GEO no longer lists it, or the refresh got no record
for it, and a fetch of it alone then says it is gone. It keeps every SRA record.

A refresh that an older version of liulab-data started is not resumed: the next run starts a
new one from the same date and leaves the old folder in place.

`refresh-status.json` in the store root shows the outcome:

```json
{
  "started": "2026-11-07T02:00:00Z",
  "ended": "2026-11-07T03:24:10Z",
  "outcome": "succeeded",
  "since": "2026-10-14",
  "last_success": "2026-11-07",
  "folder": "work/refresh/2026-11-07T020000Z",
  "counts": {"geo-soft": {"added": 2410, "replaced": 4180, "removed": 12}},
  "copy_pending": false
}
```

`last_success` changes only once the store has switched to the new build. When rclone's login
has expired, the refresh still succeeds and `copy_pending` is `true`. Log in, then run
`labdata store copy`: it copies the store and sets `copy_pending` back to `false`.

### What a refresh needs

Every step can be killed and run again. The figures below come from the first full-store
refresh, a month after the last fetch, with 32 CPUs and 90 GB. A dash means no load worth
counting; "not measured" means the step did not run in that refresh.

| Step | CPU | Memory | Scratch disk | Network (NCBI, GEO, GCS) | Wall time |
| --- | --- | --- | --- | --- | --- |
| snapshots | 1 core | 1 GB | 43 GB new | 45 GB from NCBI FTP, 600 E-utilities calls | 42 min |
| targets | 1 core | 1 GB | – | – | 1 min |
| changes | 1 core | 1 GB | – | 1 E-utilities search | 2 min |
| geo | 1 core | 4 GB | 2 GB | 6,500 GEO FTP requests, 1.6 GB | 9 min |
| eutils (beside and after the GEO lane) | 1 core | 4 GB | 1 GB | 320 E-utilities calls, 0.7 GB | 12 min, then 4 min |
| extract (each source) | up to 37 cores | up to 38 GB | 18 GB | – | 10 min (biosample), under 1 min (others) |
| confirm | 1 core | 1 GB | – | one E-utilities call per removal candidate | not measured |
| pack | 1 core | 3 GB | rewrites the raw buckets it touches | – | 7 min |
| build | not measured | not measured | 22 GB, one build | – | 44 min in the last full build |
| status | not measured | | | | |
| prune | not measured | | | | |
| copy | not measured | | | the store, to GCS | |

## Make a sample build

A sample build is a small copy of a full store's tables, for trying a change before you
build the whole store. It holds a fixed random share of each record type, made by the same
code that makes a full build. Give it the full store, an empty folder for the sample and the
share:

```bash
labdata store build --sample-of /path/to/store --root /path/to/scratch --fraction 0.01
```

The full store is only read: its raw files, snapshots and current build stay as they are.
The sample is built in the scratch folder, which becomes a store of its own, so
`open_store("/path/to/scratch")` opens it. A scratch folder takes samples of one store only.
The same share always picks the same records, and a smaller share's records are all inside a
larger one's. Notes are the exception: they can differ between shares.

Series and SRA experiments hold about the share you asked for. Samples, platforms,
BioProjects and BioSamples hold more, because they also hold what the sampled records link to.

To time queries on the sample:

```bash
pixi run python scripts/store/benchmark.py /path/to/scratch
```

The benchmark also prints the share of links whose far end the sample holds. Most links
point outside a sample, so its link-query timings are indicative only.

## Go deeper

The steps are also commands of their own: `labdata store pack`, `build`, `coverage`, `prune`
and `copy`. Run `labdata store --help` to list them, and see the
[API reference](../reference.md) for the modules behind them.
