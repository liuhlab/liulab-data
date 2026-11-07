# Run the paper store

The paper store sits beside the GEO/SRA store. It holds the papers that the store's Series and
BioProjects link to, and the status of each one. One long-running Slurm job, the paper job,
keeps it current. Each day it fetches what it can, and once a month it takes in new papers.

## Set up once

Make the paper store and point it at the GEO/SRA store it reads:

```bash
labdata papers init /path/to/paper-store --store /path/to/store \
  --copy-to gcs:<bucket>/datalake/paper-store
```

Save your logins. The NCBI one fetches PubMed records. The OpenAlex accounts buy paper
copies, 100 a day each:

```bash
labdata ncbi configure
labdata papers configure
```

`labdata papers configure` asks for one email and API key per account. Leave the email blank
to finish. Without accounts, the daily pass skips OpenAlex and says so.

For the copy to Google Cloud Storage (GCS), set up the `gcs` rclone remote and log in as
[Copy the store to GCS](store-gcs.md) shows. Leave out `--copy-to` to run with no copy.

## Fill the store the first time

The first PMC sync takes in every linked paper, so it runs longer than a monthly one. Submit
it once by hand, from the repository folder on the cluster, with a longer time limit:

```bash
export LABDATA_PAPERS=/path/to/paper-store
sbatch --job-name=labdata-papers-backfill --partition=cpu_batch \
  --cpus-per-task=2 --mem=8G --time=3-00:00:00 \
  --output=labdata-papers-backfill.%j.log \
  --wrap "pixi run labdata papers pmc-sync"
```

If it stops, submit it again. It goes on from where it stopped. The paper job can start at the
same time: it waits until the first sync is done.

## Start the paper job

```bash
export LABDATA_PAPERS=/path/to/paper-store
sbatch scripts/papers/serve.sbatch
```

The job asks for 2 CPUs and 8 GB on `cpu_batch`, for the 14 days that partition allows.
It runs the daily pass each day after 00:30 UTC, and the PMC sync on the 3rd of each month.
Each pass ends with a copy of new and changed files to GCS.

At start, the job submits the next job, which waits for this one to end and then takes over.
So the chain goes on by itself. A job submitted from inside a running job works on arc: it was
checked from a compute node with `sbatch --test-only`, which Slurm accepted.

To stop the chain, cancel the waiting job first, then the running one:

```bash
squeue --me -n labdata-papers
scancel <waiting job id>
scancel <running job id>
```

If the chain breaks, submit `scripts/papers/serve.sbatch` again by hand. A new job does only
what is due: a day with no daily pass, or a month with no PMC sync after the 3rd. The job log
says `successor not submitted` when it could not submit the next job.

## Watch it

The job log, `labdata-papers.<job id>.log`, prints a line a minute. Between passes it looks
like this:

```text
2026-11-05T07:01Z idle; last daily pass 2026-11-05, last PMC sync 2026-11-03, copy pending: no
```

Each step can also run alone: `labdata papers daily`, `labdata papers pmc-sync` and
`labdata papers copy`.

## Read `paper-status.json`

The file at the paper store root says what the job did last and where the papers stand:

```json
{
  "last_pass": {"kind": "daily", "start": "2026-11-05T00:30:02+00:00",
                "end": "2026-11-05T01:12:40+00:00", "outcome": "success"},
  "last_daily_success": "2026-11-05",
  "last_pmc_sync_success": "2026-11-03",
  "fetches": {"date": "2026-11-05", "by_tier": {"openalex": 300, "host": 2}},
  "quota": {"date": "2026-11-05", "by_key": {"a@lab.org": 100, "b@lab.org": 100, "c@lab.org": 100}},
  "counts": {"fetched": {"": 80512}, "waiting": {"openalex-backlog": 7690, "pmc-embargo": 1204},
             "manual": {"no-route": 9300, "host-blocked": 120}, "skipped": {"placeholder": 41}},
  "copy_pending": false,
  "paperpile_pending": false
}
```

- `last_daily_success` older than today, after 00:30 UTC, means a missed day.
- `quota` shows the copies each OpenAlex account bought today.
- `counts` gives the papers per status, then per reason.
- `copy_pending: true` means the GCS login had expired and the bucket is behind. Log in, then
  run `labdata papers copy`. A copy that works sets it back to `false`.

The copy never deletes in the bucket, and it leaves out the lock file and the files a pass
writes while it runs.
