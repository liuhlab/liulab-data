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
to finish. Without accounts, the daily pass skips OpenAlex and says so. A running job reads
the accounts at each pass, so new ones count from the next pass, with no restart.

For the copy to Google Cloud Storage (GCS), set up the `gcs` rclone remote and log in as
[Copy the store to GCS](store-gcs.md) shows. Leave out `--copy-to` to run with no copy.
The job's rclone reads your own config, `~/.config/rclone/rclone.conf`, even when your shell
sets `RCLONE_CONFIG`; set `LABDATA_RCLONE_CONFIG` before `sbatch` to use another file.

## Fill the store the first time

The first PMC sync takes in every linked paper, so it runs much longer than a monthly one: plan
for most of a day. It stores about half a terabyte, mostly supplementary files and figures,
so check the free space first. Submit it once by hand, from the repository folder on the
cluster, with more CPUs and a longer time limit:

```bash
export LABDATA_PAPERS=/path/to/paper-store
df -h "$LABDATA_PAPERS"
sbatch --job-name=labdata-papers-backfill --partition=cpu_batch \
  --cpus-per-task=4 --mem=8G --time=2-00:00:00 \
  --output=labdata-papers-backfill.%j.log \
  --wrap "pixi run labdata papers pmc-sync"
```

If it stops, submit it again. It goes on from where it stopped. The paper job can start at the
same time: it waits until the first sync is done.

To fill the store in rounds, add `--sample 0.01`, then `--sample 0.1`, then run it with no
`--sample`. Each round skips the papers the last round looked up this month. A sampled sync
does not count as the month's PMC sync: `paper-status.json` records it as the last pass, with
its `sample` share, but only a sync over all papers sets `last_pmc_sync_success` and
`last_pmc_sync_build`. So the paper job still runs the month's PMC sync after a sampled round.

The first copy to GCS sends all of it, with `--full`. It uploads every file and deletes
nothing in the bucket. Run it once by hand, as its own job,
after the first sync ends, so the paper job's daily pass does not wait behind it:

```bash
sbatch --job-name=labdata-papers-copy --partition=cpu_batch \
  --cpus-per-task=2 --mem=8G --time=1-00:00:00 \
  --output=labdata-papers-copy.%j.log \
  --wrap "pixi run labdata papers copy --full"
```

Log in to rclone first: the login lasts one day. Later copies send only the files added,
changed or deleted since the last copy that worked, so their cost follows the day's changes,
not the size of the store. Each copy ends by sending its copy record, a hidden file at the
paper store root that lists what the bucket holds. A new work disk without that file fetches
it from the bucket. Until a full copy has worked once, the job's copies do nothing and the log asks for
`--full`.

## Start the paper job

```bash
export LABDATA_PAPERS=/path/to/paper-store
sbatch scripts/papers/serve.sbatch
```

The job asks for 2 CPUs and 8 GB on `cpu_batch`, for the 14 days that partition allows.
It runs the daily pass each day after 00:30 UTC, and the PMC sync on the 3rd of each month.
It also runs the PMC sync when a refresh makes a new GEO/SRA build current, so the papers the
new build links come in soon after. That sync counts as the month's, so a refresh on the 1st or
2nd leaves no sync for the 3rd.
Each pass ends with a copy of its changes to GCS. The job never runs a full copy.

PMC gives the article text, figures and supplements, while OpenAlex and host links give a PDF
only. So when a paper fetched from OpenAlex or a host link later appears in PMC, the PMC sync
takes its PMC files and then deletes the old PDF.

At start, the job makes sure two more jobs wait behind it. When the running job ends, the
next one takes over; if that one fails at start, the one after it takes over. So the chain
goes on by itself: Slurm on arc accepts a job submitted from inside a running job.

To stop the chain, cancel the waiting jobs first, then the running one:

```bash
squeue --me -n labdata-papers
scancel -u "$USER" -n labdata-papers -t PENDING
scancel <running job id>
```

If the chain breaks, submit `scripts/papers/serve.sbatch` again by hand. A new job does only
what is due: a day with no daily pass, a month with no PMC sync after the 3rd, or a new build
since the last PMC sync. The job log says `successor not submitted` when it could not submit a
waiting job.

## Watch it

The job log, `labdata-papers.<job id>.log`, prints a line a minute. The first job writes it
where `sbatch --output` says, or into the folder you submitted from. Each next job writes its
log into the same folder. Between passes it looks like this:

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
  "last_pmc_sync_build": "20261031T020000Z",
  "fetches": {"date": "2026-11-05", "by_tier": {"openalex": 300, "host": 2}},
  "quota": {"date": "2026-11-05", "by_key": {"a@lab.org": 100, "b@lab.org": 100, "c@lab.org": 100}},
  "counts": {"fetched": {"": 80512}, "waiting": {"openalex-backlog": 7690, "pmc-embargo": 1204},
             "manual": {"no-route": 9300, "host-blocked": 120}, "skipped": {"retracted": 41}},
  "copy_pending": false,
  "last_copy_success": "2026-11-05T01:12:41Z",
  "paperpile_pending": false
}
```

- `last_daily_success` older than today, after 00:30 UTC, means a missed day.
- `last_pmc_sync_success` is the date of the last PMC sync over all papers. A sampled sync
  leaves it alone.
- `last_pmc_sync_build` is the GEO/SRA build that sync read. When the store's current build
  differs, the next PMC sync is due. A sampled sync leaves it alone too.
- `quota` shows the copies each OpenAlex account bought today.
- `counts` gives the papers per status, then per reason.
- `copy_pending: true` means the bucket is behind: the GCS login had expired, a copy failed
  or refused, or no full copy has worked yet. Log in, then run `labdata papers copy`, or
  `labdata papers copy --full` if the log asks for it. A copy that works sets it back to
  `false`.
- `last_copy_success` is when the last copy that worked started. The next copy sends what
  changed after it.
- `finalized: true` means the bucket holds the store's last change: a copy sent this record.

The copy reads the changes from the `files` table, so it lists only the top level of the
paper store and the bucket. It sends `paper-status.json` last, after its copy record, marked
`finalized`. It leaves out the lock file and the files a pass writes while it runs.

GCS keeps the paper store for good; the cluster disk is a work area. So a copy deletes in the
bucket only the files that left the `files` table. It refuses, sending nothing, when the table
is missing or empty, or when more than a small share of the copied files left it at once: the
table may be damaged. The log says why, with the numbers. Check the table. If the files left
on purpose, as when a change moves many files at once, run the copy by hand with the number
of files the log gives. For 1200 files:

```bash
labdata papers copy --accept-deletes 1200
```

It sends the changes and deletes those files in the bucket, up to that number.

## Check the copy

The routine check reads the status record and the copy record, and lists no paper folder.
Run it between passes. The copy is current when both of these hold:

- In `paper-status.json`, `copy_pending` is `false`, and `last_copy_success` is at or after
  `last_pass.end`: the copy after the last pass worked. If `copy_pending` is `true`, log in
  and run `labdata papers copy`.
- The copy record in the bucket matches the one on the disk. Each copy sends it last, after
  the files it lists:

  ```bash
  pixi run rclone check "$LABDATA_PAPERS" <copy_to> --max-depth 1 \
    --include "/.copied-files.parquet"
  ```

  It should report 0 differences and 1 matching file. It lists only the top level of each
  side.

A full check finds what differs when the copy record does not match, or after a change made
in the bucket by hand. It lists every file on both sides, so run it only to repair the copy,
and only when the maintainer says yes. Run it between passes too: a pass that runs beside it
changes files under it. Like the copy, it leaves out the hidden files. The status record
differs once a pass has run since the last copy:

```bash
pixi run rclone check "$LABDATA_PAPERS" <copy_to> --one-way \
  --filter "- .*" --filter "- .*/**" --filter "- /paper-status.json"
```

It should report 0 differences. If it finds differences, `labdata papers copy --full` repairs
them. It deletes nothing.

## Restore the store from GCS

GCS keeps the paper store for good. If the work disk is lost or fell behind, get the store
back from GCS before the paper job runs again. An empty disk gets the whole store; a disk that
fell behind gets only the files that changed. Downloads out of GCS cost money, so run a large
restore only with the maintainer's yes.

Stop the paper job first, as [Start the paper job](#start-the-paper-job) shows. Then run the
restore as its own job:

```bash
export LABDATA_PAPERS=/path/to/paper-store
sbatch --job-name=labdata-papers-restore --partition=cpu_batch \
  --cpus-per-task=2 --mem=8G --time=1-00:00:00 \
  --output=labdata-papers-restore.%j.log \
  --wrap "pixi run labdata papers restore --from <copy_to> --accept-download"
```

The restore prints how much it downloads. In a terminal it asks before more than 1 GB. A
batch job has no terminal, so it needs `--accept-download` for that. Without the flag, a batch
job prints the size and stops before a download of more than 1 GB.

If it stops, submit it again: it skips the files it already has. If the GEO/SRA store moved
too, set `store` in `labdata-papers.toml` to its new folder. Then start the paper job.

The restore saves the copy record last, so the job's copies send only what changes from then
on. It saves `paper-status.json` as the last copy left it, so the job knows when the last
daily pass and the last PMC sync ran. It leaves the hidden files of the disk alone, the daily
pass's journal among them.

Restore before any copy by hand. A full copy, `labdata papers copy --full`, first reads the
list of files the bucket holds. It refuses, and sends nothing, when the disk lacks more than a
few of them, as a disk that was not restored does. Its tables would replace the bucket's.
