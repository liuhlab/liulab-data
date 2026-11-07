---
search:
  exclude: true
---

# Starting a job on arc on a schedule, with nobody logged in

Probed **2026-11-06** on arc, from a held compute node and with a few read-only commands on the
login node. Nothing was submitted, scheduled or installed. The question: how can a monthly store
refresh start by itself on arc, and how can its last step, the copy to GCS with rclone, tell
cheaply that the daily GCS login has expired?

Evidence classes, as in the earlier notes. **Measured** means a command ran on 2026-11-06 and the
fact is its output. **Read** means the installed man page or an official page says it.
**Inferred** means reasoned from measured or read facts and not checked directly.

## Verdict

**Use a job that resubmits itself.** The first thing each run does is submit the next run with
`sbatch --begin=<next month>`. The next run then waits in the Slurm queue, held by the
controller and not by any node or login session. Slurm `scrontab` is switched off on arc
(measured). Cron runs on the login node and no allow or deny list blocks it (measured), but it
puts a scheduler on the one shared machine that the lab rules keep free of work, and arc
publishes no policy that allows it (read: none found).

**Cons:** a chain of self-submitted jobs breaks if one run dies before it submits its successor,
or if an administrator clears long-pending jobs. Nothing on arc shows such a sweep, but nothing
rules it out either (inferred). A cheap guard: a run that finds no successor in `squeue --me`
submits one.

## Options

| Option | Enabled on arc | Survives preemption | Survives maintenance and reboots | Limits |
| --- | --- | --- | --- | --- |
| Slurm `scrontab` | **No**: `ScronParameters` is unset and `scrontab -l` exits with "scrontab is disabled on this cluster" (measured) | n/a | n/a | n/a |
| Self-resubmitting job (`sbatch --begin`) | **Yes**: `sbatch` runs on compute nodes; `--begin` takes an absolute date or `now+N days/weeks` (read: `man sbatch`) | **Yes**: the pending successor is not running, so it cannot be preempted; a running job is requeued and restarts its script from the top (`JobRequeue=1`, `PreemptMode=REQUEUE`, measured; read: `man sbatch --requeue`) | **Likely**: pending jobs live in the controller's saved state (`StateSaveLocation` is set, measured), not on a node (inferred) | No `--begin` ceiling in the man page or in the config (read, measured). The pending job counts toward the QOS submit cap (200 on `cpu_batch`, 512 on the preemptible QOS, measured). Wall limit per run: 14 days on `cpu_batch` and the preemptible tiers, 2 hours on `quick_preemptible` (measured) |
| Cron on the login node | **Technically yes**: the `cron` service is active and neither `/etc/cron.allow` nor `/etc/cron.deny` exists, so every user may install a crontab (measured) | n/a: cron only submits; the job itself behaves as above | **Yes** if the login machine keeps its spool across reboots (inferred); a missed tick while it is down is not replayed (inferred) | Against the lab rule that the login node is a doorway only. arc publishes no policy on user crontabs (read: none found) |
| Cron on a compute node | **Runs, but unusable**: the `cron` service is active there too (measured), but a crontab would sit on one node and run outside any Slurm allocation (inferred) | n/a | Unknown | Work outside an allocation breaks the cluster's rules (lab rule) |
| Cron or a timer off the cluster (a laptop, a CI service) that runs `ssh arc sbatch …` | Possible, outside arc | n/a | Depends on that host: a laptop sleeps; a CI service needs an arc ssh key stored as a secret (inferred) | Adds an outside host that holds arc credentials; not needed when the in-queue chain works (inferred) |

### Partitions for the refresh

Preemption on arc is by partition priority tier (`PreemptType=preempt/partition_prio`, measured).
`cpu_preemptible` sits at tier 0 and `preemptible` at tier 1; `cpu_batch` and the lab's priority
partition sit at tier 1000 (measured). A resumable refresh fits `cpu_preemptible` or
`preemptible`: when preempted it is requeued and starts again from the top of its script, so
every step must skip work already done. That is how the store pipeline already behaves, since
Snakemake resumes from finished outputs (inferred).

The successor job should name a short wall time and the cheap partition only for the step that
submits the real run, or it can be the real run itself. Both work; the choice is a run-plan
detail (inferred).

## Detecting an expired GCS login

The rclone remote uses a Google OAuth token with a refresh token and no private client ID
(measured: the config has `token` with a `refresh_token`, and no `client_id`). The `expiry` field
in the token is the access token's, which rclone renews with the refresh token on its own. It
says nothing about whether the refresh token still works, so reading it offline does not detect
the daily expiry (read: rclone's Google Cloud Storage docs; inferred for this setup).

The cheap check is one tiny authenticated call before the copy:

```bash
rclone lsjson --max-depth 1 --retries 1 --low-level-retries 1 \
  --contimeout 10s --timeout 20s <gcs destination> >/dev/null
```

With a valid login it exited 0 in about 2 seconds, `pixi run` start-up included (measured). With
a revoked or expired refresh token, Google's token endpoint answers `invalid_grant` (read:
Google's OAuth 2.0 docs), so rclone exits non-zero and prints that word on stderr (inferred; not
measured, since the login was valid at probe time). The copy step then:

1. runs the check;
2. on success, copies;
3. on failure, writes a marker such as `gcs-pending` in the store folder, exits 0 so the DAG and
   the job end clean, and leaves the copy for the next run or for a manual catch-up after the
   next login.

The next monthly run, or a person after the daily re-login, sees the marker and copies. Because
`rclone copy` skips files that already match, a late copy only moves what changed (read: rclone
docs).

## Not found

- Public user docs for arc: none on the nodes (measured: the login node's message of the day is
  empty; no cluster docs under `/usr/share/doc`). Policy on crontabs and on long-pending jobs is
  therefore unknown; ask the arc administrators before relying on cron.
- Whether the controller ever purges jobs pending for weeks: no setting points to it
  (`MinJobAge` applies to finished jobs only, read), but it was not tested. A one-month pilot
  of a single `--begin` job would settle it.

## Sources

- `scontrol show config`, `scontrol show partition`, `sacctmgr show qos`, `scrontab -l`,
  `systemctl is-active` on arc, 2026-11-06.
- Installed man pages on arc: `sbatch(1)` (`--begin`, `--requeue`), `scrontab(1)`, Slurm 25.11.
- Slurm `slurm.conf` documentation: `ScronParameters`, `StateSaveLocation`, `MinJobAge`,
  <https://slurm.schedmd.com/slurm.conf.html>.
- rclone Google Cloud Storage backend, <https://rclone.org/googlecloudstorage/>.
- Google OAuth 2.0 errors (`invalid_grant`),
  <https://developers.google.com/identity/protocols/oauth2>.
