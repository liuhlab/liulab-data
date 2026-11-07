---
name: store-maintenance
description: Run the monthly or yearly maintenance run on the GEO/SRA store and the paper store, then log it.
disable-model-invocation: true
argument-hint: monthly | yearly
---

# Store maintenance

One **maintenance run** (`GLOSSARY.md`) on both stores, by one procedure: preflight, the
period's page, reports while it runs, one log entry at the end. The **period** is the argument.

## 1. Period

The period is `monthly` or `yearly`, given as the argument. Any other argument, or none: ask
"Monthly or yearly?" and wait for the answer. The date, the last log entry and the store's
state never decide it.

Done when: the user has named the period.

## 2. Preflight

Load `lab-hpc` before the first ssh. Then confirm all three, and state each to the user in one
line:

1. **Held node.** The standing `labdata-dev` job runs on arc. Short commands and checks run
   inside it, never on a login node. If it is gone, resubmitting it is the run plan's first job.
2. **rclone login.** The Google login lasts one day. `pixi run rclone lsd <copy_to>` lists the
   bucket for each store (`copy_to` in `labdata-store.toml` and in `labdata-papers.toml`). If
   it fails, ask the user to log in as `docs/how-to/store-gcs.md` shows, and wait. A run with no
   login can still refresh, but its copy and `rclone check` steps stay open until the login.
3. **Run plan.** An approved run plan (`hq-mode`'s working-style page) covers every job of this
   run. Open the run issue, titled `<Period> maintenance run YYYY-MM-DD`, and post the plan
   there: work folder, partitions, most CPU, memory and wall time per job, most jobs at once,
   and the target measure, which is the period page's budget. Show the exact scripts. Then stop
   until the user says yes. Quote the plan word for word in every subagent brief.

Done when: all three hold, and the plan's yes is on the run issue.

## 3. The period's page

- `monthly`: [`references/monthly.md`](references/monthly.md).
- `yearly`: [`references/yearly.md`](references/yearly.md). It ends by running the monthly page.

Every check on the page ends in one **outcome**: `pass`, `fail` with the issue filed, or
`skipped` with its reason. A failed check gets its own issue; carry on with the checks that do
not depend on it.

Done when: every check on the page has an outcome.

## 4. Reports

**Watch jobs early.** Probe each job in its first minutes, and diagnose a slow step at once.
Post a report on the run issue after each phase and whenever the estimate moves:

- **Progress**: steps done out of the page's steps, with each outcome so far.
- **Best estimate per phase**: wall time left for each phase, against the budget.
- **Levers**: what would change the estimate, such as a busy node, a slow NCBI reply or an
  expired login.

Mark every number measured or inferred. Past the budget, or on sustained HTTP 429 or 503 replies
from NCBI, stop the run's jobs and report; the user decides what comes next.

## 5. Log

Add one entry to `docs/maintenance-log.md`, newest first, in the shape of the entries already
there: the date, the period, the build ID, each check's outcome, wall time against budget, and
links to the issues filed. The repo is public: the entry names no hostname, username, absolute
path or bucket. Write the same way in every issue and comment of the run.

Commit the entry on a branch named `maintenance-YYYY-MM-DD`, run `pixi run check`, push, and
open a PR that links the run issue.

Done when: the PR is open, the run issue links it, and every check on the page appears in the
entry.
