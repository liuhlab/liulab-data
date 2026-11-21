---
name: store-maintenance
description: Run the monthly or yearly maintenance run on the GEO/SRA store, the paper store and the ontology store, then log it.
disable-model-invocation: true
argument-hint: monthly | yearly
---

# Store maintenance

One **maintenance run** (`GLOSSARY.md`) on the three stores, by one procedure: preflight, the
period's page, reports while it runs, one log entry at the end. The **period** is the argument.
A run ends only once all three are **finalized** (`GLOSSARY.md`): GCS holds what the run
made.

## 1. Period

The period is `monthly` or `yearly`, given as the argument. Any other argument, or none: ask
"Monthly or yearly?" and wait for the answer. The date, the last log entry and the store's
state never decide it.

Done when: the user has named the period.

## 2. Preflight

Take all five in order, and state each to the user in one line:

1. **Held node.** The standing `labdata-dev` job runs on arc. Short commands and checks run
   inside it. If it is gone, resubmitting it is the run plan's first job.
2. **rclone login.** The Google login lasts one day. `pixi run rclone lsd <copy_to>` lists
   each store's place in GCS (`copy_to` in `labdata-store.toml`, `labdata-papers.toml` and
   `labdata-ontologies.toml`). If it fails, ask the user to log in as
   `docs/how-to/store-gcs.md` shows, and wait. A refresh or an ontology update with no login
   ends `built`, not finalized, and the run stays open until the login and the copy.
3. **Code.** The refresh and the ontology update run the code in the repository folder
   they are submitted from ("Refresh the store" in `docs/how-to/store-run.md`). In that
   folder, if `git status --untracked-files=no` shows local changes, stop and ask the user.
   Otherwise run `git fetch origin`, then `git switch --detach origin/main`. Put the commit,
   `git rev-parse --short HEAD`, in the run plan; the log entry records it.

   Done when: the folder is detached at the `origin/main` just fetched, and the run plan names
   its commit.
4. **Waiting issues.** `gh issue list --label ready-for-human --state open` lists the issues
   that wait on a person. Read each, and keep those that wait on this run, such as a check of
   what the new build brings. Name them in the run plan, with any jobs they need. After the
   period's page, do each one and post its result on it.

   Done when: the run plan names the issues that wait on this run, or says none does.
5. **Run plan.** An approved run plan (`hq-mode`'s working-style page) covers every job of this
   run. Open the run issue, titled `<Period> maintenance run YYYY-MM-DD`, and post the plan
   there: work folder, partitions, most CPU, memory and wall time per job, most jobs at once,
   and the target measure, which is the period page's budget. Show the exact scripts. Then stop
   until the user says yes. Quote the plan word for word in every subagent brief.

Done when: all five hold, and the plan's yes is on the run issue.

## 3. The period's page

- `monthly`: [`references/monthly.md`](references/monthly.md).
- `yearly`: [`references/yearly.md`](references/yearly.md). It ends by running the monthly page.

Every check on the page ends in one **outcome**: `pass`, `fail` with the issue filed, or
`skipped` with its reason. A failed check gets its own issue; carry on with the checks that do
not depend on it.

Done when: every check on the page has an outcome, and each waiting issue from the preflight
has its result posted.

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
there: the date, the period, the build ID, the commit from the preflight, each check's outcome,
wall time against budget (with the page's step-time table when it has one), the ontology
update's table of vocabularies, and links to the
issues filed. The repo is public: the entry names no hostname, username, absolute path or
GCS path. Write the same way in every issue and comment of the run.

Commit the entry on a branch named `maintenance-YYYY-MM-DD`, run `pixi run check`, push, and
open a PR that links the run issue.

Done when: the PR is open, the run issue links it, and every check on the page appears in the
entry.
