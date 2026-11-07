# Maintenance log

Each maintenance run of the stores adds one entry here, newest first. A monthly run refreshes
the GEO/SRA store and checks both stores. A yearly run adds the heavy checks and removes
withdrawn SRA records. The steps are in [Run the full download](how-to/store-run.md) and
[Run the paper store](how-to/papers-run.md).

Each check ends as pass, fail or skipped. A failed check links the issue filed for it.

## 2026-11-07, monthly

The first refresh of the full store, a month after the full download.

- **Build:** `20261107T163933Z`
- **Wall time:** 65 min, against a budget of 2 h. That day's snapshots were already on disk, so
  this run cost less than a normal month. A normal month should take about 1 h 55 min.

| Check | Outcome |
| --- | --- |
| Refresh | Pass. The store switched to the new build. No HTTP 429 or 503 replies. |
| A changed and a withdrawn Series, through the API | Pass. GSE183765 shows its new Samples; GSE155703 is gone. |
| Coverage | Pass. 200,132 target Series: 200,131 held, 1 gap (`soft-missing`). |
| Quick checks, network blocked | Pass. All 13. |
| Every record, network blocked | Pass. All 31,127,761 records open and their hashes match. 2 h 52 min. |
| Refresh logs kept in `support/` | Pass. |
| Store copy to GCS, `rclone check` | Pass after a fresh login: 0 differences in 148 files. |
| Rebuild with no network | Skipped by the maintainer's choice: the refresh's own build used no network. |
| Second refresh | Skipped by the maintainer's choice: its change list was the same 388 Series. |
| `readme 4` query | Pass. 0.98 s against 2 s. |
| Three other benchmark queries | Fail on a busy node: [re-run on a quiet node](https://github.com/liuhlab/liulab-data/issues/161). |
| Paper store | Skipped: not in service yet. |

Issues filed:

- [Three benchmark queries over their limits](https://github.com/liuhlab/liulab-data/issues/161)
- [Yearly heavy check and SRA removal](https://github.com/liuhlab/liulab-data/issues/158)
- [Maintenance skill and log](https://github.com/liuhlab/liulab-data/issues/159)
