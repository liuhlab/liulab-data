---
search:
  exclude: true
---

# Trying a store change

Most changes to the store's code or table layout don't need the full build. Use the smallest
build that answers the question, and run the full build once, at the end.

| Question | Smallest build that answers it |
| --- | --- |
| Does the code run, and does each table match its declaration (`layout.check`)? | The thin-slice build in the tests (`tests/_slices.py`) |
| How does a setting move file size or write time: codec, level, row-group size? | A sample build |
| Query limits (`scripts/store/benchmark.py`), row groups a lookup reads, build time and memory | The full build, checked with `scripts/store/compare_builds.py` |

Write every trial through `layout.write`, the only code that writes build tables. Another
writer gives other sizes for the same settings.

Make a sample build as [the store how-to page](../how-to/store-run.md#make-a-sample-build)
shows. It reads the store's `raw/` and `snapshots/` and writes only the scratch root. Set
`LABDATA_THREADS` when you run `benchmark.py` on it, or the peaks won't compare.
