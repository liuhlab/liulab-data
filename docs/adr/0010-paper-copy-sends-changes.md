---
search:
  exclude: true
---

# The paper copy sends changes, read from the files table

Amended by [ADR 0011](0011-gcs-holds-the-paper-store.md): the saved file, the copy record,
also goes to the bucket, a work disk without it fetches it from there, and no copy deletes by
mirroring.

The paper job's copy to GCS sends only what changed since the last copy that worked. It lists
only the top level of the store and of the bucket, so its cost follows the day's changes, not
the size of the store. The bucket now matches the store because each change was applied, not
because a full comparison found no difference.

The changes come from the `files` table. Every paper file the store holds has a `files` row
with its path, MD5 and size, and the table has one writer. A successful copy saves the held
paths with their MD5, size and fetch date in a hidden file at the root, `.copied-files.parquet`.
The next copy compares the table with it:

- a path that is new, or whose MD5, size or fetch date changed, goes up by name, with
  `rclone copy --files-from-raw <list> --no-traverse`;
- the small top-level files, the tables among them, go up every time;
- a path that has left the table is deleted in the bucket, with `rclone delete --files-from-raw`.

A copy that fails or is skipped on an expired login leaves the saved file and
`last_copy_success` in `paper-status.json` as they were, so the next copy sends everything since
the last success.

## Considered options

- **Full mirror every time.** rclone lists every paper file on both sides, twice, after every
  pass.
- **Rows by date.** `files` gives a fetch date, not a time, and a file moved to a new path keeps
  its old date, so a date cut-off misses moves.
- **A change list the deleting code appends to.** Every place that writes, moves or deletes a
  paper file would need to log it. The PMC sync alone deletes in several places, and the first
  one forgotten loses a change for good.
- **Compare the table with the last copy's table.** No call site can forget it: a write, move or
  delete changes the table. It reads four columns of `files`, which needs no walk of the store.

## Consequences

- A paper file without a `files` row would never be copied. Every tier writes a row for each
  file it holds.
- A change made to the bucket by hand is not seen. `rclone check` finds it, and
  `labdata papers copy --full` restores a missing or changed file.
