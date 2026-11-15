---
search:
  exclude: true
---

# GCS holds the paper store, so no copy deletes by mirroring

This amends [ADR 0010](0010-paper-copy-sends-changes.md). GCS keeps the paper store for good.
The cluster disk is a work area that will not be kept. So the bucket is the source of truth: a
delete in it is only a change the store recorded, never the result of comparing the bucket with
the work disk.

- `labdata papers copy --full` uploads every file with `rclone copy`, the paper files before
  the tables that name them, and writes the copy record, `.copied-files.parquet`. It deletes
  nothing. No paper copy runs `rclone sync`.
- A routine copy deletes only the paths that left the `files` table since the copy record. It
  refuses, sends nothing and sets `copy_pending` when the table is missing or empty, or when
  more than 5% of the copy record's paths, and more than one, left it. A pass drops far
  fewer: the PDFs PMC replaces, and the files PMC renames or stops serving. A damaged table
  is likelier, and its own tables would replace the bucket's.
- Each successful copy uploads the copy record to the bucket, at the same path, after the
  files it names and the deletes, and only then saves it on the disk. A work disk without it
  fetches the bucket's, which is small; only when the bucket has none either does the copy ask
  for `--full`.

## Considered options

- **Mirror on request.** The old `--full` ran `rclone sync --delete-excluded`. Run from a fresh
  or partial work disk, it deletes from the bucket every file the disk lacks.
- **Keep the copy record on the work disk only.** A fresh disk lacks it, the copy asks for `--full`,
  and a mirror then deletes.
- **No bound on deletes.** A truncated `files` table would delete its share of the bucket in one
  copy.

## Consequences

- A top-level file deleted from the store stays in the bucket. Removing it is a step by hand.
- A copy past the bound stops the bucket's updates until someone checks the table. If the
  files left on purpose, such as in a layout change that moves many files at once,
  `labdata papers copy --accept-deletes N`, run by hand, deletes up to N of them.
