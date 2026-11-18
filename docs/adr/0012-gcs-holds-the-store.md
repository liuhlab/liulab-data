---
search:
  exclude: true
---

# GCS holds the GEO/SRA store too, and both copies share one delete bound

Amended by [ADR 0013](0013-finalized-when-gcs-holds-it.md).

This amends [ADR 0011](0011-gcs-holds-the-paper-store.md). As for the paper store, the bucket
keeps the GEO/SRA store and the cluster disk is a work area. The store holds its current state
only, so a file that leaves it leaves the bucket too, and no routine step lists every file.

So `labdata store copy` works as the paper copy does. The current build's manifest names the
current state: its files and the raw files and snapshots it read, with their sha256. A copy
record, `.copied-files.json` in the store and in the bucket, names what the last successful
copy sent. A routine copy uploads by name what is new or changed against it, then the marker;
deletes by name what the record names and the manifest no longer does; and uploads the record
last. A side file the manifest does not name, such as a README, leaves the bucket only with the
build, raw source or snapshot that holds it. No store copy runs `rclone sync`.

Both copies bound their deletes the same way:

- A copy may delete one path, or 5% of the copy record's paths if that is more; the store copy
  counts its raw files. Past that it refuses and sends nothing.
- `--accept-deletes N`, run by hand, deletes up to N, for files removed on purpose. `--full`
  deletes nothing and takes no `--accept-deletes`.
- `labdata papers copy --full` first fetches the bucket's copy record and refuses past the same
  bound, so a disk without the whole store never replaces the bucket's tables.

## Considered options

- **Mirror, as before.** `rclone sync` deleted every file the disk lacked.
- **Delete what prune deleted.** Pack also deletes raw files; a step that forgets to log its
  deletes leaves files in the bucket for good.
- **The bucket's marker and manifest as the record.** A copy cut off after the marker loses
  track of the old build. They only seed the record.
- **5% alone.** A store with fewer than 20 recorded files refuses every delete.

## Consequences

- An old build and the snapshots only it read leave the bucket at the first copy after the
  switch. Prune's grace period applies to the disk only.
- A change of bucket width replaces a source's raw files at once: the copy refuses, and
  `--accept-deletes` with the count it gives applies the change.
- With no copy record anywhere, a routine copy seeds one from the build the bucket's marker
  names, if that build is on disk, the last copy worked and GCS lists its files.
  Otherwise it asks for `--full`.
