---
search:
  exclude: true
---

# GCS holds the GEO/SRA store too, so its copy sends changes and deletes by name

[ADR 0011](0011-gcs-holds-the-paper-store.md) made the bucket the paper store's source of truth.
The same holds for the GEO/SRA store: the bucket keeps it, and the cluster disk is a work area.
The store holds its current state only, so a file that leaves it must leave the bucket too. No
routine step lists every file.

So `labdata store copy` works as the paper copy does. The current build's manifest names the
current state: the build's files and the raw files and snapshots it read, each with its sha256.
A copy record, `.copied-files.json` at the store root and in the bucket, names what the last
successful copy sent. A routine copy uploads by name the files that are new or changed against
it, then the marker. It then deletes by name the files the record names and the manifest does
not, and uploads the record last. No store copy runs `rclone sync`.

## Considered options

- **Mirror, as before.** `rclone sync --delete-excluded` deleted from the bucket every file the
  disk lacked, and listed every file on both sides at each copy.
- **Delete what prune deleted.** Prune would list the folders it removed for the copy to
  delete. Pack also deletes raw files, so every step that deletes would need to log it, and one
  forgotten leaves a file in the bucket for good: the reason ADR 0010 reads the `files` table.
- **The bucket's marker and manifest as the record.** No new file, but a copy cut off after
  the marker and before the deletes loses track of the old build.

## Consequences

- An old build and the snapshots only it read leave the bucket at the first copy after the
  switch, as before. Prune's grace period applies to the disk only.
- A copy refuses when it would delete more than 5% of the copied raw files, as the paper copy
  bounds its deletes. A change of bucket width replaces all of a source's raw files at once:
  `--full` then sends the store, and removing the old files from the bucket is a step by hand.
- With no copy record on the disk or in the bucket, a routine copy is skipped and asks for
  `--full`, which uploads the current state and deletes nothing. A file in the bucket that no
  record names, such as one from before the first `--full`, stays until removed by hand.
