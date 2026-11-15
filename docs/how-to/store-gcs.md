# Copy the store to GCS

The store keeps one current state on the cluster and a full copy in a Google Cloud Storage
(GCS) bucket. Any full copy is the store, so work can go on from the bucket if the cluster
copy is lost. The tool that moves files is `rclone`, which comes with the pixi environment.

## Set up once

Make an rclone remote of type Google Cloud Storage, here called `gcs`:

```bash
pixi run rclone config
```

Then name the place in the bucket in the store's `labdata-store.toml`:

```toml
copy_to = "gcs:<bucket>/<store>"
```

## Log in each day

Your Google login for rclone lasts one day. Renew it before a copy:

```bash
pixi run rclone config reconnect gcs:
```

A cluster node has no web browser. Answer `n` when rclone asks to use one. It then prints an
`rclone authorize ...` command. Run that command on a computer with a browser, log in, and
paste the token it prints back into the prompt on the node.

## Prune and copy

```bash
labdata store prune
labdata store copy
```

`prune` deletes builds and snapshots the current build no longer reads. It keeps each one for
a grace period after it goes out of use, so a reader can finish; the
[API reference](../reference.md#labdata.store.prune) gives its length. `--grace 0` deletes
them at once: use it only as a one-off to tidy a store nobody is reading.

`copy` sends what changed since the last copy. The bucket keeps the store for good, and the
cluster disk is only a work area, so a copy never deletes a file because the disk lacks it.

- It sends the current build, the raw files and snapshots that build read, and the store's
  `README.md`. The marker file goes after them.
- It then deletes in the bucket, by name, the files the last copy sent that the current build
  no longer reads: the old build, a snapshot only that build read, a raw file the pack step
  dropped.
- It leaves out `work/`, `support/` and `refresh-status.json`. It will not run unless the
  current build is complete.

To know what changed, the copy keeps a hidden list of the files the bucket holds,
`.copied-files.json`, in the store and in the bucket. When neither has the list, the copy makes
it from the build that the bucket's marker file names. It does so only if that build is still on
the disk and the last copy had worked. Otherwise, as for a store's first copy,
`labdata store copy` stops and asks you to run this once:

```bash
labdata store copy --full
```

It sends the whole current state and deletes nothing. It lists every file on both sides, so
keep it for a first copy or a repair. Later copies send only the changes.

A copy refuses, sending nothing, when it would delete more than a small share of the raw files:
the build's manifest may be damaged. The message says why, with the numbers. If the files left
on purpose, run `labdata store copy --full`: it sends the store and leaves those files in the
bucket.

A copy first checks the login with one listing call. When the login has expired, it stops and
exits cleanly:

```text
rclone's Google login has expired, so the bucket is behind the store. Run `rclone config reconnect gcs:`, then `labdata store copy`.
```

Log in, then run `labdata store copy` again. Files already sent are skipped. A refresh that
meets an expired login still succeeds, and marks the copy pending in `refresh-status.json`
until `labdata store copy` succeeds.

## Check the copy

Compare the store with its `copy_to`, leaving out what the copy leaves out:

```bash
pixi run rclone check "$LABDATA_STORE" <copy_to> \
  --filter "- .*" --filter "- .*/**" --filter "- /work/**" \
  --filter "- /support/**" --filter "- /refresh-status.json" \
  --filter "- /builds/<old build>/**"
```

It should report 0 differences. A build or snapshot that prune keeps for its grace period is
not in the bucket: add a filter for each one instead of counting it as a difference. A file
found only in the bucket is one a copy should have deleted. The check lists every file on both
sides, so run it only to check the copy.

## Read the store on another computer

Copy the store down, check it against the bucket, and point labdata at it:

```bash
rclone copy gcs:<bucket>/<store> /data/store
rclone check gcs:<bucket>/<store> /data/store
export LABDATA_STORE=/data/store
```

To only read the store, copy just the marker and the build it names:

```bash
rclone copy gcs:<bucket>/<store> /data/store \
  --include "/labdata-store.toml" --include "/builds/<build>/**"
```
