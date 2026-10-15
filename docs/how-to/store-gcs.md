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
24 hours after it goes out of use, so a reader can finish.

`copy` sends the marker file, the current snapshots, `raw/` and the current build, and deletes
anything else in the bucket's copy. It will not run unless the current build is complete. When
the login has expired, it stops and exits cleanly:

```text
rclone's Google login has expired, so the bucket is behind the store. Run `rclone config reconnect gcs:`, then `labdata store copy`.
```

Log in, then run `labdata store copy` again. Files already sent are skipped.

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
