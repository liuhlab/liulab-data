# Copy the store to GCS

Google Cloud Storage (GCS) holds the store. A refresh or a build runs on a work disk on the
cluster, and a copy then sends the new state to GCS. An update is done, or finalized, only
once GCS holds it. If the work disk is lost or falls behind, a restore fills it from GCS. The
tool that moves files is `rclone`, which comes with the pixi environment.

## Set up once

Make an rclone remote of type Google Cloud Storage, here called `gcs`:

```bash
pixi run rclone config
```

Then name the store's place in GCS in its `labdata-store.toml`:

```toml
copy_to = "gcs:<gcs-bucket>/<store>"
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

`copy` sends what changed since the last copy. GCS keeps the store for good, and the work
disk is only where updates run, so a copy never deletes a file because the disk lacks it.

- It sends the current build, the raw files and snapshots that build read, and the store's
  `README.md`. The marker file goes after them.
- It then deletes in GCS, by name, the files the last copy sent that the current build
  no longer reads: the old build, a snapshot only that build read, a raw file the pack step
  dropped.
- It sends `refresh-status.json` last, after its copy record, marked `finalized`. While an
  update has not switched to its new build, the copy leaves its status record on the disk,
  so GCS keeps the one before.
- It leaves out `work/` and `support/`. It will not run unless the current build is complete.

To know what changed, the copy keeps a hidden list of the files GCS holds, its copy record
`.copied-files.json`, on the work disk and in GCS. When neither has the list, the copy makes
it from the build that the marker file in GCS names. It does so only if that build is still on
the disk, the last copy had worked, and a listing finds that build's files in GCS with the
disk's sizes; it downloads nothing for that check. Otherwise, as for a store's first copy,
`labdata store copy` stops and asks you to run this once:

```bash
labdata store copy --full
```

It sends the whole current state and deletes nothing. It lists every file on both sides, so
keep it for a first copy or a repair. Later copies send only the changes.

A copy may always delete one raw file. It refuses, sending nothing, when it would delete more
than that and more than a small share of the raw files: the build's manifest may be damaged.
The message says why, with the numbers. If the files left on purpose, run the copy again with
the count the message gives:

```bash
labdata store copy --accept-deletes <count>
```

A copy first checks the login with one listing call. When the login has expired, it stops,
sends nothing and exits with an error:

```text
rclone's Google login has expired, so GCS is behind the store. Run `rclone config reconnect gcs:`, then `labdata store copy`.
```

Log in, then run `labdata store copy` again. Files already sent are skipped. A refresh that
meets an expired login ends `built`, not `finalized`: the work disk uses the new build, and
`refresh-status.json` marks the copy pending until a copy works.

## When the copy refuses because the disk is behind

After the login check, a copy checks that the work disk's copy record is the one in GCS. It
compares the MD5 hash GCS keeps for that file with the disk's, and downloads nothing. Every
update makes the same check when it starts: a refresh, a full run, a build by hand and the SRA
removal. If the two differ, the disk is behind GCS, or another disk has sent to GCS since. A
disk with no copy record while GCS holds one was never restored, and refuses the same way.
The copy then sends nothing, marks the copy pending, and names the command that fixes it:

```text
the work disk's copy record, .copied-files.json, is not the one in GCS at gcs:<gcs-bucket>/<store>, so nothing was sent: this disk is behind GCS, or another disk sent to it since. Run `labdata store restore --from gcs:<gcs-bucket>/<store>` to bring the work disk in step with GCS.
```

Run the restore, then the copy or the update again. When GCS holds no copy record at all, but
the disk does, GCS is new or was emptied: the copy asks for `labdata store copy --full`.

## Check the copy

The routine check reads the status record and the copy record, and lists no store folder. The
last update is finalized when both of these hold:

- In `refresh-status.json`, `outcome` is `finalized` and `copy_pending` is `false`. If
  `copy_pending` is `true`, log in and run `labdata store copy`.
- The copy record in GCS matches the one on the disk. Each copy sends it last, after
  the files it lists:

  ```bash
  pixi run rclone check "$LABDATA_STORE" <copy_to> --max-depth 1 \
    --include "/.copied-files.json"
  ```

  It should report 0 differences and 1 matching file. It lists only the top level of each
  side.

A full check finds what differs when the copy record does not match, or after a change made
in GCS by hand. It reads every file on both sides, so run it only to repair the copy,
and only when the maintainer says yes. It leaves out what the copy leaves out:

```bash
pixi run rclone check "$LABDATA_STORE" <copy_to> \
  --filter "- .*" --filter "- .*/**" --filter "- /work/**" \
  --filter "- /support/**" \
  --filter "- /builds/<old build>/**"
```

It should report 0 differences. A build or snapshot that prune keeps for its grace period is
not in GCS: add a filter for each one instead of counting it as a difference. A file found
only in GCS is one a copy should have deleted. If it finds files missing from GCS,
`labdata store copy --full` sends them. It deletes nothing.

## Restore the store from GCS

Restore the work disk when it is new or lost, or when a copy or an update refuses because
the disk is behind GCS. Restore before the next update:

```bash
labdata store restore --root /path/to/store --from gcs:<gcs-bucket>/<store>
```

On a disk that already has the store's marker, `--from` defaults to its `copy_to`.

- It reads the copy record in GCS and downloads what the disk lacks or holds differently.
  An empty disk gets the whole current state; a disk that fell behind gets only the
  difference. A file already on the disk with the size and hash GCS gives is not
  downloaded again.
- It deletes the files the disk's copy record names and the one in GCS does not, such as
  an old build. On a disk with no copy record, it deletes every file in `builds/`, `raw/`
  and `snapshots/` that GCS's copy record does not name.
- It leaves `work/`, `support/` and `extracts/` alone. A build makes a missing extract
  from its snapshot.
- It saves the copy record and `refresh-status.json` last, as GCS holds them. The status
  record then says `finalized`, so the next refresh knows when the last one ran.
- It holds the store's run lock, so it refuses while an update runs. If it says it cannot
  check that update and you know it has ended, add `--owner-gone`.

It prints how much it downloads, and asks before more than 1 GB. Downloads out of GCS cost
money, so a large restore needs the maintainer's yes. A batch job has no terminal to ask
in: pass `--accept-download` to let it download more than 1 GB. If it stops, run it again: it
skips the files it already has.

## Read the store on another computer

To only read the store, copy just the marker and the build it names, and point labdata at
it:

```bash
rclone copy gcs:<gcs-bucket>/<store> /data/store \
  --include "/labdata-store.toml" --include "/builds/<build>/**"
```
