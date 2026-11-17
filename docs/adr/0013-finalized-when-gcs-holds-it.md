---
search:
  exclude: true
---

# An update is finalized when GCS holds it

This amends [ADR 0011](0011-gcs-holds-the-paper-store.md) and
[ADR 0012](0012-gcs-holds-the-store.md). GCS is the only source of truth for both stores, and a
work disk is where updates run. An update is finalized only once the copy record in GCS names
the state it made. This holds for a refresh, a full run, a manual build, an SRA removal and a
paper pass. The copy sends the status record last, after the copy record, so GCS never holds
one that is running or pending.

- The work disk switches to a new build before the copy, as before. The marker in GCS moves
  only with the copy, so GCS never names an unfinished build.
- A copy skipped on an expired login leaves the update built but not finalized, and the command
  exits non-zero. Run again after the login, it does only the copy.
- Before it sends anything, every copy compares the hash GCS keeps for its copy record with the
  work disk's record, and refuses on a mismatch. A restore, the copy run the other way, brings
  the disk in step.
- An extract holds nothing its snapshot lacks. GCS does not hold extracts, and a step that
  needs a missing one makes it again.

## Considered options

- **Finalize before the switch.** No reader on the work disk would see an update GCS lacks.
  But an expired login would hold back the month's build and the paper job's PMC sync, and
  the copy would need a second path for a build that is not current.
- **Download the copy record to compare it.** The hash GCS keeps answers the same question
  with no download.
- **Copy the extracts.** Every refresh remakes them from new snapshots, so GCS would store,
  and a restore would download, files nothing needs.

## Consequences

- For up to a day, readers on the work disk may open a build GCS does not hold. If the disk
  is lost then, the next refresh makes the build again.
- A work disk that missed a copy cannot finalize until a restore brings it in step.
- A sample build on a fresh disk first makes its extracts.
