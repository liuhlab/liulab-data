---
search:
  exclude: true
---

# Keep papers in their own store

Paper files live in a paper store beside the GEO/SRA store, under its own root, with its own
config, job, status and GCS copy. The GEO/SRA builds keep the Series and BioProject links to
PMIDs; the paper job reads them from the current build. Neither store writes into the other, and
a Series' paper status is a join of the two on PMID.

## Considered options

- **Papers inside the GEO/SRA store.** One root and one copy. But that store is remade from
  public sources: a refresh rebuilds the build, and prune and removal drop what the sources no
  longer serve. Papers bought with OpenAlex quota or saved by hand in Paperpile cannot be fetched
  again for free, so every such step would need a rule to leave them alone.
- **A separate paper store.** Rebuild, prune and removal code never reaches paper files. The
  daily paper job and the monthly refresh never write the same tree, so neither waits for the
  other.

## Consequences

- A full copy of the lab's data is both stores.
- A reader that wants a Series with its paper opens the build and the paper store.
- PMC sync runs in the paper job after a refresh switches builds. Its failure never fails a
  refresh.
