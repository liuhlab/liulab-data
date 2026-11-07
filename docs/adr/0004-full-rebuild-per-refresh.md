---
search:
  exclude: true
---

# Rebuild the whole build on every refresh

A refresh writes a complete new build from `raw/` and the snapshots, as the full run does, and
switches to it. It does not split tables into accession-range chunks, and it does not fold
delta files into an older build.

## Considered options

- **Accession-range chunks, old chunks frozen.** A month's changes are scattered across the
  accession range: GEO edits old Series, and BioSample changes across the whole range. Nearly
  every chunk would be rebuilt, and lookups would read many files per table.
- **A base build plus delta files, compacted now and then** (as Delta Lake, Iceberg and
  DuckLake do). The delta is small, but finding it still needs every snapshot in full and a
  diff against the old one. It adds a second read path, deletion markers and compaction, and
  the build would no longer be remade from its inputs on every refresh.
- **Full rebuild.** Every build is whole, the switch is one rename, readers never see a half
  update, and every refresh proves the store rebuilds without the network. Its cost grows with
  the store.

## Consequences

- Refresh cost is the fetch of changed records, the snapshot downloads and one full build.
- If build time comes to matter, the next step is a cache of parsed rows keyed by each raw
  row's hash: a refresh re-parses only new or changed raw rows and still writes a whole build.
- Lookup speed is a separate question from this one. An accession index or Parquet bloom
  filters can speed lookups under any rebuild strategy.
