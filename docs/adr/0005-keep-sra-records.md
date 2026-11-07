---
search:
  exclude: true
---

# Keep SRA records; remove only withdrawn Series

A refresh removes a GEO Series that GEO no longer lists, with its Series-keyed rows, after a fetch
of it alone confirms it is gone. It never removes an SRA experiment or run, and it does not drop
records no live Series reaches. liulab-data and its consumers reach SRA through Series, so an SRA
record no live Series names is harmless, while confirming and pruning SRA records cost about an
hour a refresh.

## Consequences

- The store keeps SRA records NCBI no longer serves. A direct lookup of one returns its old
  record instead of `NotFound`.
- A live Series whose Sample still names a suppressed experiment keeps listing it until GEO
  updates the Series.
