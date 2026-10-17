---
search:
  exclude: true
---

# Keep the newest copy of a GEO Sample

A GEO Sample appears in the family SOFT file of every Series that holds it, and GEO can serve
the sample differently in two of those files. One file can hold an older rendering. The build
keeps the copy with the newest `Sample_last_update_date`. On a tie, or when a copy has no date,
it keeps the lowest-numbered Series' copy. Every other record type keeps its lowest-numbered
copy. The build writes a `copies differ` note for every record whose copies differ, whichever
copy it keeps.

## Considered options

- **Always keep the lowest-numbered copy.** Simple and stable, but the lowest-numbered Series is
  often the older one, so its copy can be the stale one.
- **Pick a copy per field.** Most differences, such as the instrument model, mean the same
  thing to a reader. Field rules would add code for no gain downstream.

## Consequences

- A rebuild can show a different source text for a sample after GEO updates one of its Series.
- The kept copy's `Sample_series_id` can still miss a Series. The links table holds every
  membership, so only a reader of that one field sees the gap.
