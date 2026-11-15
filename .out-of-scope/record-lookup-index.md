# Lookup index for record tables

A build keeps no index beside its record tables: no map from accession to row group, no
Parquet bloom filter and no key-value file. A lookup reads each row group whose accession
range could hold one of its keys.

## Why this is out of scope

Each record table is sorted by accession and written in small row groups (ADR 0003), so a
lookup already skips every row group whose range holds none of its keys. An index would cut
reads further only when a batch has fewer keys than the table has row groups. A small batch
is already fast. In a large batch nearly every row group holds a key, so an index saves
nothing: the time goes to decompressing rows and building records.

An index is also a second file per table. Every build would have to write it, keep it in step
with its table and check it, and `layout.py`, the only writer of build tables, would declare
it.

The link table needs no index for lookups by target: the build also writes it sorted by
target, as `links_by_to.parquet`.

## When to reopen

Reopen when a consumer names a batch size and a time it needs, and
`scripts/store/benchmark.py` on a full build shows the store misses that time. The option to
try first is a map from accession to row group, read with pyarrow's `read_row_groups`. Count
the row groups each lookup reads against the ones that hold a key.

## Prior requests

- #115: "Faster scattered-accession lookups in a build"
