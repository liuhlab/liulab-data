---
search:
  exclude: true
---

# Write record tables small and dense, every other table in large row groups

Every build table is written to one declaration, whatever memory the build has. Record tables
use zstd level 9 and 2,048 rows per row group. Every other table uses zstd level 3 and
122,880 rows per row group. Dates are typed `DATE`, and `TIMESTAMP` for a supplementary file's
date. A record's raw text stays in its record file. No column uses `ENUM`.

The measurements behind each choice are in the
[layout proposal from trial writes](https://github.com/liuhlab/liulab-data/issues/82#issuecomment-6245000659),
against the
[baseline benchmark](https://github.com/liuhlab/liulab-data/issues/82#issuecomment-6243988097).

## Why

- **Small record row groups.** A lookup reads every row group that holds one of its keys.
  Small groups make a batch of scattered accessions read and hold far less. The cost is a
  slightly larger file and a slower write.
- **zstd 9 for records.** Records are most of the build, so their codec sets its size. Level 9
  gave the smallest files, with the same query times as levels 1 and 3. It costs write time;
  level 1 is the fallback if build time comes to matter more than size.
- **zstd 3 and large groups elsewhere.** The link, holdings, known and other tables are read
  by range or whole. Smaller groups made them slower or larger, and gained nothing.
- **Typed dates.** Readers filter and sort by date without a cast. This changed the column
  types, so it is a breaking change for readers that compared text.
- **Raw text in the record file.** Lookups already skip the raw text column. A separate file
  repeated the accession and hash, grew the build and was no faster.
- **No `ENUM`.** Parquet already dictionary-encodes the few-valued columns, so `ENUM` saved
  nothing.

## Consequences

- Parquet records each column's codec but not its level, so a layout check confirms the codec
  and the row groups, not the level.
- A lookup of very many scattered accessions still reads nearly every record row group. No row
  group size fixes that.
