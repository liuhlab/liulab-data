# A labdata ontology store: dated snapshots of the vocabularies readings name ids from

This folder is a labdata ontology store. It keeps each vocabulary, such as UBERON, Cellosaurus
or NCBI Taxonomy, as snapshots, one per release. Searches and lookups read each vocabulary's
current snapshot, from the files here, with no download.

Google Cloud Storage (GCS) holds the ontology store, and its copy there is the one source of
truth. Each update ends with `labdata ontology copy`, and `labdata ontology restore` fills an
empty disk from GCS. Paths in this file are relative to the store root, written `<ontologies>`.

## Contents

```text
<ontologies>/
  README.md                     this file
  labdata-ontologies.toml       the marker: where it is copied
  ontology-status.json          the last update, and each vocabulary's result
  .copied-files.json            the files GCS holds, as the last copy left them
  current-snapshots.json        each vocabulary's current snapshot
  snapshots/<vocabulary>-<release>/
    terms.sqlite                the snapshot's term tables
    source/                     the source files, as downloaded
  work/                         the update's lock and downloads in progress
```

`work/` is scratch. Delete it only while no update runs.

Each vocabulary keeps its current snapshot and the one before it, so a run that opened the
store before an update still reads its files. Each update deletes older snapshots.

## labdata-ontologies.toml

- **What:** the marker that makes this folder an ontology store.
- **Layout:** TOML. `format_version`; `copy_to`, where the store is copied with rclone.
- **Made by:** `labdata ontology init`.

## ontology-status.json

- **What:** the last update: its `start`, `end` and `outcome`, and one row per vocabulary.
- **Layout:** JSON. Each row in `vocabularies` gives the `release` checked, the `result`
  (`updated`, `unchanged` or `failed`, with its `reason`), the current `snapshot`, its own-term
  count `terms`, and its source file's date `source_date`. A failed vocabulary keeps its
  snapshot.
- **Made by:** each update, which rewrites it whole.

## current-snapshots.json

- **What:** each vocabulary's current snapshot. A caller that opens the store reads it once and
  keeps those snapshots until it closes the store.
- **Layout:** JSON, vocabulary name to snapshot name, such as `"uberon": "uberon-2026-11-05"`.
- **Made by:** each update, which rewrites it whole when it switches to new snapshots.

## terms.sqlite

One SQLite file per snapshot. Query it with `sqlite3`, or with
`labdata.ontology.open_ontology_store()`.

| Table | One row per | Columns |
| --- | --- | --- |
| `terms` | term of the vocabulary's own | `id`, `name`, `obsolete` |
| `texts` | name or synonym of a term | `id`, `text`, `kind`, `scope`, `synonym_type`, `folded` |
| `texts_words` | text of a live term, by `texts` row | the search's word index |
| `retired` | retired id and the live id it points to | `old_id`, `live_id`, `reason` |
| `meta` | fact about the snapshot | `key`, `value` |

- `kind` is `name` or `synonym`. A synonym's `scope` is `EXACT`, `NARROW`, `BROAD` or
  `RELATED`, and `synonym_type` is the vocabulary's own type for it, if any. `folded` is the
  text as a search matches it: lower case, with spaces collapsed.
- `texts_words` is a SQLite full-text index (FTS5). It holds no text of its own; its row ids
  are `texts` row ids.
- `reason` is `obsolete`, `merged`, `alternative` (kept as another term's alternative id) or
  `deleted`. `live_id` is empty when the id points to no live term.
- `meta` names the vocabulary, its release, the snapshot, the id prefixes of its own terms, and
  the layout of these tables.
