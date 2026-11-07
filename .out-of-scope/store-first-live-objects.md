# Store-first live objects

labdata's live `Series`, `Sample`, `Platform`, `Experiment` and `Run` objects read NCBI
only. They do not look in the store first, and they do not fall back to it.

## Why this is out of scope

The live objects and the store mirror different sources. `Series`, `Sample` and `Platform`
read NCBI's GEO summary service, while the store keeps the parsed GEO SOFT text under its own
field names, such as `Series_pubmed_id` (ADR 0001). The store is also up to a month old. A
silent fallback would hand back older values in another shape, and one record set could mix
store and live data without the caller knowing.

Store-first reading is the consumer's choice, made all or nothing through the store API:

```python
from labdata.store import open_store

store = open_store()
experiments = store.experiments_for("GSE123456")  # same Experiment and Run types as live
records = store.raw(["GSE123456"])                # the stored SOFT text
```

A consumer that gets `NotMirrored` or `NotFound`, or has no store configured, goes fully
live instead.

If a consumer later needs store-built `Series`, `Sample` or `Platform` handles, that is a new
request for separate store objects, not a change to the live ones.

## Prior requests

- #109: "Should the store replace labdata's live Series, Sample and Run objects?"
