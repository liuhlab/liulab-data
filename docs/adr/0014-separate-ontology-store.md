---
search:
  exclude: true
---

# Keep ontologies in their own store, updated by one process

The vocabularies that study readings name ids from live in an ontology store beside the GEO/SRA
store and the paper store, with its own root, lock, status record and GCS copy, built on the
code the other two share. An update is one process under the store's lock, not a Snakemake DAG,
and it downloads only the vocabularies with a new release.

## Considered options

- **Vocabularies as more snapshots in the GEO/SRA store.** The copy, restore, prune and subset
  would come free. But each vocabulary has its own release schedule, a new release would wait
  for a refresh or a build by hand, and every change to ontology code would end in a full
  GEO/SRA build.
- **Dated folders fetched on first use, as liulab-genome keeps its releases.** Light, but GCS
  would hold no copy and nothing would name a current snapshot.
- **A separate ontology store.** A vocabulary updates on its own release, ontology code never
  touches the GEO/SRA build, and more vocabularies add no work to the other stores.

## Consequences

- The update departs from the rule that pipelines are Snakemake DAGs, as the paper job does. Its
  units are a dozen small vocabularies, each done once its snapshot file exists, so a DAG would
  add a driver and per-step commands for no scheduling gain.
- A full copy of the lab's data is three stores.
