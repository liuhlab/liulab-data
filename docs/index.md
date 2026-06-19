# liulab-data

Data curation, download, and organization utilities for the Liu Lab.

!!! note "Documentation in progress"
    This site is a scaffold. Narrative documentation is being written alongside the
    GEO feature set; for now see the [API reference](reference.md) and the project
    `README.md`.

## At a glance

```python
from labdata import Series

gse = Series("GSE131907")     # cheap — no network until you ask for something
gse.pubmed_id                 # linked publication (PubMed ID)
gse.samples                   # [Sample('GSM...'), ...]   — lazy Sample instances
gse.platforms                 # [Platform('GPL...'), ...]
gse.experiments               # [Experiment('SRX...'), ...]

gse.samples[0].platform       # Platform(...) for that sample
gse.experiments[0].runs       # [Run('SRR...'), ...]
```

The GEO/SRA object model — `Series`, `Sample`, `Platform`, `Experiment`, `Run`,
`BioProject` — is
lazy throughout: links between objects return instances that only hit NCBI when you
read one of their fields.
