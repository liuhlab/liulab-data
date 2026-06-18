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
gse.samples                   # ['GSM...', ...]
gse.experiments               # ['SRX...', ...]
```
