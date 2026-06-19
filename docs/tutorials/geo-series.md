# GEO Series: from study to FASTQ

This tutorial goes from a GEO accession to downloaded FASTQ in three short steps.
First, make sure you've [installed the package and set your NCBI email](../index.md).

## 1. Look up a GEO Series

Start from any GEO accession (`GSE…`). Building the handle is instant; reading a
field is what triggers the lookup.

```python
from labdata import Series

gse = Series("GSE131907")     # just a handle — no network yet

gse.title         # 'Single-cell landscape of lung adenocarcinoma'
gse.organism      # 'Homo sapiens'
gse.pubmed_id     # '32385277'
gse.samples       # [Sample('GSM3827114'), Sample('GSM3827115'), ...]
gse.experiments   # [Experiment('SRX5921017'), ...]
gse.url           # 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE131907'
```

Each linked item (a sample, an experiment, a run) is its own object you can drill
into the same way. See the [API reference](../reference.md) for everything they
expose.

## 2. Get the SRA run table

Want the table you'd normally click through on NCBI's **SRA Run Selector** — one row
per sequencing run, with library and instrument details? Ask for it directly. It
comes back as a [pandas](https://pandas.pydata.org/) `DataFrame`, so you can filter,
sort, and export it however you like.

```python
table = gse.make_sra_run_table()

table[["Run", "Experiment", "Instrument", "Bases", "LibraryLayout"]].head()
```

```text
          Run   Experiment           Instrument        Bases LibraryLayout
0  SRR9000001   SRX5921017  Illumina HiSeq 2500   1200000000        PAIRED
1  SRR9000002   SRX5921017  Illumina HiSeq 2500    800000000        PAIRED
...
```

```python
table.to_csv("GSE131907_runs.csv", index=False)   # hand it off to collaborators
```

## 3. Download all FASTQ files

One call fetches the FASTQ for **every run in the study**, using
[sra-tools](https://github.com/ncbi/sra-tools) under the hood. Files are organized by
study → experiment → run and gzipped for you:

```python
gse.download("./fastq", n_parallel=4)   # 4 runs at a time
```

```text
{'SRR9000001': True, 'SRR9000002': True, ...}     # each run: success?
```

The files land in a tidy tree:

```text
fastq/
└── GSE131907/
    └── SRX5921017/
        ├── SRR9000001_1.fastq.gz
        ├── SRR9000001_2.fastq.gz
        ├── SRR9000002_1.fastq.gz
        └── SRR9000002_2.fastq.gz
```

!!! note "Good to know"
    - This downloads **real sequencing data**, which can be large — point
      `output_dir` somewhere with room to spare.
    - Each finished run drops a hidden `.<run>.success` marker, so re-running
      `download()` **skips what's already done** and resumes the rest.
    - Needs `sra-tools` and `pigz` on your system (see [Installation](../index.md)).

!!! info "Just one experiment?"
    `Series.download` does the whole study. To grab a single experiment instead, use
    `SraDownloader` — see the [API reference](../reference.md).

## Going deeper

This tutorial covers the common path. For the full picture — every property of
`Series`, `Sample`, `Platform`, `Experiment`, `Run`, and `BioProject`, plus the
downloader options — head to the **[API reference](../reference.md)**.
