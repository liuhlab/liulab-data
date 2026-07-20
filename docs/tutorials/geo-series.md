# GEO Series: from study to FASTQ

This tutorial goes from a GEO accession to downloaded FASTQ in a few short steps,
using a real study — [GSE47966](https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE47966)
(*Global epigenomic reconfiguration during mammalian brain development*). First, make
sure you've [installed the package and set your NCBI email](../index.md).

## 1. Look up a GEO Series

Start from any GEO accession (`GSE…`). Building the handle is instant; reading a
field is what triggers the lookup.

```python
from labdata import Series

gse = Series("GSE47966")     # just a handle — no network yet

gse.title         # 'Global epigenomic reconfiguration during mammalian brain development'
gse.organism      # 'Homo sapiens; Mus musculus'
gse.platforms     # [Platform('GPL13112'), Platform('GPL11154')]
gse.samples       # [Sample('GSM1173819'), Sample('GSM1173773'), ...]   (65 samples)
gse.experiments   # [Experiment('SRX314994'), Experiment('SRX314993'), ...]   (65 experiments)
gse.url           # 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE47966'
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
table = gse.make_sra_run_table()      # one row per run (392 in this study)

table[["Run", "Instrument", "ReadStructure", "Bases", "LibraryLayout"]].head(3)
```

```text
         Run           Instrument ReadStructure        Bases LibraryLayout
0  SRR907554  Illumina HiSeq 2000            50  10100000000        SINGLE
1  SRR907555  Illumina HiSeq 2000            50  10100000000        SINGLE
2  SRR907556  Illumina HiSeq 2000            50  10100000000        SINGLE
...
```

`ReadStructure` gives the length of each read in a spot, joined with `+`: a
single-end 50&nbsp;bp run reads `50`, while a paired run with a 28&nbsp;bp barcode
and a 94&nbsp;bp cDNA read reads `28+94` — a quick way to spot single-cell
chemistries.

```python
table.to_csv("GSE47966_runs.csv", index=False)   # hand it off to collaborators
```

## 3. Download all FASTQ files

One call fetches the FASTQ for **every run in the study**, running a
[Snakemake](https://snakemake.github.io) pipeline (`prefetch → extract → compress`)
over [sra-tools](https://github.com/ncbi/sra-tools) under the hood. Files are organized
by study → experiment → run and gzipped for you:

```python
gse.download("./fastq", ncbi_parallel=3, cores=16)   # ≤3 downloads at once, 16 cores total
```

The two knobs are independent, which is the point: `ncbi_parallel` caps how many runs
`prefetch` from NCBI at once (keep it low to stay friendly with NCBI), while `cores`
bounds total CPU for the local extraction/gzip work. So downloads keep the network busy
while cores chew through already-fetched runs, instead of the two blocking each other.
On an HPC allocation, set `cores` to your allotted cores.

```text
{'SRR921999': True, 'SRR922000': True, 'SRR922001': True, ...}     # each run: success?
```

The files land in a tidy tree:

```text
fastq/
└── GSE47966/
    ├── SRX314994/
    │   ├── SRR921999.fastq.gz
    │   ├── SRR922000.fastq.gz
    │   └── ...
    └── SRX314993/
        └── ...
```

!!! note "Good to know"
    - This downloads **real sequencing data**, which can be large — point
      `output_dir` somewhere with room to spare. If free space looks tight, the
      download prints a rough warning before it starts (it never blocks).
    - GSE47966's runs are single-end, so each gives one `.fastq.gz`; paired-end runs
      produce `_1`/`_2` files instead.
    - **Interruptible.** A finished run leaves a `.<run>.done` marker, so re-running
      `download()` **skips what's already done** — an interrupted download resumes
      without re-fetching SRA data (and `prefetch` itself resumes a partial `.sra`).
      Safe after a Ctrl-C or an HPC walltime kill.
    - **Modest peak disk.** Snakemake reclaims each intermediate the moment the next
      step consumes it — a run's `.sra` the moment its FASTQ is extracted, the
      uncompressed FASTQ the moment they're gzipped — and drains
      extraction/compression before fetching more, so only a handful of `.sra` sit on
      disk at once. Lower `ncbi_parallel` to shrink that working set further.
    - Pass `keep_sra=True` to keep each run's `.sra` next to its FASTQ instead of
      reclaiming it after extraction (useful for re-extracting later), or
      `prefetch_only=True` to just stage the `.sra` files without extracting.
    - Needs `sra-tools` and `pigz` on your system (see [Installation](../index.md));
      Snakemake comes with `liulab-data`.

!!! tip "Missing or useless `.sra`? Download the original files"
    Occasionally a run's SRA-normalized `.sra` is gone, or wrong — e.g. a 10X run
    whose `.sra` kept only one read of a pair. The submitter's **original-format**
    files (what you see under "Data access" in the SRA Run Browser) are still there.
    Name those experiments with `original_srx` to fetch them as-is instead — download
    only, no extraction:

    ```python
    gse.download("./fastq", original_srx=["SRX34567890"])
    ```
    ```bash
    labdata geo download GSE310667 --original-srx SRX34567890
    ```

    The files land under `…/<SRX>/<SRR>/` with their original names and are md5-checked
    against NCBI; everything else in the study still takes the normal sra-tools path.
    This only sets the *mode* — pair it with `--select-srx`/`select_srx` (repeatable,
    or a whitelist file) if you also want to narrow *which* experiments run. Original
    format is heterogeneous, so labdata just downloads it and leaves the processing to
    you. Needs `curl` in addition to `sra-tools`/`pigz`. When the original file is a
    **10x cellranger BAM**, the [10x BAM → FASTQ tutorial](tenx-bam.md) turns it back
    into remap-ready FASTQ.

!!! info "Just one experiment?"
    `Series.download` does the whole study. To grab a single experiment instead, use
    `SraDownloader` — see the [API reference](../reference.md). Pass `original=True` for
    that experiment's original-format files.

!!! tip "Have a BioProject instead?"
    `BioProject` exposes the same `download()` — `BioProject("PRJNA1027859").download("./fastq")`
    lays the FASTQ out identically, under a `PRJNA…/` directory. The
    `labdata geo download` command auto-detects which you gave it, so `GSE…` and
    `PRJ…` accessions both just work.

## 4. Grab the supplementary files

Raw FASTQ isn't the whole story. Authors usually attach **supplementary files** —
processed matrices, peak calls, bigWigs, READMEs — to the Series. List them and grab
their download URLs directly:

```python
gse.supplementary_files       # ['GSE47966_RAW.tar', 'GSE47966_README.txt', 'filelist.txt']
gse.supplementary_file_urls   # full https URLs you can download
gse.supplementary_http_url    # the suppl/ directory itself
```

These are often where the *useful, ready-to-analyze* outputs live — see
[what's not in GEO](understanding-geo.md#whats-not-in-geo) for why they matter.

## Going deeper

This tutorial covers the common path. For the full picture — every property of
`Series`, `Sample`, `Platform`, `Experiment`, `Run`, and `BioProject`, plus the
downloader options — head to the **[API reference](../reference.md)**.
