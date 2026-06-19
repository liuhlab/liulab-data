# Understanding GEO & SRA

New to NCBI's sequencing databases? This page gives you just enough of the mental
model to use `labdata` confidently. For the hands-on walkthrough, see the
[GEO Series tutorial](geo-series.md).

## Two databases, one study

A sequencing study usually lives in **two linked NCBI databases**:

- **GEO** — the *processed, expression-focused* side (count matrices, the study's
  description, sample list).
- **SRA** — the *raw reads* side (the actual FASTQ files you download).

GEO records point into SRA, so you can start from a GEO accession and follow the
links all the way down to raw data.

## The hierarchy: study → sample → experiment → run

Everything is organized as a tree, from a big project down to individual FASTQ files.
Each level has its own ID prefix — and its own class in `labdata`:

| Level | Example ID | What it is | `labdata` class |
| --- | --- | --- | --- |
| BioProject | `PRJNA208503` | the whole study / "the paper" | `BioProject` |
| GEO Series | `GSE47966` | the study, as seen in GEO | `Series` |
| GEO Platform | `GPL13112` | the sequencer used | `Platform` |
| GEO Sample | `GSM1173819` | one biological sample | `Sample` |
| SRA Experiment | `SRX314994` | one library + sequencing design | `Experiment` |
| SRA Run | `SRR921999` | the raw FASTQ files | `Run` |

(IDs above are from the example study [GSE47966](https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE47966).)

A few things worth remembering:

- **It's one-to-many at every level**: one project has many samples, each sample can
  have several experiments, and each experiment can have several runs.
- **`SRR` (the run) is what you actually download** — FASTQ files live at the run
  level.
- You'll also see **`SAMN…` (BioSample)** — e.g. `SAMN02206500` — the SRA-side name for
  the biological material behind a `GSM`. `labdata` surfaces it as a `BioSample` field
  (e.g. a column in the run table) rather than a separate class.

## How `labdata` reaches this data

The package talks to NCBI for you through two channels:

- **Metadata → NCBI Entrez (E-utilities).** Every `Series`, `Sample`, `Experiment`,
  and `Run` fetches its details from Entrez *on demand* — building the object costs
  nothing; the network call happens the first time you read a field, and is cached
  afterward.
- **Raw FASTQ → [sra-tools](https://github.com/ncbi/sra-tools).** `download()` shells
  out to `prefetch` + `fasterq-dump` to pull and extract the reads for each run.

Because links return real objects, you can walk the tree in plain Python:

```python
from labdata import Series

gse = Series("GSE47966")         # GEO Series (GSE)

for experiment in gse.experiments:   # SRX level
    for run in experiment.runs:      # SRR level — the downloadable units
        print(experiment.accession, run.accession)
```

That's the whole idea: **start at a GEO Series, follow the links down to runs, then
download.** The [GEO Series tutorial](geo-series.md) shows it end to end.

## What's not in GEO

The structured records above (samples, platforms, experiments, runs) are only part of
a study. Two things commonly live *outside* that metadata — don't forget them:

- **Supplementary files.** The actual *ready-to-analyze* outputs — count matrices,
  peak calls, bigWig tracks, READMEs — are usually attached to the Series as
  supplementary files, not encoded in the metadata. They're often the most useful
  part. List and fetch them directly:

    ```python
    gse.supplementary_files       # ['GSE47966_RAW.tar', 'GSE47966_README.txt', 'filelist.txt']
    gse.supplementary_file_urls   # full https URLs you can download
    ```

- **Whatever the paper kept elsewhere.** GEO doesn't capture everything. Always read
  the paper's **"Data Availability"** and **"Code Availability"** statements: raw or
  controlled-access data may sit in another repository (dbGaP, EGA, Zenodo, …), and the
  analysis code typically lives on GitHub — none of which a GEO record will point you
  to.
