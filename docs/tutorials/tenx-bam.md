# Recovering FASTQ from a 10x cellranger BAM

Some 10x Genomics studies upload a cellranger BAM as their raw data, and SRA's
normalized `.sra` for those runs is unusable — often it kept only one read of the pair,
so `fasterq-dump` can't reconstruct the barcodes. The fix is to go back to the
submitter's **original** BAM and turn *that* into FASTQ with 10x's own `bamtofastq`,
which reads the 10x tags to split the barcode (R1), cDNA (R2), and index (I1) reads
correctly — ready to remap with STARsolo.

This is a two-step flow: **download the original BAMs**, then **convert them**.

## 1. Download the original-format BAMs

Fetch the submitter's files for the affected experiments (see
[the GEO Series tutorial's note on original files](geo-series.md)):

```bash
labdata geo download GSE208154 --original-srx SRX16000000 -o ./data
```

They land under `./data/GSE208154/<SRX>/<SRR>/<name>.bam`.

## 2. Convert the BAMs to FASTQ

```bash
labdata tenx bamtofastq GSE208154 -o ./data --cores 16
```

`labdata tenx bamtofastq` **auto-detects** which runs carry a 10x BAM (from the SRA
Data Locator listing) and converts each one that's on disk, driving `bamtofastq` as a
Snakemake DAG. The result is laid out **like the download pipeline** — flat under the
experiment directory, one prefix per run — so a later STARsolo step parses one layout:

```
data/GSE208154/<SRX>/
  <SRR>_S1_L001_R1_001.fastq.gz   # barcode + UMI
  <SRR>_S1_L001_R2_001.fastq.gz   # cDNA
  <SRR>_S1_L001_I1_001.fastq.gz   # sample index
  …
```

The `R1`/`R2`/`I1` and lane markers are preserved (STARsolo needs them), while the
files sit flat beside the experiment's other runs and glob as `<SRX>/<SRR>*.fastq.gz`,
just like a normal download.

!!! note "Good to know"
    - **Conversion-only.** This step reads BAMs that are already on disk; run the
      original-format download first. Pointing both commands at the same `-o` directory
      is intended — they share the `<GSE>/<SRX>/<SRR>/` layout.
    - **Interruptible.** A finished run leaves a `.<run>.tenx.done` marker, so
      re-running skips what's already converted. Safe after a Ctrl-C or HPC walltime
      kill.
    - `--cores` bounds total CPU across runs; `--threads-per-run` sets each
      `bamtofastq`'s `--nthreads`. On an HPC allocation, pass `--cores $SLURM_CPUS_ON_NODE`.
    - Auto-detection uses one SDL lookup per run. Pass `--all-runs` to skip it and
      convert every run of the selected experiments (e.g. when you already know they're
      all 10x), or `--select-srx` to narrow which experiments are considered.
    - On an **offline HPC compute node**, pass `--from-disk`: the run list is built by
      scanning the downloaded `<SRX>/<SRR>/*.bam` tree instead of querying NCBI, so no
      network or credentials are needed.
    - Pass `--remove-bam` to delete each run's source BAM once its FASTQ are produced —
      it reclaims the large original, and only fires after a run converts successfully
      (an interrupted run keeps its BAM for the resumed rerun).
    - Needs `10x_bamtofastq` on your system (see [Installation](../index.md)); Snakemake
      comes with `liulab-data`.

From Python, `TenxConverter` is the equivalent entry point:

```python
from labdata import TenxConverter

TenxConverter("GSE208154", "./data").convert(cores=16)
```

See the [API reference](../reference.md) for the full signature.
