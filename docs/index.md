# liulab-data

Look up a GEO study, inspect its sequencing runs, and download the raw FASTQ — in a
few lines of Python. `liulab-data` (import name `labdata`) talks to NCBI for you and
keeps every object lazy, so nothing hits the network until you actually ask for
something.

New here? Jump to the [GEO Series tutorial](tutorials/geo-series.md).

## Installation

```bash
pip install liulab-data
```

Downloading FASTQ also needs two external tools, [sra-tools](https://github.com/ncbi/sra-tools)
and [pigz](https://zlib.net/pigz/). Install them from Bioconda/conda-forge:

```bash
conda install -c bioconda -c conda-forge sra-tools pigz
```

!!! tip "Working inside the lab's pixi project?"
    Both tools are already in the project environment — `pixi install` gives you the
    Python package *and* `sra-tools`/`pigz` in one step.

## Configuration

NCBI asks every caller to identify themselves with a contact email. Set it once and
you're ready:

```bash
export NCBI_EMAIL="you@lab.org"     # required by NCBI
export NCBI_API_KEY="..."           # optional — raises your rate limit
```

!!! tip
    Prefer not to use environment variables? Run `labdata ncbi configure` to save
    your email to `~/.cache/liulab-data/` once and forget about it.

## Next steps

- **[GEO Series tutorial](tutorials/geo-series.md)** — from an accession to downloaded
  FASTQ, step by step.
- **[API reference](reference.md)** — every class and method in full detail.
