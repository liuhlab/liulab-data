# FASTQ-download DAG for labdata: prefetch -> extract -> compress, one chain per run.
#
# Driven by labdata.geo.pipeline.run, which writes the --configfile and invokes
# snakemake with `--resources ncbi=<N>` (caps concurrent prefetch, the NCBI network
# step) and `--cores <M>` (bounds total CPU across the local extract/compress steps).
# Each rule shells out to a hidden `labdata geo _<stage>` subcommand so the per-stage
# logic (tool calls) stays in Python behind the mockable sra-tools seam.
#
# The DATA files are the rule outputs, so Snakemake owns cleanup and resume:
#   prefetch -> temp({srx}/{srr})       the .sra dir     (deleted after extract)
#   extract  -> temp({srx}/.{srr}.fq)   FASTQ staging    (deleted after compress)
#   compress -> {srx}/.{srr}.done       completion marker (kept; dynamic .gz names)
# temp() reclaims each intermediate as soon as the next step consumes it, keeping
# peak disk low; the final <srx>/<srr>_*.fastq.gz land in {srx}/.

LABDATA = config["labdata"]  # e.g. "/path/to/python -m labdata"
RUNS = config["runs"]  # {srr: srx} — default sra-tools runs
ORIGINAL_RUNS = config.get("original_runs", {})  # {srr: srx} — original-format runs
KEEP_SRA = "--keep-sra" if config["keep_sra"] else ""
PREFETCH_ONLY = config["prefetch_only"]

SRA_DIR = "{srx}/{srr}"  # prefetch -O {srx} writes {srx}/{srr}/{srr}.sra
STAGING = "{srx}/.{srr}.fq"  # uncompressed FASTQ staging
DONE = "{srx}/.{srr}.done"  # completion marker
ORIGINAL_DONE = "{srx}/.{srr}.original.done"  # original-format completion marker


wildcard_constraints:
    srx=r"[SED]RX[0-9]+",
    srr=r"[SED]RR[0-9]+",


def _targets():
    """Final targets: per-run .done (or .sra under prefetch_only) plus .original.done.

    Default runs target their ``.sra`` dir under ``prefetch_only`` else the
    ``.done`` marker; original-format runs always target ``.original.done`` (their
    download is a single terminal step, unaffected by ``prefetch_only``).
    """
    template = SRA_DIR if PREFETCH_ONLY else DONE
    sra = [template.format(srx=srx, srr=srr) for srr, srx in RUNS.items()]
    original = [ORIGINAL_DONE.format(srx=srx, srr=srr) for srr, srx in ORIGINAL_RUNS.items()]
    return sra + original


rule all:
    input:
        _targets(),


rule prefetch:
    output:
        temp(directory(SRA_DIR)),
    params:
        max_size=config["max_size"],
        retries=config["retries"],
        backoff=config["backoff"],
    resources:
        ncbi=1,
    shell:
        "{LABDATA} geo _prefetch {wildcards.srr} {wildcards.srx}"
        " --max-size {params.max_size} --retries {params.retries} --backoff {params.backoff}"


# extract/compress carry higher priority than prefetch so the scheduler drains
# already-fetched runs (freeing their .sra) before fetching more — this bounds how
# far prefetch races ahead and thus the number of .sra staged on disk at once.
rule extract:
    input:
        SRA_DIR,
    output:
        temp(directory(STAGING)),
    params:
        keep=KEEP_SRA,
    threads: config["threads_per_run"]
    priority: 10
    shell:
        "{LABDATA} geo _extract {wildcards.srr} {wildcards.srx}"
        " --threads {threads} --staging {output} {params.keep}"


rule compress:
    input:
        STAGING,
    output:
        touch(DONE),
    threads: config["threads_per_run"]
    priority: 20
    shell:
        "{LABDATA} geo _compress {wildcards.srr} {wildcards.srx}"
        " --threads {threads} --staging {input}"


# Original-format runs bypass the prefetch/extract/compress chain entirely: a single
# download-only step fetches the submitter's files. It is a network job, so it shares
# the ``ncbi`` resource budget with prefetch; there is no extract/compress to follow.
rule original:
    output:
        touch(ORIGINAL_DONE),
    params:
        retries=config["retries"],
        backoff=config["backoff"],
    resources:
        ncbi=1,
    shell:
        "{LABDATA} geo _original {wildcards.srr} {wildcards.srx}"
        " --retries {params.retries} --backoff {params.backoff}"
