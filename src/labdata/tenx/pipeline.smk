# cellranger BAM -> FASTQ DAG for labdata: one bamtofastq job per run.
#
# Driven by labdata.tenx.pipeline.run, which writes the --configfile and invokes
# snakemake with `--cores <M>` (bounds total CPU across the local, CPU-bound
# bamtofastq jobs). Unlike the download DAG this stage is purely local — there is no
# network step, so no `ncbi` resource is declared.
#
# The single rule shells out to a hidden `labdata tenx _bamtofastq` subcommand so the
# per-stage logic (locating the on-disk BAM, running the tool, flattening its nested
# output into the download-style layout) stays in Python behind the mockable
# bamtofastq seam.
#
# The rule's output is the per-run completion marker, so Snakemake owns cleanup and
# resume: a run whose `.{srr}.tenx.done` already exists is skipped whole. The
# converted FASTQ (<srx>/<srr>_S1_L00N_R{1,2}_00N.fastq.gz) are produced as a side
# effect and left in place beside the marker.

LABDATA = config["labdata"]  # e.g. "/path/to/python -m labdata"
RUNS = config["runs"]  # {srr: srx}
REMOVE_BAM = "--remove-bam" if config.get("remove_bam") else ""

DONE = "{srx}/.{srr}.tenx.done"  # completion marker


wildcard_constraints:
    srx=r"[SED]RX[0-9]+",
    srr=r"[SED]RR[0-9]+",


rule all:
    input:
        [DONE.format(srx=srx, srr=srr) for srr, srx in RUNS.items()],


rule bamtofastq:
    output:
        touch(DONE),
    params:
        reads_per_fastq=config["reads_per_fastq"],
    threads: config["threads_per_run"]
    shell:
        "{LABDATA} tenx _bamtofastq {wildcards.srr} {wildcards.srx}"
        " --threads {threads} --reads-per-fastq {params.reads_per_fastq} {REMOVE_BAM}"
