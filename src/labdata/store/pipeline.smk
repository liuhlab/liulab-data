# One round of the full run: target list -> GEO lane || E-utilities lane -> pack -> build
# -> coverage -> prune -> copy, with the snapshot extracts beside the fetch.
#
# Driven by labdata.store.pipeline.run, which runs snakemake in the round's folder with
# `--resources ncbi=<N> mem_mb=<job memory>` and `--cores <M>`. Each rule's mem_mb comes from
# the config's memory plan; the step gets it and its threads as LABDATA_MEMORY_MB and
# LABDATA_THREADS, and caps DuckDB and Ray within them. Each rule calls a
# `labdata store` subcommand; its marker appears only when that command exits 0, so a rerun
# starts at the first unfinished step. Each step's output also goes to logs/<step>.log.
#
# The E-utilities lane runs twice: once beside the GEO lane, and again after it, because
# the experiment list grows with the SOFT the GEO lane saved. The second run skips every
# experiment the first saved.
#
# A lane that ends with items still failed leaves <lane>.failed beside its marker, and the
# next `labdata store run` deletes that marker, so the lane asks for those items again and
# every later step reruns. The round still builds, with the failures as gaps and notes.

import shlex

LABDATA = config["labdata"]
STORE = f"LABDATA_MEMORY_MB={{resources.mem_mb}} LABDATA_THREADS={{threads}} {LABDATA} store"
MEM = config["mem_mb"]
ROOT = "--root " + shlex.quote(config["root"])
FRACTION = f"--fraction {config['fraction']}" if config["fraction"] else ""
GEO_DATE = config["snapshots"]["geo-series"]
SRA_DATE = config["snapshots"]["sra-accessions"]
TARGETS = config["targets"]
EXTRACTS = list(config["snapshots"])
LOG = "2>&1 | tee -a {log}"


rule all:
    input:
        "copy.done",
        "coverage.done",


rule targets:
    output:
        TARGETS,
    log:
        "logs/targets.log",
    resources:
        mem_mb=MEM["targets"],
    shell:
        f"{STORE} _targets {FRACTION} --snapshot-date {GEO_DATE} {ROOT} {LOG}"


rule geo:
    input:
        TARGETS,
    output:
        touch("geo.done"),
    log:
        "logs/geo.log",
    resources:
        ncbi=1,
        mem_mb=MEM["geo"],
    shell:
        f"{STORE} _geo --series {{input}} --failed-marker geo.failed {ROOT} {LOG}"


rule eutils_beside_geo:
    input:
        TARGETS,
    output:
        touch("eutils-beside-geo.done"),
    log:
        "logs/eutils-beside-geo.log",
    resources:
        ncbi=1,
        mem_mb=MEM["eutils"],
    shell:
        f"{STORE} _eutils --series {{input}} --date {SRA_DATE} {ROOT} {LOG}"


rule eutils_after_geo:
    input:
        TARGETS,
        "geo.done",
        "eutils-beside-geo.done",
    output:
        touch("eutils.done"),
    log:
        "logs/eutils-after-geo.log",
    resources:
        ncbi=1,
        mem_mb=MEM["eutils"],
    shell:
        f"{STORE} _eutils --series {TARGETS} --date {SRA_DATE} --failed-marker eutils.failed "
        f"{ROOT} {LOG}"


rule pack:
    input:
        "geo.done",
        "eutils.done",
    output:
        touch("pack.done"),
    log:
        "logs/pack.log",
    resources:
        mem_mb=MEM["pack"],
    shell:
        f"{STORE} pack {ROOT} {LOG}"


# Leaves two cores to the fetch lanes, which may run at the same time.
rule extract:
    output:
        touch("extract-{source}.done"),
    log:
        "logs/extract-{source}.log",
    resources:
        mem_mb=MEM["extract"],
    threads: max(1, workflow.cores - 2)
    shell:
        f"{STORE} extract --source {{wildcards.source}} --workers {{threads}} {ROOT} {LOG}"


rule build:
    input:
        "pack.done",
        expand("extract-{source}.done", source=EXTRACTS),
    output:
        touch("build.done"),
    log:
        "logs/build.log",
    resources:
        mem_mb=MEM["build"],
    threads: workflow.cores
    shell:
        f"{STORE} build --workers {{threads}} {FRACTION} {ROOT} {LOG}"


rule coverage:
    input:
        "build.done",
    output:
        touch("coverage.done"),
    log:
        "logs/coverage.log",
    resources:
        mem_mb=MEM["coverage"],
    shell:
        f"{STORE} coverage {ROOT} {LOG}"


# Prune and copy wait for coverage, so a round that fails it never changes the bucket.
rule prune:
    input:
        "coverage.done",
    output:
        touch("prune.done"),
    log:
        "logs/prune.log",
    resources:
        mem_mb=MEM["prune"],
    shell:
        f"{STORE} prune {ROOT} {LOG}"


rule copy:
    input:
        "prune.done",
    output:
        touch("copy.done"),
    log:
        "logs/copy.log",
    resources:
        mem_mb=MEM["copy"],
    shell:
        (
            f"{STORE} copy {ROOT} {LOG}"
            if config["copy"]
            else "echo 'No copy_to in labdata-store.toml, so the store was not copied. "
            "Set it, then run `labdata store copy`.' " + LOG
        )
