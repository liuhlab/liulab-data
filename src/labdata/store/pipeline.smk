# One round of the full run: target list -> GEO lane || E-utilities lane -> pack -> build
# -> coverage -> prune -> copy, with the snapshot extracts beside the fetch.
#
# Driven by labdata.store.pipeline.run, which runs snakemake in the round's folder with
# `--resources ncbi=<N>` (network steps at once) and `--cores <M>`. Each rule calls a
# `labdata store` subcommand; its marker appears only when that command exits 0, so a rerun
# starts at the first unfinished step. Each step's output also goes to logs/<step>.log.
#
# The E-utilities lane runs twice: once beside the GEO lane, and again after it, because
# the experiment list grows with the SOFT the GEO lane saved. The second run skips every
# experiment the first saved.

import shlex

LABDATA = config["labdata"]
STORE = f"{LABDATA} store"
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
    shell:
        f"{STORE} _geo --series {{input}} {ROOT} {LOG}"


rule eutils_beside_geo:
    input:
        TARGETS,
    output:
        touch("eutils-beside-geo.done"),
    log:
        "logs/eutils-beside-geo.log",
    resources:
        ncbi=1,
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
    shell:
        f"{STORE} _eutils --series {TARGETS} --date {SRA_DATE} {ROOT} {LOG}"


rule pack:
    input:
        "geo.done",
        "eutils.done",
    output:
        touch("pack.done"),
    log:
        "logs/pack.log",
    shell:
        f"{STORE} pack {ROOT} {LOG}"


# Leaves two cores to the fetch lanes, which may run at the same time.
rule extract:
    output:
        touch("extract-{source}.done"),
    log:
        "logs/extract-{source}.log",
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
    shell:
        f"{STORE} prune {ROOT} {LOG}"


rule copy:
    input:
        "prune.done",
    output:
        touch("copy.done"),
    log:
        "logs/copy.log",
    shell:
        (
            f"{STORE} copy {ROOT} {LOG}"
            if config["copy"]
            else "echo 'No copy_to in labdata-store.toml, so the store was not copied. "
            "Set it, then run `labdata store copy`.' " + LOG
        )
