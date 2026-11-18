# The closing steps both store DAGs end with: extract -> build -> switch -> prune -> copy.
#
# Included first by pipeline.smk (the full run) and refresh.smk (the refresh), which set
# EXTRACT_AFTER, the markers the snapshot extracts wait for, before they include it. It also
# defines the names their own rules use: STORE, MEM, ROOT and LOG.
#
# labdata.store.pipeline.drive runs snakemake in the run's folder with
# `--resources ncbi=<N> mem_mb=<job memory>` and `--cores <M>`. Each rule's mem_mb comes from
# the config's memory plan; the step gets it and its threads as LABDATA_MEMORY_MB and
# LABDATA_THREADS, and caps DuckDB and Ray within them. Each rule calls a `labdata store`
# subcommand; its marker appears only when that command exits 0, so a rerun starts at the
# first unfinished step. Each step's output also goes to logs/<step>.log.
#
# The build does not switch the store to itself: the switch step checks its coverage first.
# A build that fails coverage is never switched to, pruned for or copied.

import shlex

LABDATA = config["labdata"]
STORE = f"LABDATA_MEMORY_MB={{resources.mem_mb}} LABDATA_THREADS={{threads}} {LABDATA} store"
MEM = config["mem_mb"]
ROOT = "--root " + shlex.quote(config["root"])
LOG = "2>&1 | tee -a {log}"
EXTRACTS = config["extracts"]
FRACTION = f"--fraction {config['fraction']} " if config.get("fraction") else ""


# Leaves two cores to the fetch lanes, which may run at the same time.
rule extract:
    input:
        EXTRACT_AFTER,
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
        f"{STORE} build --workers {{threads}} --no-switch {FRACTION}{ROOT} {LOG}"


rule switch:
    input:
        "build.done",
    output:
        touch("switch.done"),
    log:
        "logs/switch.log",
    resources:
        mem_mb=MEM["switch"],
    shell:
        f"{STORE} _switch {ROOT} {LOG}"


rule prune:
    input:
        "switch.done",
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
