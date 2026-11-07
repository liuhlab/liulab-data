# One refresh: snapshots -> change list -> GEO lane || E-utilities lane -> confirm removals
# -> reachability -> pack -> build (after the snapshot extracts) -> status -> prune -> copy.
#
# Driven by labdata.store.refresh.run, which runs snakemake in the refresh's folder,
# work/refresh/<started>/, with `--resources ncbi=<N> mem_mb=<job memory>` and `--cores <M>`.
# As in the full run's DAG, each rule calls a `labdata store` subcommand with its share of
# memory and threads, its marker appears only when that command exits 0, and its output also
# goes to logs/<step>.log. A rerun starts at the first unfinished step.
#
# The snapshots are renewed under the refresh's start date, so a killed download resumes into
# the same folders; every later step reads the newest snapshot. The E-utilities lane runs
# beside the GEO lane on the experiments the saved SOFT names, and again after it on those
# the new SOFT names. The status step writes the refresh's date and counts after the switch.

import shlex

LABDATA = config["labdata"]
STORE = f"LABDATA_MEMORY_MB={{resources.mem_mb}} LABDATA_THREADS={{threads}} {LABDATA} store"
MEM = config["mem_mb"]
ROOT = "--root " + shlex.quote(config["root"])
EXTRACTS = config["extracts"]
REFETCH = "--series refetch.txt --refetch refetch.txt --soft-only"
LOG = "2>&1 | tee -a {log}"


rule all:
    input:
        "copy.done",


rule snapshots:
    output:
        touch("snapshots.done"),
    log:
        "logs/snapshots.log",
    resources:
        ncbi=1,
        mem_mb=MEM["snapshots"],
    shell:
        f"{config['python']} -I {shlex.quote(config['downloader'])} "
        f"{shlex.quote(config['root'] + '/snapshots')} --date {config['date']} {LOG}"


rule targets:
    input:
        "snapshots.done",
    output:
        touch("targets.done"),
    log:
        "logs/targets.log",
    resources:
        mem_mb=MEM["targets"],
    shell:
        f"{STORE} _targets {ROOT} {LOG}"


rule changes:
    input:
        "targets.done",
    output:
        "refetch.txt",
    log:
        "logs/changes.log",
    resources:
        ncbi=1,
        mem_mb=MEM["changes"],
    shell:
        f"{STORE} _changes --since {config['since']} --out refetch.txt {ROOT} {LOG}"


rule geo:
    input:
        "refetch.txt",
    output:
        touch("geo.done"),
    log:
        "logs/geo.log",
    resources:
        ncbi=1,
        mem_mb=MEM["geo"],
    shell:
        f"{STORE} _geo --refetch refetch.txt --failed-marker geo.failed {ROOT} {LOG}"


rule eutils_beside_geo:
    input:
        "refetch.txt",
    output:
        touch("eutils-beside-geo.done"),
    log:
        "logs/eutils-beside-geo.log",
    resources:
        ncbi=1,
        mem_mb=MEM["eutils"],
    shell:
        f"{STORE} _eutils {REFETCH} {ROOT} {LOG}"


rule eutils_after_geo:
    input:
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
        f"{STORE} _eutils {REFETCH} --failed-marker eutils.failed {ROOT} {LOG}"


rule confirm:
    input:
        "eutils.done",
    output:
        touch("confirm.done"),
    log:
        "logs/confirm.log",
    resources:
        ncbi=1,
        mem_mb=MEM["confirm"],
    shell:
        f"{STORE} _confirm {ROOT} {LOG}"


rule reach:
    input:
        "confirm.done",
    output:
        touch("reach.done"),
    log:
        "logs/reach.log",
    resources:
        mem_mb=MEM["reach"],
    shell:
        f"{STORE} _reach {ROOT} {LOG}"


rule pack:
    input:
        "reach.done",
    output:
        touch("pack.done"),
    log:
        "logs/pack.log",
    resources:
        mem_mb=MEM["pack"],
    shell:
        f"{STORE} pack --counts pack-counts.jsonl {ROOT} {LOG}"


# Leaves two cores to the fetch lanes, which may run at the same time.
rule extract:
    input:
        "snapshots.done",
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
        f"{STORE} build --workers {{threads}} {ROOT} {LOG}"


rule status:
    input:
        "build.done",
    output:
        touch("status.done"),
    log:
        "logs/status.log",
    resources:
        mem_mb=MEM["status"],
    shell:
        f"{STORE} _refreshed {ROOT} {LOG}"


rule prune:
    input:
        "status.done",
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
