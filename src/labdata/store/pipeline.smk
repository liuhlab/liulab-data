# One round of the full run: target list -> GEO lane || E-utilities lane -> pack, then the
# closing steps closing.smk holds: extract -> build -> switch -> prune -> copy. The snapshot
# extracts run beside the fetch.
#
# The E-utilities lane runs twice: once beside the GEO lane, and again after it, because
# the experiment list grows with the SOFT the GEO lane saved. The second run skips every
# experiment the first saved.
#
# A lane that ends with items still failed leaves <lane>.failed beside its marker, and the
# next `labdata store run` forces that lane's rule, so the lane asks for those items again
# and every later step reruns. The round still builds, with the failures as gaps and notes.

# The extracts need no fetch step.
EXTRACT_AFTER = []


include: "closing.smk"


GEO_DATE = config["snapshots"]["geo-series"]
SRA_DATE = config["snapshots"]["sra-accessions"]
TARGETS = config["targets"]


rule all:
    default_target: True
    input:
        "copy.done",


rule targets:
    output:
        TARGETS,
    log:
        "logs/targets.log",
    resources:
        mem_mb=MEM["targets"],
    shell:
        f"{STORE} _targets {FRACTION}--snapshot-date {GEO_DATE} {ROOT} {LOG}"


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
