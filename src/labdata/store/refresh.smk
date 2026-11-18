# One refresh: snapshots || geo-series patch -> change list -> GEO lane || E-utilities lane
# -> confirm withdrawn Series -> pack, then the closing steps closing.smk holds: extract (after
# the snapshots) -> build -> switch -> prune -> copy.
#
# The snapshots a build reads are renewed under the refresh's start date, so a killed download
# resumes into the same folders; every later step reads the newest snapshot. The geo-series
# snapshot is patched from the previous one in its own step, beside the FTP downloads. The
# E-utilities lane runs beside the GEO lane on the experiments the saved SOFT names, and again
# after it on those the new SOFT names. The switch records the refresh's date and counts.

SNAPSHOTS = ["snapshots.done", "geo-series.done"]
EXTRACT_AFTER = SNAPSHOTS


include: "closing.smk"


FTP = sorted(set(EXTRACTS) - {"geo-series"})
DOWNLOAD = (
    f"{config['python']} -I {shlex.quote(config['downloader'])} "
    f"{shlex.quote(config['snapshots'])} --date {config['date']}"
)
REFETCH = "--series refetch.txt --refetch refetch.txt --soft-only"


rule all:
    default_target: True
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
        f"{DOWNLOAD} {' '.join(FTP)} {LOG}"


rule geo_series:
    output:
        touch("geo-series.done"),
    log:
        "logs/geo-series.log",
    resources:
        ncbi=1,
        mem_mb=MEM["geo_series"],
    shell:
        f"{DOWNLOAD} geo-series {LOG}"


rule targets:
    input:
        SNAPSHOTS,
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


rule pack:
    input:
        "confirm.done",
    output:
        touch("pack.done"),
    log:
        "logs/pack.log",
    resources:
        mem_mb=MEM["pack"],
    shell:
        f"{STORE} pack --counts pack-counts.jsonl {ROOT} {LOG}"
