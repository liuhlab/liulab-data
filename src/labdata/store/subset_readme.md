# A subset of a labdata store

This folder is a subset of a labdata store. It holds the metadata records of a fixed list of
GEO Series and of every record they link to: GEO Samples and Platforms, SRA studies,
experiments, samples and runs, BioProjects and BioSamples. The tables and their layout are
those of a full store, so the `labdata` Python package and DuckDB read it the same way.
Paths in this file are relative to the folder, written `<store>`.

Current build: `${build}`, cut at ${cut} by `${command}` at commit `${commit}`,
labdata ${version}. It was cut from build `${source_build}` of the full store, which was made
at commit `${source_commit}` and reads these snapshots: ${snapshots}. The list holds
${series} Series.

## Contents

```text
<store>/
  README.md                     this file
  labdata-store.toml            the marker: the current build, and the build it was cut from
  builds/<build-id>/            the tables (query these)
```

The build folder has its own `README.md` and `manifest.json`. They list each file with its
size in bytes and its sha256, the Series list, and each link from a listed Series to a Series
outside the list. To check every file against its manifest:

```bash
labdata store verify --root <store>
```

## What it holds

- Every listed Series the full store held, and every record reached from one by following
  links forward: a Series to its Samples, a Sample to its experiment, an experiment to its
  runs, study, sample and BioSample, and each of them to its Platform or BioProject.
- The parent of each held BioProject, as a record only.
- Every link of a held record, and every link of a paper that a held Series or BioProject
  links. So a paper or a Platform shared with other Series still lists them all.
- No Series outside the list. Opening one raises `NotMirrored`: the store knows it and does
  not hold it.

## What it lacks

There is no `raw/`, `snapshots/` or `extracts/`, so nothing can be built or refreshed here.
The marker names no copy target, so `labdata store copy` has nowhere to send it. To change
the list or take a newer build, run `labdata store subset` again on the full store.

## Read it

```python
from labdata.store import open_store

store = open_store("<store>")
series = store.open("GSE246308")
```

Or set `LABDATA_STORE` to this folder, and `open_store()` finds it.
