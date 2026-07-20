# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Calendar Versioning](https://calver.org/) using
`YYYY.MM.MICRO` (e.g. `2026.6.0`).

## [Unreleased]

### Fixed

- `Series.pubmed_id` returned a verbose repr (e.g. `IntegerElement(23828890, attributes={})`)
  because Biopython parses `PubMedIds` as `IntegerElement`, whose `str()` is that repr; it
  now unwraps through `int()` and returns the bare id (e.g. `"23828890"`).

### Added

- Initial pixi scaffold: `pyproject.toml` with `[tool.pixi.*]` manifest, conda-forge +
  bioconda channels, `biopython`/`pandas`/`typer` runtime deps, `osx-arm64` + `linux-64` platforms,
  `default`/`test`/`docs` py313 environments, and standard tasks (`lint`, `fmt`, `typecheck`,
  `test`, `check`, `build`, `docs`).
- Quality gates mirroring liulab-genome: ruff rule set (E, W, F, I, UP, B, C4, SIM, PT,
  PTH, N, D, RUF) with the numpy docstring convention; pyright basic mode targeting
  py3.12; pytest with `--strict-config`, `xfail_strict`, warnings-as-errors, a `network`
  marker deselected by default, and branch coverage. `.pre-commit-config.yaml` with the
  ruff hooks plus pyright as a local pixi-backed hook.
- GitHub Actions: `ci.yml` (lint/typecheck + pytest on the py313 `test` env + wheel
  build + strict docs build), `release.yml` (PyPI OIDC trusted publishing on `v*` tags),
  `docs.yml` (MkDocs build + GitHub Pages deploy), and `claude.yml`.
- `labdata._cache` — cache-directory resolution under `$XDG_CACHE_HOME/liulab-data`
  (default `~/.cache/liulab-data`), with `liulab_data_cache_dir` and `ensure_cache_dir`.
- `labdata.exceptions` — the `LabdataError` hierarchy: `AccessionError` (also a
  `ValueError`), `CredentialsError`, `EntrezError`, and `DownloadError`.
- `labdata.ncbi` — NCBI E-utilities access:
  - `config` resolves credentials from `NCBI_EMAIL`/`NCBI_API_KEY`, a cached
    `ncbi.toml` (written `0600`), or an interactive prompt; raises `CredentialsError`
    when nothing is configured and prompting is impossible.
  - `EntrezClient`, a thin, mockable wrapper over `Bio.Entrez` exposing typed
    `esearch`/`esummary`/`esummary_many`/`elink`/`efetch` helpers — the single network
    seam for the package. `esummary_many` fetches summaries for a whole UID list in one
    batched (chunked) request, `efetch` returns raw text (e.g. the SRA `runinfo` CSV),
    and every helper's result is memoized per client instance so records sharing a
    client never repeat a request (`clear_cache()` drops the memo).
- `labdata.geo` — a lazy GEO/SRA object model in `labdata/geo/records.py`, exported at
  the top level: `Series` (GSE), `Sample` (GSM), `Platform` (GPL), `Experiment` (SRX),
  `Run` (SRR), and `BioProject` (PRJNA/PRJEB/PRJDB). Each validates its accession at
  construction (`AccessionError` otherwise) and resolves fields lazily via
  `cached_property`; construction does no network I/O. Records compare/hash by
  class + accession.
  - **Links return instances, not strings.** `Series.samples`→`[Sample]`,
    `Series.platforms`→`[Platform]`, `Series.experiments`→`[Experiment]`,
    `Series.bioprojects`→`[BioProject]`, `Sample.series`→`Series`,
    `Sample.platform`→`Platform`, `Sample.experiments`→`[Experiment]`,
    `Experiment.runs`→`[Run]`, `Run.experiment`→`Experiment`, `BioProject.series`→
    `[Series]`, `BioProject.experiments`→`[Experiment]`. Linked instances share the
    parent's Entrez client. Link resolution issues one `elink` plus a single batched
    `esummary` for the whole set (instead of one `esummary` per UID), and each linked
    instance is **seeded** with the UID/summary already fetched so reading its fields
    needs no further request.
  - Every record exposes a `url` (GEO acc.cgi page for `GSE`/`GSM`/`GPL`, SRA web page for
    `SRX`/`SRR`, BioProject page for `PRJ…`); GEO records also expose
    `supplementary_http_url` (derived from the accession, no request) and
    `supplementary_file_urls` (full download URLs, listed lazily over HTTP).
  - `Series.make_sra_run_table()` returns a tidy, run-level (`SRR`) `pandas.DataFrame`
    like NCBI's SRA Run Selector — `elink(gds→sra)` then one batched `efetch` of the
    `runinfo` report, with columns renamed to Run-Selector names (`AvgSpotLen`, `Bases`,
    `Instrument`) and the key fields (`Run`, `BioSample`, `Experiment`, `LibraryName`, …)
    ordered first. Size is reported as `size_MB` (E-utilities `runinfo` exposes no exact
    byte count).
  - `labdata.geo.sratools` — FASTQ downloader built on **sra-tools**. `SraDownloader`
    takes an `Experiment` and, per run, runs `prefetch` then `fasterq-dump`, gzips the
    FASTQ with `pigz` (falling back to `gzip`), removes the intermediate `.sra`/temp
    files, and writes a `.<SRR>.success` marker so reruns skip finished work. Output lands
    in `<output_dir>/<SRX>/` (default `./`). `Series.download(output_dir=".", n_parallel=1)`
    applies this across the whole Series — laid out as `<output_dir>/<GSE>/<SRX>/<SRR>*.fastq.gz`
    — parallelized at the run (`SRR`) level via a thread pool; a failed run is recorded
    `False` without aborting the batch. Every
    subprocess goes through the single `_run` seam (mocked in tests — no live tools or
    network in CI), failures surface as a new `DownloadError`. `sra-tools` and `pigz` are
    added to the pixi/conda deps (external binaries, not in `[project.dependencies]`).
  - `BioProject` (Entrez `bioproject`, v2.0 `DocumentSummarySet`) exposes `title`,
    `name`, `description`, `organism`, `data_type`, `registration_date`, `submitter`,
    its GEO `series`, and its SRA `experiments`. `Series.bioprojects` resolves via
    `gds`→`bioproject` elink.
  - `supplementary_files` lists a record's `suppl/` directory lazily over HTTP via the new
    `labdata.geo._web` seam (single mockable `list_directory`; 404 → empty). Replaces the
    earlier `supplementary_file_types` (removed — only exposed extensions). The GEO
    "overall design" text remains out of scope (not exposed by E-utilities).
  - SRA records parse the `sra` esummary `ExpXml`/`Runs` fragments for organism,
    instrument model, study (`SRP`), title, and per-run spot/base counts and public flag.
- `EntrezClient.esummary` handles both esummary response shapes — the classic `DocSum`
  list (`gds`/`sra`) and the v2.0 `DocumentSummarySet` (`bioproject`).
- CLI: `labdata version` and `labdata ncbi configure` (store NCBI credentials).
- MkDocs Material docs *pipeline* (`mkdocs.yml`, stub `docs/{index,reference}.md`) — page
  content is intentionally deferred until the GEO part is finalized.
- MIT license, README, AGENTS.md pointer, CLAUDE.md working agreement.
