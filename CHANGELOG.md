# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Calendar Versioning](https://calver.org/) using
`YYYY.MM.MICRO` (e.g. `2026.6.0`).

## [Unreleased]

### Added

- Initial pixi scaffold: `pyproject.toml` with `[tool.pixi.*]` manifest, conda-forge +
  bioconda channels, `biopython`/`typer` runtime deps, `osx-arm64` + `linux-64` platforms,
  py312/py313 environments, and standard tasks (`lint`, `fmt`, `typecheck`, `test`,
  `check`, `build`, `docs`).
- Quality gates mirroring liulab-genome: ruff rule set (E, W, F, I, UP, B, C4, SIM, PT,
  PTH, N, D, RUF) with the numpy docstring convention; pyright basic mode targeting
  py3.12; pytest with `--strict-config`, `xfail_strict`, warnings-as-errors, a `network`
  marker deselected by default, and branch coverage. `.pre-commit-config.yaml` with the
  ruff hooks plus pyright as a local pixi-backed hook.
- GitHub Actions: `ci.yml` (lint/typecheck + pytest matrix over `test-py312/313` + wheel
  build + strict docs build), `release.yml` (PyPI OIDC trusted publishing on `v*` tags),
  `docs.yml` (MkDocs build + GitHub Pages deploy), and `claude.yml`.
- `labdata._cache` — cache-directory resolution under `$XDG_CACHE_HOME/liulab-data`
  (default `~/.cache/liulab-data`), with `liulab_data_cache_dir` and `ensure_cache_dir`.
- `labdata.exceptions` — the `LabdataError` hierarchy: `AccessionError` (also a
  `ValueError`), `CredentialsError`, and `EntrezError`.
- `labdata.ncbi` — NCBI E-utilities access:
  - `config` resolves credentials from `NCBI_EMAIL`/`NCBI_API_KEY`, a cached
    `ncbi.toml` (written `0600`), or an interactive prompt; raises `CredentialsError`
    when nothing is configured and prompting is impossible.
  - `EntrezClient`, a thin, mockable wrapper over `Bio.Entrez` exposing typed
    `esearch`/`esummary`/`elink` helpers — the single network seam for the package.
- `labdata.geo` — a lazy GEO/SRA object model in `labdata/geo/records.py`, exported at
  the top level: `Series` (GSE), `Sample` (GSM), `Platform` (GPL), `Experiment` (SRX),
  and `Run` (SRR). Each validates its accession at construction (`AccessionError`
  otherwise) and resolves fields lazily via `cached_property`; construction does no
  network I/O. Records compare/hash by class + accession.
  - **Links return instances, not strings.** `Series.samples`→`[Sample]`,
    `Series.platforms`→`[Platform]`, `Series.experiments`→`[Experiment]`,
    `Sample.series`→`Series`, `Sample.platform`→`Platform`, `Sample.experiments`→
    `[Experiment]`, `Experiment.runs`→`[Run]`, `Run.experiment`→`Experiment`. Linked
    instances share the parent's Entrez client.
  - GEO records expose `geo_url` (acc.cgi) and `supplementary_http_url` (derived from the
    accession, no request); SRA records expose `url` (SRA web page).
  - `Series.bioproject_ids` (`PRJNA…`, plain strings) via `gds`→`bioproject` elink.
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
