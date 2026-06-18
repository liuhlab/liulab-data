# CLAUDE.md — liulab-data contributor & agent agreement

`liulab-data` (import name **`labdata`**) is a lightweight package of data curation,
download, and organization utilities for the Liu Lab. It starts with GEO/NCBI and will
grow to cover Zenodo and other data sources. Keep modules general — nothing GEO-specific
leaks into the package's shared layers (`_cache`, `exceptions`, `ncbi`).

## Toolchain

- **pixi** (conda-forge + bioconda) is the only supported toolchain. Do not use bare
  pip/poetry/uv. `pyproject.toml` is the single source of truth.
- Build backend: **hatchling** + **hatch-vcs** (version derived from git tags, CalVer
  `YYYY.MM.MICRO`; never hand-edit a version).
- Platforms: `osx-arm64` (local dev) and `linux-64` (CI). Pure Python — keep it that way
  unless a native dependency is genuinely required.

## Repository layout (src-layout)

```
src/labdata/
  __init__.py        public API: Series, __version__
  _cache.py          cache-dir resolution ($XDG_CACHE_HOME/liulab-data)
  exceptions.py      LabdataError hierarchy
  cli.py             thin Typer CLI (labdata version, labdata ncbi configure)
  ncbi/              NCBI E-utilities: credential config + EntrezClient seam
  geo/               GEO records (Series today; Sample/Experiment/Run later)
tests/               pytest suite, mirrors src; mocks the EntrezClient seam
```

## Quality gates (all green before commit)

`pixi run check` = `lint` + `fmt-check` + `typecheck` + `test`.

- **ruff** (lint + format), NumPy docstring convention, 100-col lines.
- **pyright** basic mode, py3.12 baseline.
- **pytest** strict markers/config, `xfail_strict`, warnings-as-errors, branch coverage.

## Conventions / invariants

- **Type annotations on every public function**; NumPy-style docstrings
  (Parameters / Returns / Raises / Examples) on public API.
- **Network access is concentrated in one seam.** Every NCBI call goes through
  `labdata.ncbi.EntrezClient`. Domain classes take a `client` argument so tests can
  substitute a fake — never call `Bio.Entrez` directly from a domain class.
- **Lazy, cached domain objects.** `Series(...)` is cheap; network happens on first
  property access (`functools.cached_property`) and is cached thereafter.
- **Accessions are validated at construction** and malformed input raises
  `AccessionError` (a `ValueError` subclass).
- **Credentials**: resolve via env (`NCBI_EMAIL`/`NCBI_API_KEY`) → cache file
  (`~/.cache/liulab-data/ncbi.toml`) → interactive prompt. Never hang a non-interactive
  process — raise `CredentialsError` with an actionable message instead.

## Testing

- No live network in CI. Mock the `EntrezClient` seam with canned responses; assert
  laziness (the client is hit once, then served from cache).
- Live tests against real NCBI are marked `@pytest.mark.network` and deselected by
  default (`-m "not network"`); run them explicitly with `pixi run test -m network`.

## Out of scope right now

- Documentation **content** (page bodies, skills) — deferred until the GEO part is final.
  The docs *pipeline* (mkdocs + the `docs` env + workflow) is scaffolded; pages are stubs.
- `Sample` / `Experiment` / `Run` classes — `Series` returns accession strings only.
