# liulab-data

Data curation, download, and organization utilities for the Liu Lab.

Import name: `labdata`.

## Status

Early scaffolding. The first feature is the GEO part: `labdata.Series`, a lazy handle
on a GEO Series (`GSE000000`) that resolves its linked publication, samples (GSMs), and
SRA experiments (SRXs) through NCBI Entrez. Zenodo and other databases come later.

## Development

This project uses [pixi](https://pixi.sh) with `conda-forge` + `bioconda` channels.
All Python tooling is managed by pixi.

```bash
pixi install            # solve & install the default env (resolves from pixi.lock if present)
pixi shell              # activate the env
pixi run check          # lint + fmt-check + typecheck + test (the CI gate)
```

### NCBI credentials

Entrez requests need a contact email (and accept an optional API key that raises the
rate limit). Configure once:

```bash
labdata ncbi configure                       # interactive prompt, cached to ~/.cache/liulab-data/
# or set env vars (take precedence over the cache file):
export NCBI_EMAIL="you@lab.org"
export NCBI_API_KEY="..."                     # optional
```

See [`CLAUDE.md`](./CLAUDE.md) for the full contributor/agent working agreement.

## License

MIT — see [`LICENSE`](./LICENSE).
