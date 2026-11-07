---
search:
  exclude: true
---

# OpenAlex downloads

Research for [#139](https://github.com/liuhlab/liulab-data/issues/139), under the stage 4 map
[#136](https://github.com/liuhlab/liulab-data/issues/136). Gathered 2026-11-07.

Each fact carries its evidence class: **measured** (a live probe on 2026-11-07, no key),
**read** (stated by the cited page) or **inferred** (my reasoning from read or measured facts).
Licences and terms of use are out of scope by standing rule.

## Answer

- OpenAlex serves its own cached copy of a paper from the content API:
  `https://content.openalex.org/works/{work_id}.pdf` or `.grobid-xml`. Each download costs
  $0.01, so a free key's $1 a day buys **100 files a day**. The budget resets at midnight UTC.
- Looking a work up by PMID or DOI is **free** (single-entity get). A batch filter by up to 100
  PMIDs costs $0.0001 per call.
- Every work object lists all known copies in `locations`. A copy's `pdf_url` is the host's own
  PDF link (publisher, PMC mirror, bioRxiv, arXiv, an institutional repository). Fetching that
  link costs no OpenAlex quota. So the cheap path is: free lookup, then the host's `pdf_url`,
  and the content API only when no host link works.
- The budget belongs to the **account**, not the key. Keys under one account share one $1.
  Several keys add up only when each comes from a separate account with its own verified
  email. The docs say nothing for or against one person holding several accounts.

## Content endpoint

| Fact | Class | Source |
| --- | --- | --- |
| OpenAlex caches full text for 60M+ OA works, as PDF and as Grobid TEI XML. | read | [Fulltext][fulltext] |
| URL form `https://content.openalex.org/works/{work_id}.pdf`; `.grobid-xml` for TEI XML. Key as `api_key=` or `Authorization: Bearer`. | read | [Fulltext][fulltext], [Authentication][auth] |
| A work's `has_content` (`pdf`, `grobid_xml` booleans) says which formats exist; `content_urls` gives the links. `content_urls` is API-only, not in the snapshot. | read | [Works attributes][works-attr] |
| The content endpoint serves one file per work (the `is_primary` row of the archive manifest), resolved server-side from the work ID. | read | [Fulltext][fulltext] |
| TEI XML comes with `Content-Encoding: gzip`; plain `curl` needs `--compressed`. | read | [Fulltext][fulltext] |
| A `HEAD` request on a content URL is free and returns size and type. | read | [Fulltext][fulltext] |
| `HEAD` on a content URL with no key returns `401`. | measured | `curl -I https://content.openalex.org/works/W2949140641.pdf` |
| Both probe works (one by PMID, one by DOI) had `has_content: {pdf: true, grobid_xml: true}`. | measured | probes below |
| Grobid can't parse every PDF (no OCR), so some works have a PDF and no XML. | read | [Fulltext][fulltext] |
| The official CLI (`pip install openalex-official`, `openalex download --content pdf,xml`) handles parallel downloads, retries and checkpointing. | read | [CLI][cli] |
| Whole-archive sync (S3-compatible R2 buckets plus a daily Parquet manifest) is a paid add-on to the Member+ or Partner plan only. | read | [Fulltext][fulltext], [Pricing][pricing] |

## Cost and quota

| Fact | Class | Source |
| --- | --- | --- |
| Content download: $10 per 1,000 calls, so $0.01 each. PDF and TEI XML cost the same. | read | [Example costs][costs], [CLI][cli] |
| Single-entity get (by ID, DOI or PMID): free. List and filter: $0.10 per 1,000 calls (100 results per call). | read | [Example costs][costs] |
| Free account: $1 a day, no payment method. That buys 100 content downloads. | read | [Pricing][pricing], [Example costs][costs] |
| No key: $0.10 a day, shared by everyone on the caller's IP address. | read, measured | [Example costs][costs]; the `meta.notice` of a keyless list call said so |
| No-key responses carried `x-ratelimit-limit-usd: 0.1`, and a single-work get cost `x-ratelimit-cost-usd: 0`. A filter call cost 1 credit, $0.0001. | measured | probes below |
| Budget resets at midnight UTC. `X-RateLimit-Reset` gives the seconds left. | read, measured | [Pricing][pricing], [Authentication][auth]; at 16:01 UTC the header read 28694 s, which ends at 00:00 UTC |
| Over the daily budget, the API returns `429`. A query that costs more than what is left is refused before it runs, and the refusal is free. | read | [Authentication][auth], [Example costs][costs] |
| Per-second limit: 10 requests (no key), 30 (free key or prepaid), 50 (annual plan). A per-second `429` costs nothing. | read | [Authentication][auth] |
| New accounts made on or after 3 November 2026 spend their first $1 at most $0.10 every 5 minutes (about 50 minutes for the first day's 100 files). The `429` carries `Retry-After`. | read | [Authentication][auth] |
| Usage headers on every response: `X-RateLimit-Limit`, `-Remaining`, `-Credits-Used`, `-Reset`, plus USD variants. `GET /rate-limit?api_key=...` reports the balance. | read, measured | [Authentication][auth] |
| Beyond $1: prepaid usage in $1 steps, drawn only after the free budget runs out, expiring 3 months after the last purchase. | read | [Pricing][pricing] |
| The pricing change and the content API came with the blog post "New Features and Usage-Based Pricing", dated 2026-02-25. It also said keys are required; today's docs still allow keyless use at $0.10. | read | [OpenAlex blog][blog] |

## Several keys

| Fact | Class | Source |
| --- | --- | --- |
| "Every account gets $1 of API usage per day." | read | [Pricing][pricing] |
| An agent-made key "spends from the person's daily budget"; the person's own account key is separate. So two keys on one account share one budget. | read | [Authentication][auth] |
| A key works only after its email is verified. `POST /api-keys` refuses an address that already has an account (`409`) and returns `429` for too many keys from one network or email domain in a day. | read | [Authentication][auth] |
| The docs and the blog post neither allow nor forbid one person using several accounts; neither mentions it. | read | [Authentication][auth], [Pricing][pricing], [blog][blog] |
| Several keys, each from its own account, each give $1 a day, used side by side. Each also has its own 30 requests a second. | inferred | from the per-account budget above |
| The new-account pacing applies to each new account, so a fresh key gives its first 100 files over about 50 minutes. | inferred | [Authentication][auth] |

## Mapping a paper

| Fact | Class | Source |
| --- | --- | --- |
| `GET /works/pmid:{pmid}` and `GET /works/doi:{doi}` (or the `https://doi.org/...` URL form) return one work, free. | read, measured | [Get single entities][single] |
| `GET /works?filter=ids.pmid:A\|B\|...` takes up to 100 values per call. A two-PMID batch returned both works for 1 credit. | read, measured | [Authentication][auth] (OR limit); probe below |
| `ids` carries `openalex`, `doi`, `mag`, `pmid`, `pmcid`. The PMC id appears when OpenAlex knows it. | read, measured | [Works attributes][works-attr] |
| A work can hold several DOIs, such as a preprint DOI and a journal DOI. The work's `doi` holds one; each copy's own DOI sits on its location. The `doi` filter matches any of them. | read | [Works attributes][works-attr], [Locations][locations] |
| PMID 31160781 resolved to a journal article whose bioRxiv preprint sits in the same work as a location. So a preprint and its journal version are often **one** OpenAlex work. | measured | probe below |

## What locations give

| Fact | Class | Source |
| --- | --- | --- |
| `locations` lists every copy. The single-work endpoint shows all of them; list results show the first 100. | read | [Works attributes][works-attr] |
| `pdf_url` is "a direct link to a full-text PDF of this copy", the host's PDF, not OpenAlex's cache. It is null when only a landing page is known. | read | [Locations][locations] |
| `is_oa` means this copy is free to read without paying or logging in. `version` is `publishedVersion`, `acceptedVersion` or `submittedVersion`. | read | [Locations][locations] |
| `best_oa_location` picks the best OA copy: publisher over repository, published over accepted over submitted, a direct PDF over none, and PMC or arXiv over other repositories. It feeds `open_access.oa_url`. | read | [Locations][locations] |
| `open_access.oa_url` may be a PDF or a landing page. | read | [Works attributes][works-attr] |
| Don't map a work to OpenAlex's archived file via `locations[].pdf_url`; that field is the original host's URL. | read | [Fulltext][fulltext] |
| PMID 31160781 (NSMB 2019): journal copy closed, `pdf_url` null; best OA copy is the bioRxiv preprint, with a direct `pdf_url` on biorxiv.org. | measured | probe below |
| DOI 10.1038/s41586-020-2649-2 (Nature 2020, PMCID 7759461): 17 locations. Best OA copy is the publisher, `pdf_url` on nature.com. PMC and Europe PMC copies are OA but have `pdf_url: null`. arXiv and some repositories carry direct PDF links. | measured | probe below |
| The PMC location gives no direct PDF, so PMC files are better fetched from NCBI by the PMCID in `ids.pmcid`. | inferred | from the Nature probe |
| A host `pdf_url` costs no OpenAlex quota, as it is not on `content.openalex.org`. Whether the host serves a PDF to a script (redirects, bot checks) varies by host and is not something OpenAlex promises. | inferred | [Locations][locations] |

## Probes

All on 2026-11-07, no key, four requests in total.

```bash
curl -s "https://api.openalex.org/works/pmid:31160781?select=id,doi,ids,best_oa_location,open_access,has_content,content_urls,locations,has_fulltext"
curl -s "https://api.openalex.org/works/https://doi.org/10.1038/s41586-020-2649-2?select=id,doi,ids,best_oa_location,open_access,has_content,content_urls,locations"
curl -s "https://api.openalex.org/works?filter=ids.pmid:31160781|32939066&select=id,ids&per_page=100"
curl -sI "https://content.openalex.org/works/W2949140641.pdf"
```

## Implications for the stage 4 job

These are inferences for the spec, not decisions.

- Resolve every PMID with batched `ids.pmid` filters (100 per call, about $0.0001 each). The
  whole linked set costs cents, then keep `ids`, `has_content`, `content_urls` and `locations`.
- Tier order per paper: PMC by PMCID (NCBI), then host `pdf_url`s in `best_oa_location` and
  `locations` order, then the content API. Only the last spends quota.
- Per key: 100 files a day, reset at 00:00 UTC. Read `X-RateLimit-Remaining-USD` and stop a key
  before it runs out, rather than waiting for the `429`. Make keys from separate accounts, or
  they share one budget.
- `HEAD` on the content URL is free; use it to confirm a file exists before spending $0.01.

## Left uncertain

- Whether one person may hold several accounts. The docs are silent; this note does not go
  further, per the no-terms rule.
- Whether a failed content download (a `404`, or a file that is not a PDF) is charged. The docs
  only say a pre-run refusal is free.
- How often host `pdf_url`s work for a script. Measure that on the real PMID set.
- Not probed with a key: the content download itself and the `/rate-limit` endpoint.

[auth]: https://help.openalex.org/api/authentication/
[blog]: https://blog.openalex.org/?p=3953
[cli]: https://help.openalex.org/access/cli/
[costs]: https://help.openalex.org/access/example-costs/
[fulltext]: https://help.openalex.org/access/fulltext/
[locations]: https://help.openalex.org/data/locations/
[pricing]: https://help.openalex.org/access/pricing/
[single]: https://help.openalex.org/api/get-single-entities/
[works-attr]: https://help.openalex.org/data/works/attributes/
