---
search:
  exclude: true
---

# Tier coverage of GEO-linked papers

Question ([#141](https://github.com/liuhlab/liulab-data/issues/141), map
[#136](https://github.com/liuhlab/liulab-data/issues/136)): on a random sample of the store's
PMIDs, how many papers can each download tier serve? Tiers: the PMC bucket, a host `pdf_url`
from OpenAlex, OpenAlex's own paid copy. Builds on
[PMC routes](https://github.com/liuhlab/liulab-data/blob/research/pmc-routes/docs/research/pmc-routes.md)
and
[OpenAlex downloads](https://github.com/liuhlab/liulab-data/blob/research/openalex-downloads/docs/research/openalex-downloads.md).

All probes ran on 2026-11-07 with no account, no key and no email. Each number is
**measured** (counted on a live reply) or **inferred** (scaled or combined from measured
fractions). Intervals are 95% Wilson intervals; a scaled count is the fraction times the
stratum size. The scripts are in [`tier-coverage/`](tier-coverage/); the sample seed is
20261107.

## Answer

- **The PMC bucket gives 81% of PMIDs their XML and 68% a PDF.** Stratum A is 77,674 PMIDs.
  All 300 sampled were in the bucket; 83% [79–87%] had a PDF, about 64,700 papers
  (measured, scaled).
- **The XML-only papers are author manuscripts.** 16% [12–21%] of stratum A are
  manuscripts with no open-access flag; none of the 48 sampled had a PDF in the newest
  version (measured).
- **OpenAlex knows nearly every paper outside the bucket**: 98% of stratum B and 99% of
  stratum C, nearly all with a DOI (measured).
- **A host `pdf_url` exists for about half of B and 40% of C** (measured), but a script gets
  a PDF from only 19% [14–26%] of those links. Most publisher links return a 403 bot check
  (measured, 150 links). So host links add about 1,500 PDFs over B and C (inferred).
- **bioRxiv is the one host that always served.** 15 of 15 links gave a PDF; Cell Press,
  PNAS, Wiley, Oxford and ASH links gave a bot check every time (measured).
- **OpenAlex's paid copy exists for 54% of B and 39% of C**, about 8,000 papers (inferred).
  It overlaps the host links almost fully: only 24 of 500 B papers and none of 500 C papers
  had a paid copy without a host link (measured).
- **About 3,100 B papers and 6,600 C papers have no tier at all** (inferred): no PDF link,
  no paid copy. In C, 39% [35–43%] are closed access (measured).

## Strata

Counted on `pmid-pmcid-doi.parquet` (measured).

| Stratum | Rule | PMIDs |
| --- | --- | --- |
| A | PMCID, in bucket `pmc-oa-opendata` | 77,674 |
| B | PMCID, not in bucket | 6,797 |
| C | no PMCID | 11,040 |
| All | | 95,511 |

The table holds 95,511 PMIDs, 120 more than the 95,391 from the PMID count. 16 PMIDs are
below 100,000 (PMIDs 1 to 5 among them), all from BioProject XML only; they look like
placeholder values, not GEO papers (measured; "placeholder" inferred). No stratum C PMID
has a DOI in the table (measured).

## Stratum A: the PMC bucket

300 PMIDs. Each article's folder was listed over HTTPS and its newest JSON record read
(measured). S3 answered a 4-thread run with `503 Slow Down`; the sequential run with a
0.2 s pause had no error.

| Measure | n | Fraction [95%] | Scaled to 77,674 |
| --- | --- | --- | --- |
| Folder present in bucket | 300 | 100% [98.7–100] | 77,674 |
| Newest version has a PDF | 250 | 83.3% [78.7–87.1] | 64,700 [61,100–67,700] |
| No PDF in any version (XML only) | 49 | 16.3% | 12,700 |
| `is_pmc_openaccess` true | 252 | 84.0% [79.4–87.7] | 65,200 |
| `is_manuscript` true | 66 | 22.0% [17.7–27.0] | 17,100 |
| Manuscript, not open access | 48 | 16.0% [12.3–20.6] | 12,400 |
| Several versions | 8 | 2.7% [1.4–5.2] | 2,100 |

Flags against PDF (measured):

| `is_pmc_openaccess` | `is_manuscript` | Articles | Newest has PDF |
| --- | --- | --- | --- |
| true | false | 234 | 232 |
| true | true | 18 | 18 |
| false | true | 48 | 0 (1 has a PDF in an older version) |

So "author manuscript" alone does not predict a missing PDF: a manuscript later opened by
the publisher has both flags and a PDF. The rule is `is_pmc_openaccess` (measured). Licence
codes: CC BY 167, TDM 61, CC BY-NC-ND 50, CC BY-NC 18, other 4 (measured).

## Strata B and C: OpenAlex lookup

500 PMIDs each, 10 batch calls of 100 (`filter=ids.pmid:`), keyless; the 10 calls cost
$0.001 of the $0.10 daily keyless budget (measured from `x-ratelimit-remaining-usd`).
"Non-PMC `pdf_url`" means a location whose `pdf_url` is not on an NCBI or Europe PMC host.

| Measure | B (n = 500) | B scaled | C (n = 500) | C scaled |
| --- | --- | --- | --- | --- |
| Found in OpenAlex | 98.0% [96.4–98.9] | 6,660 | 99.0% [97.7–99.6] | 10,930 |
| Has DOI | 97.6% | 6,630 | 99.0% | 10,930 |
| Has PMCID in OpenAlex | 98.0% | 6,660 | 0.2% | 20 |
| `is_oa` | 98.0% | 6,660 | 60.2% [55.8–64.4] | 6,650 |
| Non-PMC `pdf_url` | 50.0% [45.6–54.4] | 3,400 | 39.8% [35.6–44.2] | 4,390 |
| `has_content.pdf` (paid copy) | 54.0% [49.6–58.3] | 3,670 | 39.0% [34.8–43.3] | 4,310 |
| Paid copy, no host link | 4.8% [3.2–7.0] | 330 | 0% [0–0.8] | 0 |
| Neither (or not found) | 45.2% [40.9–49.6] | 3,070 | 60.2% [55.8–64.4] | 6,650 |

First non-PMC host link, by host type (measured fraction of the 500):

| Host type | B | C |
| --- | --- | --- |
| Publisher | 41.8% | 30.6% |
| bioRxiv or medRxiv | 6.0% | 4.8% |
| Other repository | 2.0% | 4.4% |

`oa_status` (measured): B is green 235, bronze 146, hybrid 73, gold 33, diamond 3. Every B
paper is OA through its PMC copy, which serves no file to a script. C is closed 194, bronze
113, hybrid 75, gold 56, green 55, diamond 2.

By publication year, works with a non-PMC `pdf_url` / works found (measured):

| Year | B | C |
| --- | --- | --- |
| before 2010 | 6 / 9 | 0 / 4 |
| 2010–2017 | 103 / 183 (56%) | 44 / 93 (47%) |
| 2018 on | 141 / 298 (47%) | 155 / 398 (39%) |

## Can a script fetch the host link?

150 of the 449 B and C works with a non-PMC `pdf_url`, drawn at random; for each, the
link the tier would try first (`best_oa_location` if it qualifies, else the first in
`locations`). One GET each, User-Agent `labdata/0.1`, 1 per second, 30 s timeout,
redirects followed, no retry. A reply is a PDF when its body starts with `%PDF`; a 403 page
naming a challenge or captcha counts as a bot check (measured).

| Result | Links | Fraction [95%] |
| --- | --- | --- |
| PDF | 29 | 19.3% [13.8–26.4] |
| Bot check (all HTTP 403) | 106 | 70.7% |
| HTML page, HTTP 200 | 12 | 8.0% |
| Image, HTTP 200 | 2 | 1.3% |
| Plain 403 | 1 | 0.7% |

By stratum: B 17 / 78 (21.8% [14.1–32.2]), C 12 / 72 (16.7% [9.8–26.9]). By host type:
preprint 15 / 15, repository 3 / 10, publisher 11 / 125 (8.8%) (measured).

By host domain (measured):

| Domain | Links | PDF | Other result |
| --- | --- | --- | --- |
| `www.cell.com` | 29 | 0 | 29 bot check |
| `www.biorxiv.org` | 15 | 15 | |
| `www.pnas.org` | 13 | 0 | 13 bot check |
| `onlinelibrary.wiley.com` (+ `febs.`) | 15 | 0 | 15 bot check |
| `academic.oup.com` | 12 | 0 | 12 bot check |
| `www.nature.com` | 7 | 0 | 7 HTML |
| `www.jci.org` | 5 | 5 | |
| `ashpublications.org` | 5 | 0 | 5 bot check |
| `www.jbc.org` | 4 | 0 | 4 bot check |
| `link.springer.com` | 3 | 3 | |
| ASM (`jvi.`, `jb.asm.org`) | 4 | 0 | 4 bot check |
| CSHL Press (`rnajournal.`, `genome.cshlp.org`) | 6 | 0 | 6 bot check |
| Company of Biologists | 4 | 0 | 4 bot check |
| `ars.els-cdn.com` | 2 | 0 | 2 JPEG (graphical abstract) |
| `hal.science` | 2 | 2 | |
| `www.zora.uzh.ch` | 2 | 0 | 2 HTML |
| `www.jneurosci.org` | 2 | 0 | 2 bot check |
| Other repositories (6 domains, 1 each) | 6 | 1 | 3 bot check, 2 HTML |
| Other publishers (14 domains, 1 each) | 14 | 3 | 9 bot check, 1 HTML, 1 plain 403 |

Not every `pdf_url` points at a PDF: two were Elsevier graphical-abstract JPEGs and two were
landing pages (`doi.org`, `sciencedirect.com`) (measured). Nature `.pdf` links returned an
HTML page with HTTP 200, not a challenge (measured); why was not checked.

## Coverage by tier

Inferred: scaled fractions above, combined in tier order. Host-link PDFs are the stratum's
non-PMC `pdf_url` count times its fetch rate, on the first link only, so a paper with a
second working link is missed (an undercount).

| Tier | A (77,674) | B (6,797) | C (11,040) | All (95,511) |
| --- | --- | --- | --- | --- |
| PMC bucket XML | 77,674 | 0 | 0 | 77,674 (81%) |
| PMC bucket PDF | 64,700 | 0 | 0 | 64,700 (68%) |
| Host `pdf_url`, script gets a PDF | n/a | ~740 | ~730 | ~1,470 (1.5%) |
| OpenAlex paid copy, rest of those with one | n/a | ~2,930 | ~3,580 | ~6,500 (6.8%) |
| Nothing | 0 XML; ~13,000 no PDF | ~3,100 | ~6,700 | ~9,800 (10%) |

At 100 paid files a day per key, the paid tier's ~6,500 papers take about 65 key-days
(inferred, from the OpenAlex note's quota).

## Left uncertain

- Whether a second host link in `locations` would serve when the first did not. Only the
  first was tried.
- What the Nature `.pdf` HTML page is: a consent or login interstitial, or a block.
- Whether stratum C has papers outside OpenAlex's PMID index under a DOI only; the lookup
  was by PMID.
- The 16 low PMIDs: whether they are real GEO links or parse artefacts in the PMID count.
