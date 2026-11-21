---
search:
  exclude: true
---

# Search leaves out words too common to rank fast

A search ranks shared-word candidates by BM25, which scores every text the query's words match.
On the largest vocabularies a common word, such as "cell" or "region", matches enough texts to
make one search slow. So before ranking, the search drops a label's most common words until the
texts the rest match fit under a cap (`_RANK_CAP` in `labdata.ontology.reader`). A label made
only of such words gets its whole-text matches and no shared-word candidates; a label that mixes
them with rarer words is ranked on the rarer words.

## Considered options

- **Rank every match.** Complete, but one search on a common word scores most of a vocabulary.
- **A fixed list of stop words.** Each vocabulary has its own common words, and a fixed list
  would drop words that are rare in one vocabulary and common in another.
- **Drop by count against a cap (chosen).** Each vocabulary's own counts decide, and BM25 gives
  a common word the least weight, so the ranking loses little.

## Consequences

- A label of one common word that names no term exactly finds nothing. The caller sees an empty
  list, not a slow search.
