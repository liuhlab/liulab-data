# Reasons for missing raw data

The store does not record why a GEO sequencing Series has no reads in SRA. It holds such a
Series like any other, with no link to an SRA experiment, and keeps the Series' text as GEO
served it.

## Why this is out of scope

The store mirrors each source and adds no reading of its own (ADR 0001). GEO gives no
structured field for why raw data is missing. Its relation labels name SRA, BioProject and
BioSample, but not dbGaP, GSA or "withheld". The reason, when there is one, is in free text.
GSE158055, for example, has 284 samples that link only to BioSamples, and its
`Series_overall_design` says the raw data is unavailable for patient privacy.

Turning that prose into a reason is a judgment call, and it belongs downstream, in a
consumer such as seqforge. The store gives that consumer what it needs: the store README has
a DuckDB query that lists Series with no SRA experiment, and each Series' full text is in
its record.

## Prior requests

- #111: "GEO sequencing Series with no SRA experiments, such as GSE158055"
