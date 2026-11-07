---
search:
  exclude: true
---

# Remove withdrawn SRA records once a year, never in the monthly refresh

This amends [ADR 0005](0005-keep-sra-records.md). The monthly refresh still never removes an SRA
experiment or run. A yearly maintenance run does, with its own step, `labdata store remove-sra`.

A candidate is a held experiment, or a run in one, that the newest `SRA_Accessions.tab` marks
non-public, or a held experiment no live Series reaches. A live Series is one the newest
`geo-series` snapshot lists. A candidate is removed only when a fetch of its experiment alone
confirms it gone, by the rule the Series confirm uses; any other answer keeps it. Removals go in
as removal parts, applied by pack.

## Considered options

- **Keep every SRA record for good.** The store keeps growing with records NCBI no longer serves.
- **Remove them in each refresh.** Confirming SRA candidates and finding unreached records cost
  about an hour of the first refresh, for little change from one month to the next.
- **Remove them once a year.** The monthly refresh stays light, and the store drops dead records
  within a year.

## Consequences

- For up to a year, a direct lookup of a withdrawn experiment returns its old record.
- An unreached experiment that NCBI still serves stays, so the step asks again each year about
  records fetched by hand.
