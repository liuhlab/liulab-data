#!/usr/bin/env bash
# Stream SRA_Accessions.tab once. Writes, under OUT:
#   accessions_rows.tsv     rows whose Accession is in TARGETS or whose Alias names a GSM in GSMS
#   accessions_tally.tsv    row counts by Type, Status and Visibility
#   accessions_gsm.tsv      route, SRX count, number of GSMs: how many experiments each GSM
#                           reaches through the EXPERIMENT alias ("GSMn" or "GSMn: title"),
#                           the RUN alias ("GSMn_k") and either
# Usage: accessions_check.sh BULK_ROOT DATE TARGETS GSMS OUT
set -euo pipefail
bulk=$1 date=$2 targets=$3 gsms=$4 out=$5
mkdir -p "$out"
awk -F'\t' -v OFS='\t' -v T="$targets" -v G="$gsms" -v O="$out" '
BEGIN {
  while ((getline a < T) > 0) want[a] = 1
  while ((getline g < G) > 0) gsm[g] = 1
}
NR == 1 { print > (O "/accessions_rows.tsv"); next }
{
  g = ""
  if (match($10, /^GSM[0-9]+/)) g = substr($10, 1, RLENGTH)
  if (($1 in want) || (g in gsm)) print > (O "/accessions_rows.tsv")
  tally[$7 OFS $3 OFS $9]++
  if (g == "") next
  rest = substr($10, length(g) + 1)
  if ($7 == "EXPERIMENT" && (rest == "" || rest ~ /^: /)) {
    if (!((g, $1) in se)) { se[g, $1] = 1; ne[g]++ }
    if (!((g, $1) in su)) { su[g, $1] = 1; nu[g]++ }
  } else if ($7 == "RUN" && rest ~ /^_[0-9]+$/) {
    if (!((g, $11) in sr)) { sr[g, $11] = 1; nr[g]++ }
    if (!((g, $11) in su)) { su[g, $11] = 1; nu[g]++ }
  }
}
END {
  for (k in tally) print k, tally[k] > (O "/accessions_tally.tsv")
  for (g in ne) hist["experiment_alias" OFS ne[g]]++
  for (g in nr) hist["run_alias" OFS nr[g]]++
  for (g in nu) hist["either" OFS nu[g]]++
  for (h in hist) print h, hist[h] > (O "/accessions_gsm.tsv")
}' "$bulk/sra-accessions/$date/SRA_Accessions.tab"
