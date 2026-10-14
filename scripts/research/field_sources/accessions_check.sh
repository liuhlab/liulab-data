#!/usr/bin/env bash
# Stream SRA_Accessions.tab once. Writes, under OUT:
#   accessions_rows.tsv     rows whose Accession is in TARGETS or whose Alias is a GSM in GSMS
#   accessions_tally.tsv    row counts by Type, Status and Visibility
#   accessions_gsm.tsv      GSM aliases on EXPERIMENT rows: how many SRX each maps to
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
($1 in want) || ($10 in gsm) { print > (O "/accessions_rows.tsv") }
{ tally[$7 OFS $3 OFS $9]++ }
$7 == "EXPERIMENT" && $10 ~ /^GSM[0-9]+$/ {
  n[$10]++
  if ($3 == "live" && $9 == "public") live[$10]++
}
END {
  for (k in tally) print k, tally[k] > (O "/accessions_tally.tsv")
  for (g in n) { hist["all\t" n[g]]++; if (g in live) hist["live_public\t" live[g]]++ }
  for (h in hist) print h, hist[h] > (O "/accessions_gsm.tsv")
}' "$bulk/sra-accessions/$date/SRA_Accessions.tab"
