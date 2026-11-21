---
search:
  exclude: true
---

# How the EBI's Ontology Lookup Service holds many ontologies

Dated **2026-11-21**. The question: how does the EBI's Ontology Lookup Service (OLS4) declare,
load, version and serve a few hundred ontologies, and what should liulab-data's ontology store
copy from it, so that a 13th vocabulary needs a new entry and no new code?

This note is not about calling OLS4 as a search service. Its search options, ranking, coverage
of the 12 vocabularies and rate limits are in seqforge's note,
[looking up ontology terms for the reading's vocabularies](https://github.com/liuhlab/seqforge/blob/research/ontology-term-lookup/docs/research/ontology-term-lookup-2026-11-21.md).

Evidence classes. **Measured** means a call or a download made on 2026-11-21, and the fact is
its output: a few OLS4 API calls, the OBO Foundry registry file, and the OBO files of nine
vocabularies streamed once each. **Read** means the named source file or page says it.
**Inferred** says so. OLS4 code paths are in
[`EBISPOT/ols4` at commit `c857752`](https://github.com/EBISPOT/ols4/tree/c857752998e0db9e49dfe08950cfdfbf95d8bffb)
(2026-11-18), cited as `path:line`.

## Verdict

- **OLS4 is driven by a list of entries, not by code per ontology.** Its list is the OBO
  Foundry registry plus an EBI file of additions and overrides. Each entry names an id, a
  prefix and a download address, and may name label, synonym and definition properties and base
  URIs. Defaults cover the usual OBO case, so most entries carry no extra keys (read).
- **It reloads every ontology on every run and keeps one copy.** A failed load falls back to the
  last good copy of that ontology alone, flagged as a fallback (read). It keeps no older version.
- **"Own term" is decided by the term's address, not by imports.** A term belongs to an
  ontology when its address starts with that ontology's prefix address. In OLS4, a CL term
  inside UBERON is flagged "not imported" and also "not defined here" (measured). For our 12
  vocabularies the id prefix alone is enough (measured on 9 files), and it is needed: only 16,143
  of the UBERON file's terms are UBERON's own; the rest come from GO, CL, ChEBI and others.
- **The OBO Foundry registry cannot be the only source of download addresses.** It lists no
  `uberon.obo` product, only `uberon-basic.obo` (measured), and it says nothing about versions.
  It is a useful cross-check for each vocabulary's prefix.
- **Correction to the brief:** OLS4 no longer uses Solr or Neo4j. At this commit it loads
  everything into PostgreSQL (read: `README.md:34`, `docs/facet_optimization.md:7`).

## 1. The ontology list and one entry

**Where the list lives (read).** The load starts from one merged config file. In production,
`fetch_configs` copies two files from an EBI file server, `obo.json` and `ebi.json`
(`dataload/nextflow/ols_dataload.nf:37-55`). `obo.json` is the OBO Foundry registry, turned from
YAML to JSON (`dataload/configs/get-foundry-config.sh`). `ebi.json` holds EBI additions and
overrides; the repo copy, `ebi_ontologies.json`, has 128 entries (measured). The OBO file is
read first, so an EBI key wins (`ols_dataload.nf:60-66`). The merge works key by key: an entry
with the same id replaces only the keys it names (`dataload/merge_configs/.../MergeConfigs.java:87-104`).

**What an entry carries (read).** From the registry: `id`, `preferredPrefix`, `ontology_purl`
(the download address) and `products`. With no `ontology_purl`, the loader takes the first
product whose address ends in `.owl` (`dataload/rdf2json/.../OntologyGraph.java:253-271`). An
entry with `is_obsolete: true` is skipped (`RDF2JSON.java:191-199`). The EBI file adds, among
others, `base_uri`, `label_property`, `synonym_property`, `definition_property`,
`hierarchical_property`, `hidden_property`, `preferred_root_term`, `reasoner` and
`shortFormExtractionPattern`. A real EBI entry, the Experimental Factor Ontology
(`ebi_ontologies.json:518-547`, shortened):

```json
{
  "id": "efo",
  "preferredPrefix": "EFO",
  "definition_property": ["http://purl.obolibrary.org/obo/IAO_0000115",
                          "http://www.ebi.ac.uk/efo/definition"],
  "synonym_property": ["http://www.ebi.ac.uk/efo/alternative_term",
                       "http://www.geneontology.org/formats/oboInOwl#hasExactSynonym"],
  "base_uri": ["http://www.ebi.ac.uk/efo/EFO_"],
  "reasoner": "OWL2",
  "ontology_purl": "http://www.ebi.ac.uk/efo/efo.owl"
}
```

**Defaults (read).** With no `label_property`, the label is `rdfs:label` or one of three other
title properties (`LabelAnnotator.java:19-37`). The synonym properties always include the four
OBO scopes plus `hasSynonym`; `synonym_property` adds to them
(`SynonymAnnotator.java:17-30`). So an OBO ontology needs no extra keys.

**Our 12 in the EBI file (read).** Only three of them have an EBI entry, and each is small.
MONDO loads `mondo-international.owl` instead of `mondo.owl` (`ebi_ontologies.json:70`). UBERON
names two preferred root terms (`:415`). NCBITaxon names its base URI (`:955`). The rest load
straight from the registry entry.

**Adding an ontology (read).** The owner fills in a spreadsheet and opens a GitHub issue. The
issue template names `preferredPrefix` and `ontology_purl` as the two important fields, and says
OLS checks that address often for new releases
(`.github/ISSUE_TEMPLATE/add-a-new-ontology-to-the-ebi-ols-instance-.md`). The production
`ebi.json` lives off GitHub, so a pull request to `ebi_ontologies.json` may not be the path
(inferred).

## 2. Loading, versions and failures

**Steps (read).** The load is a Nextflow pipeline (`ols_dataload.nf:57-191`). Per ontology,
`rdf2json` downloads the OWL file, follows its imports and writes one JSON file. Its limits are
500 GB of memory and 10 hours (`:217-222`). Then a linker pass over all of them works out which
ontology defines each term. Then come a PostgreSQL load, a text tagger index, optional
embeddings, and a load report.

**How often (measured, inferred).** UBERON, HsapDv, ChEBI and NCBITaxon all show a `loaded`
time between 00:08 and 00:10 on 2026-11-21. So OLS4 reloads every ontology together, at least
on that night, and probably nightly (inferred). Nothing in `rdf2json` compares a release with
the last run's before it parses the file again (read: `OntologyGraph.java:253-279`).

**Version record (read, measured).** Each ontology gets `loaded` (the load time) and
`sourceFileTimestamp` (`OntologyGraph.java:370-378`). The API also shows the file's version IRI
and its `owl:versionInfo` as `version`. Measured on 2026-11-21:

| Ontology | `version` | `versionIri` |
| --- | --- | --- |
| UBERON | none | `.../uberon/releases/2026-11-05/uberon.owl` |
| HsapDv | `2025-01-23` | `.../life-stages/releases/2025-01-23/components/hsapdv.owl` |
| ChEBI | `257` | `.../chebi/257/chebi.owl` |
| NCBITaxon | `2026-11-09` | `.../ncbitaxon/2026-11-09/ncbitaxon.owl` |

So `version` is missing when a file has no `versionInfo`, and the version IRI is the field
that is always there for these four. OLS4's NCBITaxon is the OBO build of the taxonomy, 12 days
behind NCBI's daily dump.

**Failures (read).** Each ontology loads in its own task, and a failure does not stop the run
(`errorStrategy 'ignore'`, `ols_dataload.nf:221`). When `OLS_LAST_RUN_DIR` holds that
ontology's JSON from the last good run, the loader uses it (`--mergeOutputWith`, `:240-243`). It
writes status `FALLBACK`, and flags the ontology `is_fallback` with a reason the website shows
(`RDF2JSON.java:246-316`). With no earlier copy the status is `FAILED_NO_FALLBACK`. After a
`SUCCESS` or `FALLBACK`, the new JSON overwrites the saved one (`ols_dataload.nf:256-258`). So
OLS4 keeps one copy per ontology and no history.

## 3. Imported terms and the defining ontology

**Two flags (read).** `imported` is true for a term that only came in through an `owl:imports`
file (`OntologyGraph.java:283-307`). `isDefiningOntology` is true when the term's address starts
with one of the ontology's base URIs. The linker always adds
`http://purl.obolibrary.org/obo/{preferredPrefix}_` to the `base_uri` list
(`dataload/linker/create_manifest/src/linker_pass1.rs:146-153, 292-300`). `definedBy` lists the
ontologies that define the term. The search option `local=true` keeps only defining terms
(`V1SearchController.java:153-155`).

**Only the address flag works (measured).** OBO releases merge imported terms into the main
file. In OLS4's UBERON, `CL:0000540` (neuron) has `imported: false`, `isDefiningOntology: false`
and `definedBy: ["cl"]`. UBERON's own `UBERON:0002421` has `isDefiningOntology: true`.

**Is one prefix enough for our 12? Yes (measured).** Term ids by prefix in each OBO file,
obsolete terms included:

| File | Own prefix, terms | Largest other prefixes |
| --- | --- | --- |
| `uberon.obo` | UBERON 16,143 | GO 6,918; CL 1,484; CHEBI 778; NCBITaxon 480 |
| `cl.obo` | CL 3,890 | GO 7,757; UBERON 5,245; full gene addresses 1,179; PR 980; CLM 955 |
| `mondo.obo` | MONDO 36,094 | full gene addresses 5,806; UBERON 5,360; HP 4,860; GO 4,142 |
| WBbt, PATO, HsapDv, MmusDv, WBls, FBdv | own prefix only | none |

The full addresses are `identifiers.org` gene ids (NCBI Gene, HGNC, Ensembl). `CLM` is CL's
marker gene sets, not cell types. ChEBI's file holds only `CHEBI` ids (measured; see the end of
this note). All 5,243 Cellosaurus ids in the file's first 3 MB start `CVCL_` (measured), and
the taxdump holds only taxonomy ids. So an
own-prefix filter is both needed and enough for the 12. A list of prefixes, as OLS4's
`base_uri` list allows, covers a later vocabulary that needs two (inferred).

## 4. Search fields and options

**Per term (read, measured).** OLS4 keeps every synonym in one `synonym` field, and also each
scope under its own property name. The old search API returns them apart, as `exact_synonyms`,
`related_synonyms`, `narrow_synonyms` and `broad_synonyms` (`V1SearchController.java:82,
224-227`). Each synonym also keeps its OBO synonym type. On `UBERON:0002421` (hippocampal
formation), the synonym "hippocampus" is NARROW and typed `INCONSISTENT`: "used in an
inconsistent or confusing way, typically between species" (measured). Obsolete terms are those
with `owl:deprecated true`, among other tests (`IsObsoleteAnnotator.java:44-59`).

**Options worth copying (read).** The search takes `exact`, `queryFields`, `obsoletes` (off by
default), `local`, `childrenOf`, `allChildrenOf`, `isLeaf`, `rows` and `start`
(`V1SearchController.java:65-124`).

- `exact` and `queryFields`: cheap for us, as whole-text and name-only searches.
- `local`: our store always applies it.
- `obsoletes`: our default matches OLS4's; a flag to include them costs little.
- `allChildrenOf`: a subtree limit, such as "under Metazoa" for the taxonomy or "part of
  brain" for UBERON. It needs each term's ancestors stored. Worth it later, not now.

## 5. Identifiers

**OLS4's rule (read).** OLS4 builds each term's short form from its address. When the address
starts with a base URI it writes `PREFIX_local`. Then it turns the first `_` into `:` for the
CURIE (`ShortFormAnnotator.java:21-78, 115-121`). With no `preferredPrefix`, it uses the
upper-cased ontology id. `shortFormExtractionPattern` is a regular expression for odd addresses.
Bioregistry, a community list of id prefixes, is used only to turn cross-references into links
(`dataload/linker/link/src/bioregistry.rs:6-13`). It is not used to build a term's own id.

**Id form for our 12 (measured, read).** Store the id as each source writes it:

| Vocabulary | Id form | Note |
| --- | --- | --- |
| UBERON, WBbt, CL, MONDO, PATO, HsapDv, MmusDv, WBls, FBdv, ChEBI | `UBERON:0002421`, `WBbt:…`, `CL:…`, `MONDO:…`, `PATO:…`, `HsapDv:…`, `MmusDv:…`, `WBls:…`, `FBdv:…`, `CHEBI:41879` | as in each file's `id:` line; mixed case as each ontology spells it |
| NCBI Taxonomy | `NCBITaxon:9606` | the taxdump holds `9606`; the store adds the prefix |
| Cellosaurus | `CVCL_0030` | Cellosaurus's own form. Bioregistry's CURIE would be `cellosaurus:0030`, with `CVCL` as the part it drops (read: bioregistry.io API) |

A lookup could also accept `cellosaurus:0030` and a bare `9606`, and map them to the stored
form (inferred).

## 6. Related EBI tools

**ZOOMA** maps free text to terms, from text that EBI curators have already mapped by hand.
`propertyValue=hippocampus` returns `UBERON_0002421` (measured). Its curated sets are now
published as SSSOM tables (a standard table format for mappings) in
[`mapping-commons/ebi-text-mappings`](https://github.com/mapping-commons/ebi-text-mappings).
The sources include Expression Atlas, HCA, FAANG and BioSamples (read: its README). OLS4 loads
them, when switched on, and shows them on each term as `curatedFrom`
(`ols_dataload.nf:312-326`; measured on `UBERON:0002421`). **Relevant to seqforge:** these are
past human decisions on the same kind of sample label. They could be one more table in the
store, looked up before the word search (inferred).

**OxO** serves cross-references between ontologies, such as `UBERON:0002421` → `FMA:74038`
(measured). It does not turn text into terms. It could help when a paper names a term from a
vocabulary we don't hold, but it is not needed for label search (inferred).

## 7. The OBO Foundry registry as a source

**What it holds (measured).** `https://obofoundry.org/registry/ontologies.jsonld` lists 268
ontologies. All 10 of our OBO Foundry vocabularies are there, `active`, with `preferredPrefix`
and an OWL `ontology_purl`. Nine list `{id}.obo` as a product. UBERON lists only
`uberon/uberon-basic.obo`, though `purl.obolibrary.org/obo/uberon.obo` serves the full file
(measured in the table below). The registry has no version, no version rule and no base URI.

**So (inferred):** the registry cannot make a new vocabulary a one-line entry by itself. The
address `purl.obolibrary.org/obo/{id}.obo` works for all 10, so a declaration can default to
it. A test can check each declared prefix against the registry's `preferredPrefix`.

## Download facts (measured 2026-11-21)

All 12 can be downloaded by script, with no login, from stable addresses:

| Vocabulary | Address | Size | Version field in the file |
| --- | --- | --- | --- |
| UBERON | `purl.obolibrary.org/obo/uberon.obo` | 22.5 MB | `data-version: releases/2026-11-05` |
| WBbt | `purl…/wbbt.obo`, which redirects to GitHub raw `master` | 1.6 MB | `releases/2025-08-19` |
| CL | `purl…/cl.obo` | 19.8 MB | `releases/2026-11-13` |
| MONDO | `purl…/mondo.obo` | 53.4 MB | `releases/2026-11-03` |
| PATO | `purl…/pato.obo`, which redirects to GitHub raw `master` | 0.8 MB | `releases/2026-10-01/pato.obo` |
| HsapDv | `purl…/hsapdv.obo` | 0.14 MB | none in the OBO file; header `date: 05:02:2021` |
| MmusDv | `purl…/mmusdv.obo` | 0.10 MB | none in the OBO file; same as HsapDv |
| WBls | `purl…/wbls.obo`, which redirects to GitHub raw `master` | 0.4 MB | `wbls/releases/2026-04-16/wbls-simple.owl` |
| FBdv | `purl…/fbdv.obo` | 0.12 MB | `fbdv/releases/2026-11-02` |
| ChEBI | `purl…/chebi.obo`, which redirects to `ftp.ebi.ac.uk` | 274 MB | `data-version: 257` |
| Cellosaurus | `ftp.expasy.org/databases/cellosaurus/cellosaurus.obo` | 118.6 MB | `data-version: 57.0`; also `api.cellosaurus.org/release-info` |
| NCBI Taxonomy | `ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz`, with a `.md5` beside it | 80.4 MB | none; Last-Modified, rebuilt daily |

**HsapDv and MmusDv do have a version.** Both OBO purls redirect to GitHub's
`releases/latest/download/` of `developmental-stage-ontologies`. The OWL files there carry
`owl:versionInfo 2025-01-23` and a version IRI with that date; OLS4 reports the same version
(measured). The OBO files carry no `data-version`. So their snapshot can be named
`hsapdv-2025-01-23`, from the OWL header or the GitHub release.

More facts:

- **Synonym scopes:** UBERON, WBbt, CL, MONDO, PATO, HsapDv, WBls and FBdv use all four. MmusDv
  has no BROAD synonym. ChEBI showed only EXACT and RELATED. Every Cellosaurus synonym found was
  RELATED.
- **Obsolete terms:** `is_obsolete` appears in all the OBO Foundry files. For ChEBI, see the
  next paragraph. Cellosaurus's OBO has no `is_obsolete`; deleted accessions are listed in
  `cellosaurus_deleted_ACs.txt`.
- **`replaced_by` can point into another vocabulary:** a CL term points to a CLO term, and a
  MONDO term to a CHEBI term. WBbt and WBls have obsolete terms with no `replaced_by`.
- **The taxdump** has `names.dmp`, `merged.dmp` (an old id merged into a new one) and
  `delnodes.dmp`.

**ChEBI's retired ids are mostly `alt_id` lines, not obsolete terms.** A streamed scan of
`chebi.obo` stopped at its 500-second limit after 174,293 `[Term]` stanzas, most of the file. It
found one `is_obsolete: true` (measured). OLS4, which loads `chebi.owl` of the same release 257,
counts 19,427 obsolete ChEBI classes. One is `CHEBI:1`, replaced by `CHEBI:18357`
((R)-noradrenaline), which lists `CHEBI:1` among its alternative ids (measured). So the OWL turns
each `alt_id` into an obsolete class, while the OBO file keeps it as an `alt_id` line on the live
term (inferred). A lookup by id must index `alt_id`, or an old ChEBI id finds nothing.

## What the ontology store should take from it

1. **One declaration per vocabulary, as data.** Each entry names: the id, the own-term
   prefixes, the download address, the reader kind (`obo`, `cellosaurus`, `taxdump`), and the
   version rule (`data-version` header, OWL `versionInfo`, Last-Modified date, or a release
   API). Defaults cover a plain OBO Foundry file: address `purl.obolibrary.org/obo/{id}.obo`,
   the four synonym scopes, `name:` as the label. A new OBO vocabulary is then three lines.
2. **Own terms by id prefix, from a declared list.** It is the rule OLS4 uses, and it is the
   one that works on merged OBO files. Imports in the file say nothing useful.
3. **Check the version before downloading.** OLS4 parses every file every night. We read a
   header, a Last-Modified date or an `.md5`, and download only what changed.
4. **A failed vocabulary leaves its current snapshot in place.** Record a status per
   vocabulary in the update's status record (updated, unchanged, failed), as OLS4 does with
   `SUCCESS` and `FALLBACK`, and let the other vocabularies go on.
5. **Record what OLS4 records:** the release, the source file's date, the load time and the
   term counts, per snapshot.
6. **Keep the synonym type next to the scope.** A candidate that matched through an
   `INCONSISTENT` synonym should say so, so the model that picks among candidates can see it.
7. **A lookup by id knows every retired id.** Each reader gives the same table of old id → live
   id or "gone": `alt_id` and `replaced_by` from OBO files, `merged.dmp` and `delnodes.dmp` from the
   taxdump, and Cellosaurus's deleted list. A new vocabulary fills the same table.
8. **Search options to add as they are needed:** whole text only, name only, include obsolete.
   A subtree limit waits until a reading needs it.
9. **The EBI's curated text mappings are a candidate source,** loaded as a table beside a
   vocabulary. That is a later decision, not part of the first 12.

**Leave out:** a database server (PostgreSQL now; Solr and Neo4j before), OWL parsing with
import resolution and a reasoner (a 500 GB, 10-hour task in OLS4), a nightly reload of
everything, the cross-ontology linker, embeddings, and Bioregistry at load time. Twelve
declared prefixes need no prefix registry.
