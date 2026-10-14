# Prototype: the Python API for one record and its links

Throwaway code for [The Python API: opening one record and following its links](https://github.com/liuhlab/liulab-data/issues/28).
It follows the record model from
[The source record model: entities, links and keys](https://github.com/liuhlab/liulab-data/issues/26)
and the store root rules from issue #29. None of it goes into `src/labdata`.

The backend is a JSON file behind a batch-shaped interface (`Backend` in `labstore.py`). Each
method takes a collection of accessions, so a columnar backend can answer it with one scan or one
join. The table format is open in issue #27, so nothing here picks one.

## Files

- `labstore.py`: the stub. `open_store`, `Store.open`, `open_many`, `links`, `follow`,
  `experiments_for`, `experiments_for_many`, `table`, and the errors.
- `store/`: a store root. `labdata-store.toml` is the marker; `records.json` is the fixture.
- `fetch_fixture.py`: regenerates `records.json` live from NCBI.
- `demo.py`: the walkthrough below.

The fixture holds 78 records from 3 slices: PRJNA1027859 (SRA-only, C. elegans, 6 experiments),
the SuperSeries GSE246310 with its 3 SubSeries trimmed to 2 GSMs each, and GSE19456, where
GSM484820 maps to 2 SRX. Everything else these records link to is left out on purpose, so the
demo can show `NotMirrored`.

## Run it

From the repo root:

```bash
pixi run python prototypes/python_api/demo.py
```

To refetch the fixture (about 20 requests at 2 per second; uses `NCBI_EMAIL` and `NCBI_API_KEY`
or the per-user `ncbi.toml` when set):

```bash
pixi run python prototypes/python_api/fetch_fixture.py
```

## Demo output

The output below is pasted verbatim from a run on 2026-10-14. Each `[backend calls: ...]` line counts
the calls the fake backend received for that step.

```text

=== 1. open_store: root from argument, then $LABDATA_STORE, then the per-user file
open_store()                 -> StoreNotConfigured: store not configured: pass open_store(root=...), set LABDATA_STORE, or run `labdata store configure`
open_store(root=<no marker>) -> NotAStore: 'python_api' is not a labdata store: no labdata-store.toml
LABDATA_STORE=.../store; open_store() -> Store('store', 78 records)

=== 2. store.experiments_for(acc), the offline twin of labdata.experiments_for
PRJNA1027859  -> ['SRX24283130', 'SRX24283131', 'SRX24283132', 'SRX24283133', 'SRX24283134', 'SRX24283135']
GSM484820     -> ['SRX018673', 'SRX018710']
SRR28716558   -> ['SRX24283130']
GSE246310     -> NotMirrored: 30 records under GSE246310 are not in this store: GSM7866520, GSM7866521, GSM7866522, GSM7866523, ...
GSE246310, partial=True -> ['SRX22239377', 'SRX22239378', 'SRX22240054', 'SRX22240055', 'SRX26377937', 'SRX26377938']

=== 3. store.open(acc): a GEO Sample whose one GSM has two SRA experiments
GSM484820  type='GEO Sample'  source='SOFT'
  status='Public' submitted=2009-12-14 updated=2019-05-15 published=2010-12-05
  content_hash=sha256:eed04df55197f798...  snapshot_date=2026-10-14
  41 raw fields, first 6:
    Sample_title = Lung_pooled 3 control pigs (C)
    Sample_geo_accession = GSM484820
    Sample_status = Public on Dec 05 2010
    Sample_submission_date = Dec 14 2009
    Sample_last_update_date = May 15 2019
    Sample_type = SRA
  9 links (-> forward, <- read from the far end):
    -> BioSample       SAMN00010819   [SOFT, in GSM484820]
    -> SRA             SRX018673      [Entrez link, in GSM484820]
    -> SRA             SRX018673      [SOFT, in GSM484820]
    -> SRA             SRX018710      [Entrez link, in GSM484820]
    -> SRA             SRX018710      [SOFT, in GSM484820]
    -> platform        GPL9126        [SOFT, in GSM484820]
    <- series          GSE19456       [SOFT, in GSE19456]
    <- series          GSE19456       [SOFT, in GSM484820]
    <- series          GSE19970       [SOFT, in GSM484820]
  gsm.field('Sample_characteristics_ch1') = ['tissue: lung', 'treatment group: control']
  gsm.follow('SRA') = ['SRX018673', 'SRX018710']
  records are frozen: 'tuple' object does not support item assignment
  [backend calls: {'get_many': 1, 'links_many': 1}]

=== 4. Following links both ways (SRA-only study PRJNA1027859)
SRR28716558  type='SRA run'  source='SRA XML'
  status='public' submitted=None updated=None published=2024-05-28
  content_hash=sha256:5796ab8f59da791d...  snapshot_date=2026-10-14
  128 raw fields, first 4:
    RUN@accession = SRR28716558
    RUN@alias = 2235__N2_wild_type_neuron_nuclei_library-read-1.fastq.gz
    RUN@total_spots = 455092911
    RUN@total_bases = 55521335142
SRR28716558 -run of-> SRX24283130 -study-> SRP502277 -BioProject-> PRJNA1027859
PRJNA1027859 -BioProject of-> ['SRP502277']
PRJNA1027859 -PubMed-> [('PMID:39637862', 'PubMed')]
SRX24283130 -SRA sample-> SRS21047787 -BioSample-> SAMN40935616 (status='live', published=2024-05-28)

=== 5. SuperSeries <-> SubSeries, BioProject umbrella, one GSM in two Series
GSE246307 -SubSeries of-> ['GSE246310']
    stated by SOFT in GSE246307
    stated by SOFT in GSE246310
GSE246310 -SuperSeries of-> ['GSE246307', 'GSE246308', 'GSE246309']
GSE246310 -BioProject-> PRJNA1032473 -umbrella of-> ['PRJNA1032481', 'PRJNA1032482', 'PRJNA1032483']
GSM484820's two experiments lead to two SRA studies and two BioProjects:
  SRX018673 -study-> SRP002215 -BioProject-> ['PRJNA120321'] -GEO-> ['GSE19456']
  SRX018710 -study-> SRP002222 -BioProject-> ['PRJNA122621'] -GEO-> []
  GSM484820 -series-> ['GSE19456', 'GSE19970']

=== 6. NotMirrored vs NotFound
GSE1X          -> AccessionError: not a resolvable accession: 'GSE1X'
GSE246309      -> Record: GEO Series
SAMN00010819   -> NotMirrored: SAMN00010819 (BioSample) exists at its source but is not in this store
GSM9999999999  -> NotFound: GSM9999999999 (GEO Sample) is not in the store's source index
[backend calls: {'get_many': 1, 'links_many': 1, 'known_many': 1}]

=== 7. Bulk paths over the whole fixture: one backend call per step
open_many(78 accessions) -> 78 records Counter({'Record': 78})  [backend calls: {'get_many': 1, 'links_many': 1}]
links(78 accessions) -> Frame of 358 link rows  [backend calls: {'links_many': 1}]
accession  relation     other      source  stated_in
GPL18573   platform of  GSE246307  SOFT    GSE246307
GPL18573   platform of  GSE246308  SOFT    GSE246308
GPL18573   platform of  GSE246309  SOFT    GSE246309
GPL18573   platform of  GSE246310  SOFT    GSE246310
... 354 more rows
follow(15 SRX, 'run') -> 15 runs  [backend calls: {'links_many': 1}]
follow(8 GSM, 'SRA') -> {'GSM484820': ['SRX018673', 'SRX018710'], 'GSM484821': ['SRX018674'], 'GSM7866518': ['SRX22239377'], 'GSM7866519': ['SRX22239378'], 'GSM7866526': ['SRX22240054'], 'GSM7866527': ['SRX22240055'], 'GSM8571270': ['SRX26377937'], 'GSM8571271': ['SRX26377938']}  [backend calls: {'links_many': 1}]
experiments_for_many(6 roots, partial=True) -> {'PRJNA1027859': 6, 'GSE246307': 2, 'GSE246308': 2, 'GSE246309': 2, 'GSM484820': 2, 'GSM484821': 1}  [backend calls: {'links_many': 2, 'get_many': 2}]
  (one links_many + one get_many per level of the walk, for all roots together)

=== 8. One query across the store, columnar
table('SRA experiment', [LIBRARY_STRATEGY, INSTRUMENT_MODEL]) -> 15 rows  [backend calls: {'scan': 1}]
   2  ATAC-seq   NextSeq 500
   2  ChIP-Seq   NextSeq 500
   3  RNA-Seq    Illumina Genome Analyzer
   6  RNA-Seq    Illumina NovaSeq 6000
   2  RNA-Seq    NextSeq 500
table('GEO Sample', ..., where=RNA-Seq) -> 4 rows  [backend calls: {'scan': 1}]
accession   status  published   content_hash                  Sample_library_strategy  Sample_organism_ch1
GSM484820   Public  2010-12-05  sha256:eed04df55197f798395db  RNA-Seq                  Sus scrofa
GSM484821   Public  2010-12-05  sha256:fc00d55ea9a5bf3e6bbb2  RNA-Seq                  Sus scrofa
GSM7866526  Public  2024-12-31  sha256:71824072aba027312e113  RNA-Seq                  Homo sapiens
GSM7866527  Public  2024-12-31  sha256:f5959a604d4bcd6cd08c8  RNA-Seq                  Homo sapiens
```

## What the demo shows

- `open_many` over all 78 records takes one `get_many` and one `links_many` call. `links` and
  `follow` over a list take one call. `experiments_for_many` over 6 roots takes 2 calls of each
  kind, one per level of the walk. A real backend would answer each call with one scan or join.
- `table` returns columns, not records, from one `scan`. `Frame` stands in for an Arrow table.
- GSM484820 has two SRA experiments. Each is stated twice, once by its SOFT record and once by
  an Entrez link. The two experiments belong to two SRA studies and two BioProjects, because the
  GSM also sits in GSE19970.
- The SuperSeries link appears twice, stated by the SuperSeries and by the SubSeries.

## Choices made where #26 is silent

- **Inverse relation names.** #26 fixes the forward names. The stub adds an inverse name for
  each one, for example `run` and `run of`, or `SRA` and `GEO`. You can follow either name.
- **Links are stored once in one direction.** The SubSeries statement `SubSeries of: GSE246310`
  is stored as `GSE246310 SuperSeries of GSE246307`, with `stated_in = GSE246307`.
- **`stated_in` beside `source`.** #26 names the source kind (SOFT, SRA XML). The stub also
  keeps the record whose text stated the link.
- **BioProject umbrella.** The relation `umbrella of` comes from BioProject XML
  `ProjectLinks/Link/Hierarchical[@type="TopAdmin"]`.
- **SRA record text.** Each `EXPERIMENT`, `STUDY`, `SAMPLE` and `RUN` element is cut from the
  `EXPERIMENT_PACKAGE` as served. `SUBMISSION` and `Organization` are dropped, so the center name
  is not on the study yet.
- **SRA dates and status.** SRA XML states a date only on runs. Studies, experiments and samples
  have empty typed dates. `SRA_Accessions.tab` has them, but #26 calls it an index only.
- **Papers.** A paper is the string `PMID:<n>`. Opening it raises an error that says to
  follow `PubMed` links instead.
- **GEO data tables.** GSM484820's SOFT block has a 6 MB data table. The fixture drops the table
  rows. The content hash still covers the full block as served.

## Open questions

1. **What does `experiments_for` return offline?** seqforge asked for the same return type as
   `labdata.experiments_for`, which returns live `Experiment` objects.
   - *Return `Record` (the stub).* Simple and frozen. seqforge's swap stops being one line, since
     `Experiment.runs` and `.title` become `follow("run")` and `field(...)`.
   - *Return `Experiment`, served from the store.* The swap stays one line. `Experiment` then needs
     an offline mode, and its properties need defining over raw fields.
   - *Return accessions only.* Cheapest. The caller then calls `open_many`.
2. **What happens when a walk reaches a record the store does not hold?** The stub raises
   `NotMirrored` and lists the missing records. `partial=True` returns only the held records.
   - *Raise (the stub).* seqforge knows to fall back to NCBI. A store trimmed by design fails
     every call that reaches a missing record.
   - *Return the held part and the missing list together.* Nothing is lost silently. The return
     type is no longer a plain list.
   - *Return the held part silently.* Easiest to call. Gives wrong answers on a partial store.
3. **Where does `NotFound` come from?** An offline store can tell "not held" from "does not
   exist" only with a list of everything that exists. The stub counts any accession a held record
   links to as existing, so `GSM9999999999` is `NotFound` only because nothing links to it.
   - *Keep the bulk accession lists (`SRA_Accessions.tab`, the GEO esummary dump) in the store.*
     Answers offline. Costs some GB and goes stale between refreshes.
   - *Check live on a miss.* Always right. Needs the network, which an offline store is meant to
     avoid.
   - *Drop `NotFound` offline.* Every valid absent accession raises `NotMirrored`, and the
     caller checks NCBI. Simplest, and it moves the work to seqforge.
4. **One link row per statement, or one per link?** The same link is often stated by both ends
   (GSE to GSM, SuperSeries to SubSeries).
   - *One row per statement (the stub).* Keeps exactly who said what. About twice the rows for
     GSE to GSM links, and `follow` must deduplicate.
   - *One row per link and source, with a list of `stated_in`.* Fewer rows. A nested column.
   - *One row per link, with a list of sources.* Smallest. Harder to query for "links only SOFT
     states", the case behind issue #35.
5. **Keep GEO sample data tables?** Most sequencing samples have none. Old ones can be large.
   - *Drop the rows, hash the full block (the stub).* Small store. Nobody can recheck the hash
     from the stored text.
   - *Keep the full block.* Text exactly as served. Store size grows with a few very large records.
   - *Drop the rows and hash only what is kept.* The hash can be rechecked. It no longer matches
     the bytes NCBI serves.
6. **Which lookups need their own sorted copy of a table?** Extra sorted copies are allowed only
   where a lookup needs one. Candidates:

   | Lookup | What a sorted copy buys | Cost |
   | --- | --- | --- |
   | Record by accession | `open_many` reads a few row groups, not the whole table. Needed whatever else is chosen | None beyond the main copy |
   | Link by `from` | `follow` forward and `experiments_for` walks read one range per accession | One copy of the link table |
   | Link by `to` | Inverse names (`run of`, `series`, `BioProject of`) read one range instead of a full scan | A second copy of the link table |
   | Record by type | `table("SRA experiment")` reads only that type | Split files by type instead of sorting. Costs nothing |
   | Record by BioProject or root Series | One range per study, so seqforge reads a study's records together | A derived root column. A record with two roots (GSM484820) is in two places |
   | Record by organism or assay | Fast filters such as "all mouse RNA-Seq" | These are raw fields, not typed columns (#26). Needs a derived column per source name, and stays stale until the next rebuild |

   The two link copies look necessary for following links both ways at scale. The by-study copy
   is the one to argue about.
