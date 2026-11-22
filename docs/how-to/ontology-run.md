# Run the ontology store

The ontology store sits beside the GEO/SRA store and the paper store. It keeps
vocabularies: the term lists that sample labels name ids from, such as UBERON for tissues or
Cellosaurus for cell lines. The store keeps each vocabulary as dated snapshots, one per
release, named `<vocabulary>-<release>`, such as `uberon-2026-11-05`.

The store answers two questions from files on disk, with no download:

- **Search**: which terms of one vocabulary match a label's text, such as "hippocampus"?
- **Lookup**: what term does an id name? Retired ids resolve too, such as a merged taxonomy
  id or an old ChEBI id.

Google Cloud Storage (GCS) holds the ontology store. One update a month, in the maintenance
run, brings in each new release and ends with a copy to GCS.

## What the store holds

Each vocabulary keeps its current snapshot and the one before it. A search returns only the
vocabulary's own terms, never an obsolete one.

| Vocabulary | Names | Comes from | Release read from | Snapshot name |
| --- | --- | --- | --- | --- |
| `ncbitaxon` | species and other taxa, by id only | NCBI Taxonomy, `taxdump.tar.gz` | the file's date, from `Last-Modified` | `ncbitaxon-<file date>` |
| `uberon` | anatomy, across animals | OBO Foundry, `uberon.obo` | `data-version` in the file's header | `uberon-YYYY-MM-DD` |
| `wbbt` | worm anatomy and cell types | OBO Foundry, `wbbt.obo` | `data-version` | `wbbt-YYYY-MM-DD` |
| `cl` | cell types | OBO Foundry, `cl.obo` | `data-version` | `cl-YYYY-MM-DD` |
| `cellosaurus` | cell lines | Cellosaurus, `cellosaurus.obo` and its list of deleted ids | `data-version` | `cellosaurus-<release number>` |
| `mondo` | diseases | OBO Foundry, `mondo.obo` | `data-version` | `mondo-YYYY-MM-DD` |
| `pato` | qualities, such as sex | OBO Foundry, `pato.obo` | `data-version` | `pato-YYYY-MM-DD` |
| `hsapdv` | human developmental stages | OBO Foundry, `hsapdv.obo` | `owl:versionInfo` in `hsapdv.owl` | `hsapdv-YYYY-MM-DD` |
| `mmusdv` | mouse developmental stages | OBO Foundry, `mmusdv.obo` | `owl:versionInfo` in `mmusdv.owl` | `mmusdv-YYYY-MM-DD` |
| `wbls` | worm life stages | OBO Foundry, `wbls.obo` | `data-version` | `wbls-YYYY-MM-DD` |
| `chebi` | chemicals, such as drugs | EBI, `chebi.obo` | `data-version` | `chebi-<release number>` |
| `fbdv` | fly developmental stages | OBO Foundry, `fbdv.obo` | `data-version` | `fbdv-YYYY-MM-DD` |
| `efo` | assays, such as RNA-seq | EBI on GitHub, `efo.obo` | the `releases/vX.Y.Z/` path in `data-version` | `efo-X.Y.Z` |

When a version names a date, as `releases/2026-11-05` does, the release is that date.
When it holds a `releases/vX.Y.Z/` path, as EFO's does, the release is `X.Y.Z`.
Otherwise it is the version as written, such as `257`. NCBI Taxonomy states no release, so
its snapshot is named by the file's date. It is looked up by id only and has no search.

## Search and look up terms

Open the store once. It keeps each vocabulary's current snapshot until you close it, so an
update that runs meanwhile changes nothing you read:

```python
from labdata.ontology import open_ontology_store

with open_ontology_store() as store:
    print(store.snapshots["uberon"])
    for c in store.search("hippocampus", "uberon", limit=3):
        print(c.id, c.name, c.matched_on, c.scope, c.synonym_type)
    print(store.lookup("CHEBI:1"))
    print(store.lookup(9606).name)
```

```text
uberon-2026-11-05
UBERON:0001954 Ammon's horn synonym RELATED None
UBERON:0002421 hippocampal formation synonym NARROW INCONSISTENT
UBERON:0005384 dorsal part of hippocampus name None None
Term(id='CHEBI:1', name=None, obsolete=False, replaced_by=('CHEBI:18357',), retired=<Reason.ALTERNATIVE: 'alternative'>, snapshot='chebi-257')
Homo sapiens
```

The store is found from the `root` argument, then `LABDATA_ONTOLOGIES`, then the pointer that
`labdata ontology init` saves. Whole-text matches come first: on the name, then on an exact
synonym, then on other synonyms. Matches on shared words follow. The
[API reference](../reference.md#labdata.ontology) gives every field.

The same from the shell. A search prints the snapshot, then one tab-separated line per
candidate: id, name, matched on, scope, synonym type and the text that matched:

```console
$ labdata ontology search uberon hippocampus
snapshot: uberon-2026-11-05
UBERON:0001954	Ammon's horn	synonym	RELATED	-	hippocampus
UBERON:0002421	hippocampal formation	synonym	NARROW	INCONSISTENT	hippocampus
UBERON:0005384	dorsal part of hippocampus	name	-	-	dorsal part of hippocampus
$ labdata ontology lookup CHEBI:1
id: CHEBI:1
retired: alternative
replaced by: CHEBI:18357
snapshot: chebi-257
$ labdata ontology lookup 9606
id: NCBITaxon:9606
name: Homo sapiens
snapshot: ncbitaxon-2026-11-21
```

## Set up once

Make the store and save the pointer to it:

```bash
labdata ontology init /path/to/ontology-store \
  --copy-to gcs:<gcs-bucket>/datalake/ontology-store
```

For the copy to GCS, set up the `gcs` rclone remote and log in as
[Copy the store to GCS](store-gcs.md) shows. Leave out `--copy-to` to run with no copy.

## Update the store

The update needs internet, so run it as a Slurm job, from the repository folder on the
cluster:

```bash
export LABDATA_ONTOLOGIES=/path/to/ontology-store
sbatch scripts/ontology/update.sbatch
```

For each vocabulary, the update reads the release from the file's first bytes or its date,
with no full download. It downloads only the vocabularies whose release has no snapshot yet.
It checks the taxonomy file against NCBI's checksum, and refuses a new release that holds far
fewer terms than the current one. Then it makes the new snapshots current, deletes snapshots
older than the previous one, and copies the store to GCS. The first update downloads every
vocabulary and copies the whole store.

The job log ends with one line per vocabulary:

```text
ncbitaxon: updated (ncbitaxon-2026-11-21)
uberon: unchanged (uberon-2026-11-05)
chebi: failed (GET https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.obo failed: HTTP 503 from https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.obo); keeps chebi-256
```

A vocabulary that fails keeps its current snapshot while the others update, and the job
exits 1. Once the cause is fixed, submit it again: it downloads only what is still missing.
The update also exits 1 when the copy to GCS is pending.

On a node with internet, `labdata ontology update` runs the same update with no job. While
an update, copy or restore runs, the others refuse to start. If the message says it cannot check
whether the other one still runs, check with `squeue` on its cluster. Once you know it has
ended, run the same command again with `--owner-gone`.

## When a source host refuses the cluster

A vocabulary whose host refuses the cluster fails each month with a `ConnectionRefusedError`.
The failed line in the job log names the file's address. Until the host lets the cluster in,
bring the file in by hand:

1. On a machine that can reach the host, download the file from that address.
2. Copy it to the cluster. Keep its date, as `scp -p` or `rsync -t` do: the status record
   shows it as `source_date`.
3. Give the file to the update with `--source <vocabulary>=<path>`, once for each such
   vocabulary:

   ```bash
   export LABDATA_ONTOLOGIES=/path/to/ontology-store
   sbatch scripts/ontology/update.sbatch --source chebi=/path/to/chebi.obo
   ```

The update then reads that vocabulary from the file, not from the web. It reads the release
from the file's header and keeps a copy of the file in the new snapshot. Your file stays as it
was. The other vocabularies update from the web as usual.

`--source` takes one OBO file whose header states a `data-version`. In the table above, that
is a vocabulary that comes from one file and has its release read from `data-version`. For
any other, the update refuses and names why.

## Read `ontology-status.json`

The file at the store root records the last update, with one row per vocabulary:

```json
{
  "copy_pending": false,
  "end": "2026-11-21T17:02:40+00:00",
  "outcome": "finalized",
  "start": "2026-11-21T16:40:02+00:00",
  "vocabularies": {
    "chebi": {"reason": "GET https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.obo failed: HTTP 503 from https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.obo",
              "release": null, "result": "failed", "snapshot": "chebi-256", "source_date": "2026-10-01", "terms": 219034},
    "uberon": {"reason": null, "release": "2026-11-05", "result": "unchanged",
               "snapshot": "uberon-2026-11-05", "source_date": "2026-11-05", "terms": 16143}
  }
}
```

- `outcome` is `running`, `failed`, `built` or `finalized`. A vocabulary that fails makes it
  `failed`. `finalized` means GCS holds the update.
- Each row gives the `release` checked, the `result` (`updated`, `unchanged` or `failed`,
  with its `reason`), the current `snapshot`, its count of own terms, `terms`, and its source
  file's date, `source_date`.
- `copy_pending: true` means GCS is behind. Log in, then run `labdata ontology copy`. The next
  update also copies first.

## Check the copy

The update copies the store to GCS at its end. It sends the new snapshots, deletes the pruned
ones by name, then sends its copy record and `ontology-status.json` last. A copy refuses when
it would delete more snapshots than it adds, past a small share. If the snapshots left on
purpose, run the copy by hand with their number, such as `labdata ontology copy
--accept-deletes 3`.

The copy is current when both of these hold:

- In `ontology-status.json`, `outcome` is `finalized` and `copy_pending` is `false`.
- The copy record in GCS matches the one on the disk:

  ```bash
  pixi run rclone check "$LABDATA_ONTOLOGIES" <copy_to> --max-depth 1 \
    --include "/.copied-files.json"
  ```

  It should report 0 differences and 1 matching file.

Each update and each copy first checks that the disk's copy record is the one in GCS. If it
is not, the disk is behind GCS, and the update or copy refuses and names the restore.

## Restore the store from GCS

Restore the work disk when it is new or lost, or when an update refuses because the disk is
behind GCS. A restored disk can search at once, and holds each snapshot's source file, so it
can rebuild its tables with no download. Downloads out of GCS cost money, so run a large
restore only with the maintainer's yes:

```bash
export LABDATA_ONTOLOGIES=/path/to/ontology-store
labdata ontology restore --from <copy_to> --accept-download
```

The restore prints how much it downloads. In a terminal it asks before more than 1 GB. A batch
job has no terminal, so it needs `--accept-download` for that. An empty disk gets the whole
store; a disk that fell behind gets only the files that changed.
