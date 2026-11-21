# How the stores work

Three stores live on a work disk on the cluster. The GEO/SRA store keeps study metadata
from NCBI. The paper store keeps the papers those studies link to. The ontology store keeps the
vocabularies that sample labels name ids from. Google Cloud Storage (GCS) holds all three and is
the source of truth. An update is done only once GCS holds it.

This page shows how data moves from its sources to GCS. Each colour in its diagrams means one
thing: **the GEO/SRA store**{ .d-key .d-store }, **the paper store**{ .d-key .d-paper },
**GCS**{ .d-key .d-gcs }, **a failure**{ .d-key .d-fail } and
**an update or paper set aside**{ .d-key .d-idle }. To run a store, follow
[Run the full download](store-run.md), [Copy the store to GCS](store-gcs.md),
[Run the paper store](papers-run.md) or [Run the ontology store](ontology-run.md).

## From the sources to GCS

The GEO/SRA store takes its records from NCBI. The paper store reads which papers to fetch from
the current GEO/SRA build, then fetches each paper from the cheapest source that has it. The
ontology store reads only its own sources, so the diagram leaves it out. All three stores copy
their changes to GCS, and a new work disk restores from GCS.

<figure class="diagram">
--8<-- "docs/diagrams/store-overview.svg"
<figcaption>Records flow from their sources through the work disk to GCS.</figcaption>
</figure>

Each source gives the stores these records:

- **NCBI FTP bulk files**: the lists of SRA, BioProject and BioSample records, each month.
- **E-utilities `esummary`**: a summary of every GEO Series, each month.
- **The GEO download site**: each Series' family SOFT file and its supplementary file list.
- **E-utilities `efetch`**: the SRA experiment XML for each Series.
- **The PMC Open Access bucket**: article XML, figures and supplements.
- **PubMed and the PMC ID converter**: each paper's details, IDs and retraction notices.
- **OpenAlex**: work details, and PDFs bought within a daily quota.
- **Publisher sites**: free PDF links.
- **The OBO Foundry**: UBERON, CL, ChEBI and the other OBO vocabularies.
- **The NCBI Taxonomy FTP site**: species names, with merged and deleted taxonomy ids.
- **The Cellosaurus FTP site**: cell lines, and the list of deleted cell lines.

## The GEO/SRA store keeps one build per update

For each target GEO Series, the store holds the Series with its Samples and Platforms. It also
holds the linked SRA studies, experiments, runs and samples, and the BioProject and BioSample
records. Each update makes a new build, a complete set of tables that never changes once
written. A small marker file names the current build, and readers open that one through
[`BuildReader`](../reference.md).

A full run and a monthly refresh are Snakemake workflows. Each step runs once its inputs
exist. Only a few network steps run at once, so the store stays gentle on NCBI.

<figure class="diagram">
--8<-- "docs/diagrams/store-update.svg"
<figcaption>Every update ends the same way: extract, build, switch, prune and copy.</figcaption>
</figure>

The dashed steps run only in a refresh. A refresh downloads new snapshots and fetches again
only the GEO Series that changed. Before it packs, it confirms which Series NCBI withdrew. A
Series goes only when a fetch of that Series alone finds it gone. A full run skips the dashed
steps and fetches every target Series from the snapshots on disk. The SRA lane runs twice
because each saved SOFT file can name more SRA experiments.

### Each update ends finalized, failed or abandoned

The status record, `refresh-status.json`, names the outcome of the last update.

<figure class="diagram">
--8<-- "docs/diagrams/store-states.svg"
<figcaption>An update is finalized only once GCS holds its status record.</figcaption>
</figure>

- **running**: the update holds the run lock. It starts only when the work disk matches GCS.
- **failed**: a step failed, or the new build missed too many target Series. Readers keep the
  old build. The next run resumes in the same folder.
- **abandoned**: a new update replaced it. A refresh that waits too long to resume is
  abandoned too. Only the run folder records this outcome.
- **built**: the work disk reads the new build, but GCS does not hold it yet. If the copy
  fails, the next run copies first.
- **finalized**: GCS holds the new build and the status record.

When the run lock is held, the store asks Slurm whether the job that took it still runs. If it
has ended, the next update takes the lock. If Slurm cannot tell, you confirm with
`--owner-gone`.

## The paper store climbs a ladder of sources

The paper store fetches the papers that target GEO Series and held BioProjects link by PMID
or DOI. It reads those links from the current GEO/SRA build, so a new build brings in new
papers. Each paper waits on the first tier that can still serve it, cheapest first:

<figure class="diagram">
--8<-- "docs/diagrams/paper-ladder.svg"
<figcaption>Each paper climbs from the cheapest tier until one serves it.</figcaption>
</figure>

- A tier that has no copy, or fails, passes the paper on. Repeated server errors pass it on
  too.
- A paper published within the last 365 days waits for PMC and tries only the free tiers.
- A retracted paper is skipped, whatever its state.
- PMC gives the article XML, figures and supplements, where the other tiers give only a PDF.
  When PMC later has a paper, the PMC sync saves its files and then deletes the other PDF.

One long Slurm job, the paper job, runs two kinds of pass:

- **The daily pass**, after 00:30 UTC: buy PDFs from OpenAlex within each account's daily
  quota, try each publisher link once, then rewrite the manual queue.
- **The PMC sync**, on the 3rd of each month and whenever a new GEO/SRA build becomes
  current: take in new papers, look up their details, then fetch what changed in the PMC
  bucket.

Every pass takes the store's lock, checks that the work disk matches GCS, does its work and
saves all four tables together. It ends with a copy to GCS. The paper job keeps two more jobs
queued behind it, so a new one takes over when its time on Slurm runs out.

## The ontology store keeps two snapshots of each vocabulary

The ontology store keeps each vocabulary as snapshots, one per release, such as
`uberon-2026-11-05`. A small record names each vocabulary's current snapshot. A reader that
opens the store keeps those snapshots until it closes, so an update changes nothing it reads.

One update, a single process under the store's lock, checks each vocabulary's release. It
downloads only the vocabularies with a new release and builds their tables. Then it makes them
current, keeps the previous snapshot of each, deletes older ones and copies the store to GCS.
A vocabulary that fails keeps its current snapshot while the others update. The status record,
`ontology-status.json`, names the outcome, as `refresh-status.json` does, and the result for
each vocabulary.

## All three stores copy to GCS the same way

A copy starts with a check. It compares the copy record that GCS holds with the one on the
work disk. If they differ, the work disk is behind GCS, so the copy refuses and you restore
first. Then the copy sends, in this order:

1. The new or changed files, by name.
2. The top-level files, such as the marker and `README.md`.
3. The deletes, by name.
4. The copy record: the list of files GCS now holds.
5. The status record, marked `finalized`.

Because the status record goes last, GCS never holds a status record for files it lacks. No
copy mirrors the disk with `rclone sync`. A guard stops a copy that would delete more raw
files, paper files or snapshots than a small share of them, unless you allow it. A restore runs the same
steps the other way, and asks before a large download.

## When each store changes

| When | GEO/SRA store | Paper store | Ontology store |
| --- | --- | --- | --- |
| Each day | Nothing runs. | The daily pass, then a copy. | Nothing runs. |
| Each hour, if needed | Nothing runs. | A failed pass or a pending copy runs again. | Nothing runs. |
| Each month | The refresh, started by hand in the maintenance run. | The PMC sync on the 3rd, or as soon as a new build is current. | The update, started by hand in the maintenance run. It downloads only the vocabularies with a new release. |
| Each year | Heavy checks, the refresh, then removal of withdrawn SRA records, a new build and a copy. | Nothing extra runs. | Nothing extra runs. |
| After a store code change | One full build at the end. | Nothing extra runs. | If the table layout changed, the next update remakes the tables from the kept source files, with no download. |

The [maintenance log](../maintenance-log.md) records each monthly and yearly run.
